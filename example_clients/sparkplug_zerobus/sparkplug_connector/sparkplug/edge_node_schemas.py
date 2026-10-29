"""What the connector does when an NBIRTH arrives (``BirthNotifier`` and ``DatabricksSql``):
``CALL <node schema>.create_edge_node_schema(prefix, group, edge node, false)``, which
(re)builds the edge node's per-model views from its latest NBIRTH. The procedures are configurable
(``edge_node_schemas.procedures``, ``catalog.schema.procedure``); each is called with those four arguments.
Only NBIRTHs call them: a DBIRTH is handled like data messages.

Fed with the rows Zerobus has acknowledged (the confirmed writer's listener), so the NBIRTH is in the raw
table; before calling, it waits until the row is readable there (the procedure reads the latest NBIRTH
back). Off the MQTT and Zerobus paths: NBIRTHs of one edge node within ``coalesce_s`` make one call (a
rebirth often brings two), a failed call is retried with backoff (procedures already done aren't called
again; ~5 minutes, then given up and counted), and a newer NBIRTH replaces a pending one. One call at a time
per edge node: a newer NBIRTH waits for the call running for its edge node (each run replaces the same views; two at
once break each other), then runs. ``owns`` picks the NBIRTHs this caller handles (the writer reports every row of
its table, so with two sources on one table each caller takes its own source's). Calls pending
at stop are logged and dropped: the next NBIRTH (or ``create_all_edge_node_schemas``) catches up.
A procedure's name goes into the statement's text (identifiers can't be bound), so it must be a
three-part name (``nbirth_procedures``).

After each call of a ``create_edge_node_schema`` procedure (``builds_views``; another procedure in the list is not
checked) it checks that the procedure made the edge node's objects (``check_views``, on by default): in the
procedure's catalog, the schema ``<prefix>_<group>_<edge node>`` and, for each model (each top-level Template metric
of the NBIRTH), the views ``<model>``, ``<model>_vw`` and ``<model>_asof_vw``; names as the procedure makes them, lower case, each
run of characters other than ``[A-Za-z0-9]`` replaced by ``_`` (``node_schema_name``, ``model_view_names``). An NBIRTH
without models, or of a group in ``schema_only_groups`` (the connector's diagnostics): the schema only. Something missing fails the attempt: the procedure is called again at the next one.

SQL runs on a SQL warehouse through the Statement Execution API as the Zerobus service principal (OAuth
M2M). It needs the ``databricks-sql-access`` entitlement, ``CAN USE`` on the warehouse, and in each node
catalog ``USE CATALOG`` + ``CREATE SCHEMA``, ``USE SCHEMA``, ``EXECUTE`` on the procedure and ``SELECT`` on
what it reads (``sparkplug/README.md``, Databricks setup).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

import httpx

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 6
FIRST_RETRY_S, MAX_RETRY_S = 5.0, 120.0
CHECK_EVERY_S = 2.0


def retry_delay(attempt: int) -> float:
    return min(FIRST_RETRY_S * (1 << min(attempt, 10)), MAX_RETRY_S)


# catalog.schema.procedure; each part plain or backquoted (no backquote or line break inside)
_PART = r"(?:[A-Za-z0-9_]+|`[^`\r\n]+`)"
PROCEDURE_NAME = re.compile(rf"{_PART}\.{_PART}\.{_PART}")
_CATALOG = re.compile(rf"({_PART})\.")
_NOT_ALNUM = re.compile(r"[^A-Za-z0-9]+")


def _normalized(name: str) -> str:
    """A name as the procedure makes it: lower case, each run of characters other than [A-Za-z0-9] -> "_"."""
    return _NOT_ALNUM.sub("_", name).lower()


def node_schema_name(prefix: str, group_id: str, edge_node_id: str) -> str:
    """The edge node's schema, as the procedure names it."""
    return _normalized(f"{prefix}_{group_id}_{edge_node_id}")


def birth_models(msg: str) -> tuple[str, ...]:
    """The models of an NBIRTH (its ``msg`` JSON): each top-level Template metric, named as the procedure names its
    views (the metric's name and ``version``; Tahu's JSON has no ``version`` on a metric, so the name)."""
    try:
        metrics = json.loads(msg).get("metrics") or []
    except (ValueError, AttributeError):
        return ()
    models = []
    for m in metrics:
        if isinstance(m, dict) and m.get("dataType") == "Template" and isinstance(m.get("name"), str):
            model = _normalized(m["name"] + str(m.get("version") or ""))
            if model not in models:
                models.append(model)
    return tuple(models)


def model_view_names(model: str) -> tuple[str, str, str]:
    """The three views the procedure makes per model: the base view, ``_vw`` and ``_asof_vw``."""
    return model, f"{model}_vw", f"{model}_asof_vw"


def builds_views(procedure: str) -> bool:
    """Whether the check applies after this procedure: one named ``create_edge_node_schema`` (in any catalog and
    schema) builds the schema and views the check looks for; another (a grant, a builder of other objects) is called,
    not checked."""
    return procedure.rsplit(".", 1)[-1].strip("`").lower() == "create_edge_node_schema"


class ObjectsMissing(Exception):
    """The procedure returned, but the edge node's schema or views aren't there."""


def quoted_table(name: str) -> str:
    """``catalog.schema.table`` with each part backquoted (Unity Catalog names may have hyphens); raises
    ``ValueError`` unless three non-empty parts without a backquote or line break."""
    parts = name.split(".")
    if len(parts) != 3 or not all(p and not re.search(r"[`\r\n]", p) for p in parts):
        raise ValueError(f"raw table must be catalog.schema.table: {name}")
    return ".".join(f"`{p}`" for p in parts)


def nbirth_procedures(settings: dict[str, Any]) -> list[str]:
    """The procedures an ``edge_node_schemas`` section calls on NBIRTH: ``procedures``, a list or text
    separated by commas. Raises ``ValueError`` for a name that isn't ``catalog.schema.procedure``."""
    if "node_schemas" in settings:
        raise ValueError("edge_node_schemas.node_schemas is now procedures: catalog.schema.procedure names, "
                         "e.g. <node schema>.create_edge_node_schema")
    value = settings.get("procedures")
    values = value.split(",") if isinstance(value, str) else list(value or [])
    names = [str(v).strip() for v in values if str(v).strip()]
    bad = [n for n in names if not PROCEDURE_NAME.fullmatch(n)]
    if bad:
        raise ValueError(f"NBIRTH procedure must be catalog.schema.procedure: {', '.join(bad)}")
    return names


class Sql(Protocol):
    async def execute(self, statement: str, params: dict[str, str]) -> list[list[str | None]]: ...


@dataclass(frozen=True)
class Birth:
    group_id: str
    edge_node_id: str
    msg_id: str
    models: tuple[str, ...] = ()                    # birth_models of its NBIRTH

    @property
    def key(self) -> str:
        return f"{self.group_id}/{self.edge_node_id}"


@dataclass(frozen=True)
class NotifierStatus:
    births: int
    calls: int
    failures: int
    gave_up: int
    pending: int
    last_error: str | None
    checked: int = 0                                # calls whose schema and views were found


class BirthNotifier:
    def __init__(self, sql: Sql, raw_table: str, procedures: list[str], prefix: str = "nb", coalesce_s: float = 2.0,
                 readable_within_s: float = 60.0, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                 clock: Callable[[], float] = time.time, check_views: bool = True,
                 schema_only_groups: frozenset[str] = frozenset(),
                 owns: Callable[[dict[str, Any]], bool] = lambda row: True):
        self._raw_table = quoted_table(raw_table)
        procedures = nbirth_procedures({"procedures": procedures})
        self._sql, self._procedures, self._prefix = sql, procedures, prefix
        self._coalesce, self._readable_within, self._sleep, self._clock = coalesce_s, readable_within_s, sleep, clock
        self._latest: dict[str, Birth] = {}
        self._tasks: set[asyncio.Task] = set()
        self._check_views, self._schema_only, self._owns = check_views, schema_only_groups, owns
        self._running: dict[str, asyncio.Lock] = {}     # per edge node: one call at a time
        self._births = self._calls = self._failures = self._gave_up = self._checked = 0
        self._last_error: str | None = None

    def accept(self, records: list[str]) -> None:
        """The confirmed writer's listener: JSON records Zerobus has just acknowledged. Only queues work."""
        for text in records:
            if '"message_type":"NBIRTH"' not in text:
                continue
            row = json.loads(text)
            if row.get("message_type") != "NBIRTH" or not self._owns(row):
                continue                            # another source's NBIRTH on the same table: its own caller
            self._births += 1
            b = Birth(row["group_id"], row["edge_node_id"], row["msg_id"], birth_models(row.get("msg") or ""))
            self._latest[b.key] = b
            self._schedule(self._attempts(b, delay=self._coalesce))

    def _schedule(self, coro) -> None:
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _attempts(self, b: Birth, delay: float) -> None:
        done: set[str] = set()
        await self._sleep(delay)
        for attempt in range(MAX_ATTEMPTS):
            # one call at a time per edge node: each call replaces the same views, two at once break each other
            async with self._running.setdefault(b.key, asyncio.Lock()):
                if self._latest.get(b.key) is not b:
                    return                          # a newer NBIRTH of this edge node has its own call
                if await self.run(b, attempt, done):
                    return
            if attempt + 1 < MAX_ATTEMPTS:
                await self._sleep(retry_delay(attempt))

    async def run(self, b: Birth, attempt: int, done: set[str]) -> bool:
        """One attempt: True when every procedure is done (or given up), False to retry."""
        try:
            await self._await_readable(b)
            for procedure in self._procedures:
                if procedure in done:
                    continue
                t0 = time.monotonic()
                await self._sql.execute(f"CALL {procedure}(:prefix, :group_id, :edge_node_id, false)",
                                        {"prefix": self._prefix, "group_id": b.group_id, "edge_node_id": b.edge_node_id})
                self._calls += 1
                logger.info("%s for %s (NBIRTH %s) in %d ms", procedure, b.key, b.msg_id, (time.monotonic() - t0) * 1000)
                if self._check_views and builds_views(procedure):
                    await self._check(procedure, b)     # ObjectsMissing: called again at the next attempt
                done.add(procedure)
            if self._latest.get(b.key) is b:
                del self._latest[b.key]
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - retried with backoff, then given up
            self._failures += 1
            self._last_error = f"{b.key}: {e}"
            if attempt + 1 >= MAX_ATTEMPTS:
                self._gave_up += 1
                if self._latest.get(b.key) is b:
                    del self._latest[b.key]
                logger.error("Gave up calling the NBIRTH procedures for %s after %d attempts (done: %s): %s",
                             b.key, MAX_ATTEMPTS, sorted(done), e)
                return True
            logger.warning("NBIRTH procedure call for %s failed (attempt %d of %d, retrying in %.0f s): %s",
                           b.key, attempt + 1, MAX_ATTEMPTS, retry_delay(attempt), e)
            return False

    async def _check(self, procedure: str, b: Birth) -> None:
        """The edge node's schema and its models' views, in the procedure's catalog."""
        catalog = _CATALOG.match(procedure).group(1)
        schema = node_schema_name(self._prefix, b.group_id, b.edge_node_id)
        views = bool(b.models) and b.group_id not in self._schema_only
        if views:
            rows = await self._sql.execute(f"SELECT table_name FROM {catalog}.information_schema.views "
                                           "WHERE table_schema = :schema", {"schema": schema})
            found = {r[0] for r in rows if r and r[0]}
            missing = [v for m in b.models for v in model_view_names(m) if v not in found]
        else:
            rows = await self._sql.execute(f"SELECT count(*) FROM {catalog}.information_schema.schemata "
                                           "WHERE schema_name = :schema", {"schema": schema})
            missing = [] if rows and rows[0] and rows[0][0] not in (None, "0") else ["the schema"]
        if missing:
            raise ObjectsMissing(f"{procedure} returned, but {catalog}.{schema} lacks {', '.join(missing)}")
        self._checked += 1
        if views:
            logger.info("%s.%s has the views of %d models for %s", catalog, schema, len(b.models), b.key)
        else:
            logger.info("%s.%s exists for %s (schema only)", catalog, schema, b.key)

    async def _await_readable(self, b: Birth) -> None:
        deadline = self._clock() + self._readable_within
        while True:
            rows = await self._sql.execute(f"SELECT count(*) FROM {self._raw_table} WHERE msg_id = :msg_id", {"msg_id": b.msg_id})
            if rows and rows[0] and rows[0][0] not in (None, "0"):
                return
            if self._clock() > deadline:
                raise TimeoutError(f"NBIRTH {b.msg_id} not readable in {self._raw_table} after {self._readable_within:.0f} s")
            await self._sleep(CHECK_EVERY_S)

    def latest_for_test(self, b: Birth) -> None:
        self._latest[b.key] = b

    def status(self) -> NotifierStatus:
        return NotifierStatus(self._births, self._calls, self._failures, self._gave_up, len(self._latest), self._last_error,
                              self._checked)

    async def stop(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._latest:
            logger.warning("Stopped with NBIRTH procedure calls pending for %s", sorted(self._latest))


class SqlError(Exception):
    """A statement that didn't succeed (the warehouse's message), or an HTTP failure."""


class DatabricksSql:
    """SQL on a SQL warehouse (Statement Execution API) as a service principal: an OAuth M2M token from the
    workspace's ``/oidc/v1/token``, reused until a minute before it expires."""

    POLL_S, TIMEOUT_S = 1.0, 300.0

    def __init__(self, workspace_url: str, client_id: str, client_secret: str, warehouse_id: str,
                 transport: httpx.AsyncBaseTransport | None = None, clock: Callable[[], float] = time.time):
        self._url = workspace_url.rstrip("/")
        self._client_id, self._client_secret, self._warehouse = client_id, client_secret, warehouse_id
        self._http = httpx.AsyncClient(timeout=60.0, transport=transport)
        self._clock = clock
        self._token: str | None = None
        self._token_expires = 0.0

    async def execute(self, statement: str, params: dict[str, str]) -> list[list[str | None]]:
        body = {"warehouse_id": self._warehouse, "statement": statement, "wait_timeout": "30s", "on_wait_timeout": "CONTINUE",
                "parameters": [{"name": k, "value": v, "type": "STRING"} for k, v in params.items()]}
        r = await self._call("POST", "/api/2.0/sql/statements", body)
        deadline = self._clock() + self.TIMEOUT_S
        while r.get("status", {}).get("state") in ("PENDING", "RUNNING"):
            if self._clock() > deadline:
                raise SqlError(f"statement still running after {self.TIMEOUT_S / 60:.0f} min: {statement}")
            await asyncio.sleep(self.POLL_S)
            r = await self._call("GET", f"/api/2.0/sql/statements/{r['statement_id']}", None)
        status = r.get("status", {})
        if status.get("state") != "SUCCEEDED":
            raise SqlError(f"{status.get('state')}: {status.get('error', {}).get('message', '(no message)')}")
        return r.get("result", {}).get("data_array") or []

    async def _call(self, method: str, path: str, body: dict[str, Any] | None) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {await self._bearer()}"}
        return self._check(await self._http.request(method, self._url + path, json=body, headers=headers), path)

    async def _bearer(self) -> str:
        if self._token is None or self._clock() > self._token_expires:
            res = await self._http.post(self._url + "/oidc/v1/token", auth=(self._client_id, self._client_secret),
                                        data={"grant_type": "client_credentials", "scope": "all-apis"})
            t = self._check(res, "/oidc/v1/token")
            self._token = t["access_token"]
            self._token_expires = self._clock() + max(0, int(t.get("expires_in", 3600)) - 60)
        return self._token

    @staticmethod
    def _check(res: httpx.Response, path: str) -> dict[str, Any]:
        if res.status_code // 100 != 2:
            raise SqlError(f"HTTP {res.status_code} on {path}: {res.text[:300]}")
        return res.json()

    async def close(self) -> None:
        await self._http.aclose()
