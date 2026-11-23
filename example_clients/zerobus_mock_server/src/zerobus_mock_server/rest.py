"""REST listener: OAuth token endpoint, REST insert endpoint, and the control API.

Endpoints and response shapes follow the Zerobus REST docs:

* ``POST /oidc/v1/token`` (form body, client credentials) returns an access token.
* ``POST /zerobus/v1/tables/<catalog.schema.table>/insert`` takes a JSON array
  (or JSON lines) and returns ``200 {}`` on success.
* Errors are ``{"error_code": ..., "message": ...}``. Unsupported content types
  return 415.

The ``/_mock/...`` routes are the mock's control API. They are not part of the
Zerobus API and should only be reachable from test code.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

from .auth import RESOURCE_RE, TokenProblem, parse_authorization_details, parse_basic
from .faults import FAULT_TYPES, Firing, FaultError
from .schema import SchemaError
from .state import MockState
from .validation import RecordRejected, validate_object

HTTP_ERROR_CODES = {
    400: "INVALID_PARAMETER_VALUE",
    401: "UNAUTHENTICATED",
    403: "PERMISSION_DENIED",
    404: "NOT_FOUND",
    409: "ABORTED",
    415: "INVALID_PARAMETER_VALUE",
    429: "RESOURCE_EXHAUSTED",
    500: "INTERNAL_ERROR",
    502: "UNAVAILABLE",
    503: "UNAVAILABLE",
    504: "DEADLINE_EXCEEDED",
}
ERROR_STATUS = {
    "INVALID_PARAMETER_VALUE": 400,
    "OUT_OF_RANGE": 400,
    "NOT_FOUND": 404,
    "UNAUTHENTICATED": 401,
    "PERMISSION_DENIED": 403,
    "UNAVAILABLE": 503,
    "RESOURCE_EXHAUSTED": 429,
    "INTERNAL_ERROR": 500,
}
JSON_TYPES = ("application/json", "application/jsonl")


def error_body(error_code: str, message: str) -> Dict[str, str]:
    return {"error_code": error_code, "message": message}


def _oauth_error(status: int, error: str, description: str) -> JSONResponse:
    return JSONResponse({"error": error, "error_description": description}, status_code=status)


class _RecordsParseError(Exception):
    pass


def parse_records(body: bytes, content_type: str) -> List[Dict[str, Any]]:
    """Parse a JSON array, one JSON object, or JSON lines into record objects."""
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        raise _RecordsParseError("the body is not valid UTF-8")

    def lines() -> List[Any]:
        out = []
        for n, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                raise _RecordsParseError(f"line {n} is not valid JSON")
        return out

    if content_type == "application/jsonl":
        items = lines()
    else:
        try:
            parsed = json.loads(text) if text.strip() else []
        except ValueError:
            parsed = None
        if parsed is None:
            items = lines()
        elif isinstance(parsed, list):
            items = parsed
        elif isinstance(parsed, dict):
            items = [parsed]
        else:
            raise _RecordsParseError("the body must be a JSON array of objects or JSON lines")
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            raise _RecordsParseError(f"record {i} is not a JSON object")
    return items


def _bearer(request: Request) -> Optional[str]:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


async def _apply_http_fault(state: MockState, firing: Firing) -> Optional[Response]:
    """Apply a REST fault that answers the request. Returns the response, or None to continue."""
    f = firing.fault
    if f.type == "token_invalid_client":
        state.log_firing(firing, "401 invalid_client")
        return _oauth_error(401, "invalid_client", "client authentication failed (injected)")
    if f.type == "token_invalid_authorization_details":
        state.log_firing(firing, "401 invalid_authorization_details")
        return _oauth_error(401, "invalid_authorization_details", "authorization_details rejected (injected)")
    if f.type == "http_error":
        status = int(f.params["status"])
        code = HTTP_ERROR_CODES.get(status, "UNAVAILABLE")
        message = f.params.get("message") or f"injected HTTP {status}"
        headers = {}
        if f.params.get("retry_after_seconds") is not None:
            headers["Retry-After"] = str(f.params["retry_after_seconds"])
        state.log_firing(firing, f"{status} {code}")
        return JSONResponse(error_body(code, message), status_code=status, headers=headers)
    if f.type == "unauthorized":
        state.log_firing(firing, "401 UNAUTHENTICATED")
        return JSONResponse(error_body("UNAUTHENTICATED", "injected: token rejected"), status_code=401)
    if f.type == "permission_denied":
        state.log_firing(firing, "403 PERMISSION_DENIED")
        return JSONResponse(error_body("PERMISSION_DENIED", "injected: caller lacks privileges"), status_code=403)
    if f.type == "table_not_found":
        state.log_firing(firing, "404 NOT_FOUND")
        return JSONResponse(error_body("NOT_FOUND", f"Table \"{firing.table}\" cannot be found. (injected)"), status_code=404)
    if f.type == "slow_response":
        delay = float(f.params["delay_ms"]) / 1000.0
        state.log_firing(firing, "delayed response", f"{f.params['delay_ms']} ms")
        await asyncio.sleep(delay)
        return None
    if f.type == "schema_change":
        specs = f.params["add_columns"]
        try:
            snap = state.apply_schema_change(firing.table, specs)
        except (SchemaError, ValueError) as exc:
            state.log_firing(firing, "schema_change failed", str(exc))
            return None
        state.log_firing(firing, "schema_change applied", f"table now schema v{snap.version}")
        return None
    return None


def create_rest_app(state: MockState) -> FastAPI:
    app = FastAPI(title="Zerobus Ingest mock server (test double)", docs_url=None, redoc_url=None, openapi_url=None)

    # ------------------------------------------------------------ Zerobus API

    @app.post("/oidc/v1/token")
    async def token(request: Request):
        raw = (await request.body()).decode("utf-8", "replace")
        params = {k: v[-1] for k, v in parse_qs(raw, keep_blank_values=True).items()}
        basic = parse_basic(request.headers.get("authorization"))
        client_id, secret = basic if basic else (params.get("client_id", ""), params.get("client_secret", ""))
        if not state.known_client(client_id, secret):
            return _oauth_error(401, "invalid_client", "client authentication failed")
        if params.get("grant_type") != "client_credentials":
            return _oauth_error(400, "unsupported_grant_type", "grant_type must be client_credentials")
        if params.get("scope") != "all-apis":
            return _oauth_error(400, "invalid_scope", "scope must be all-apis")
        match = RESOURCE_RE.match(params.get("resource", ""))
        if not match:
            return _oauth_error(400, "invalid_target", "resource must be api://databricks/workspaces/<id>/zerobusDirectWriteApi")
        workspace_id = match.group(1)
        if state.cfg.server.workspace_id and workspace_id != state.cfg.server.workspace_id:
            return _oauth_error(400, "invalid_target", f"workspace {workspace_id} is not served here")
        try:
            table = parse_authorization_details(params.get("authorization_details", ""), state.tables.names())
        except TokenProblem as exc:
            return _oauth_error(exc.status, exc.error, exc.description)

        for firing in state.registry.evaluate("token", "rest", table):
            resp = await _apply_http_fault(state, firing)
            if resp is not None:
                return resp

        token_value, ttl = state.tokens.issue(client_id, table, workspace_id)
        return JSONResponse(
            {"access_token": token_value, "token_type": "Bearer", "expires_in": ttl, "scope": "all-apis"},
            status_code=200,
        )

    @app.post("/zerobus/v1/tables/{table}/insert")
    async def insert(table: str, request: Request):
        ctype = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
        if ctype not in JSON_TYPES:
            return JSONResponse(
                error_body("INVALID_PARAMETER_VALUE", f"unsupported Content-Type {ctype or '(none)'}; use application/json or application/jsonl"),
                status_code=415,
            )
        if not state.tables.has(table):
            return JSONResponse(error_body("NOT_FOUND", f"Table \"{table}\" cannot be found."), status_code=404)

        for firing in state.registry.evaluate("insert", "rest", table):
            resp = await _apply_http_fault(state, firing)
            if resp is not None:
                return resp

        check = state.tokens.check(_bearer(request), table)
        if check == "UNAUTHENTICATED":
            return JSONResponse(error_body("UNAUTHENTICATED", "missing, unknown or expired access token"), status_code=401)
        if check == "PERMISSION_DENIED":
            return JSONResponse(error_body("PERMISSION_DENIED", f"the token was not issued for {table}"), status_code=403)

        body = await request.body()
        try:
            records = parse_records(body, ctype)
        except _RecordsParseError as exc:
            return JSONResponse(error_body("INVALID_PARAMETER_VALUE", str(exc)), status_code=400)

        snap = state.tables.snapshot(table)
        rows: List[Dict[str, Any]] = []
        for i, record in enumerate(records):
            record_firings = state.registry.evaluate("record", "rest", table)
            injected = None
            for firing in record_firings:
                if firing.type == "record_too_large":
                    state.log_firing(firing, "400 OUT_OF_RANGE", f"record {i}")
                    return JSONResponse(
                        error_body("OUT_OF_RANGE", f"record {i} exceeds the maximum message size (injected)"),
                        status_code=400,
                    )
                if firing.type == "schema_mismatch":
                    causes = firing.params["causes"]
                    state.log_firing(firing, "400 INVALID_PARAMETER_VALUE", f"record {i}")
                    injected = (causes, firing.params.get("error_code"))
            if injected is not None:
                causes, code = injected
                msg = f"record {i} does not fit {table} (injected: {', '.join(causes)}; error_code {code})"
                state.tables.record_rejection(table, causes)
                return JSONResponse(error_body("INVALID_PARAMETER_VALUE", msg), status_code=400)

            size = len(json.dumps(record, separators=(",", ":")).encode("utf-8"))
            if size > state.cfg.limits.max_payload_bytes:
                return JSONResponse(
                    error_body(
                        "OUT_OF_RANGE",
                        f"record {i} is {size} bytes; the maximum is {state.cfg.limits.max_payload_bytes}",
                    ),
                    status_code=400,
                )
            try:
                rows.append(validate_object(snap, record))
            except RecordRejected as exc:
                state.tables.record_rejection(table, [c.token for c in exc.causes])
                return JSONResponse(
                    error_body("INVALID_PARAMETER_VALUE", f"record {i} does not fit {table}: {exc.render()}"),
                    status_code=400,
                )

        # The request is atomic: nothing is stored unless every record fits.
        state.tables.append(table, snap, rows, protocol="rest", stream_id=None, offsets=None)
        return JSONResponse({}, status_code=200)

    # ------------------------------------------------------------ control API

    @app.get("/_mock/health")
    async def health():
        return {"status": "ok", "tables": state.tables.names(), "faults": len(state.registry.list())}

    @app.get("/_mock/ca.pem")
    async def ca_pem():
        return PlainTextResponse(state.tls.ca_cert.read_text(), media_type="application/x-pem-file")

    @app.get("/_mock/faults")
    async def list_faults():
        return {"faults": state.registry.list(), "types": {k: v.description for k, v in FAULT_TYPES.items()}}

    @app.post("/_mock/faults", status_code=201)
    async def add_fault(spec: Dict[str, Any]):
        try:
            fault = state.registry.add(spec)
        except FaultError as exc:
            return JSONResponse(error_body("INVALID_PARAMETER_VALUE", str(exc)), status_code=400)
        return JSONResponse(fault.as_dict(), status_code=201)

    @app.delete("/_mock/faults/{fault_id}")
    async def remove_fault(fault_id: str):
        if not state.registry.remove(fault_id):
            return JSONResponse(error_body("NOT_FOUND", f"no fault {fault_id}"), status_code=404)
        return {"removed": fault_id}

    @app.delete("/_mock/faults")
    async def clear_faults():
        state.registry.clear()
        return {"faults": 0}

    @app.get("/_mock/events")
    async def events(since: int = 0, fault_id: Optional[str] = None, table: Optional[str] = None):
        return {"events": state.registry.events(since=since, fault_id=fault_id, table=table)}

    @app.get("/_mock/tables")
    async def list_tables():
        return {"tables": [state.tables.describe(n) for n in state.tables.names()]}

    @app.get("/_mock/tables/{table}")
    async def describe_table(table: str):
        if not state.tables.has(table):
            return JSONResponse(error_body("NOT_FOUND", f"no table {table}"), status_code=404)
        return state.tables.describe(table)

    @app.get("/_mock/tables/{table}/rows")
    async def table_rows(
        table: str,
        stream: Optional[str] = None,
        offset_min: Optional[int] = None,
        offset_max: Optional[int] = None,
        destination: str = "table",
        since_seq: int = 0,
        limit: int = 1000,
    ):
        if not state.tables.has(table):
            return JSONResponse(error_body("NOT_FOUND", f"no table {table}"), status_code=404)
        if destination not in ("table", "fallback", "all"):
            return JSONResponse(error_body("INVALID_PARAMETER_VALUE", "destination must be table, fallback or all"), status_code=400)
        limit = max(1, min(limit, 10000))
        recs = state.tables.read(
            table,
            stream_id=stream,
            offset_min=offset_min,
            offset_max=offset_max,
            destination=destination,
            since_seq=since_seq,
            limit=limit,
        )
        return {"table": table, "count": len(recs), "rows": [r.as_json() for r in recs]}

    @app.get("/_mock/tables/{table}/stats")
    async def table_stats(table: str, key: Optional[str] = None):
        if not state.tables.has(table):
            return JSONResponse(error_body("NOT_FOUND", f"no table {table}"), status_code=404)
        stats = state.tables.stats(table, key=key)
        stats["injected_faults"] = {}
        for e in state.registry.events(table=table):
            stats["injected_faults"][e["fault_id"]] = stats["injected_faults"].get(e["fault_id"], 0) + 1
        return stats

    @app.post("/_mock/tables/{table}/alter")
    async def alter_table(table: str, body: Dict[str, Any]):
        if not state.tables.has(table):
            return JSONResponse(error_body("NOT_FOUND", f"no table {table}"), status_code=404)
        try:
            snap = state.apply_schema_change(table, body.get("add_columns", []))
        except (SchemaError, ValueError) as exc:
            return JSONResponse(error_body("INVALID_PARAMETER_VALUE", str(exc)), status_code=400)
        return {"table": table, "schema_version": snap.version}

    @app.post("/_mock/reset")
    async def reset(body: Optional[Dict[str, Any]] = None):
        body = body or {}
        tables = body.get("tables")
        state.reset(
            tables=tables,
            faults=bool(body.get("faults", True)),
            schema=bool(body.get("schema", False)),
        )
        return {"reset": True, "tables": tables or "all"}

    return app
