"""REST listener: OAuth token, insert (JSON array or object, atomic), and the /_mock control API (test code only)."""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, Optional
from urllib.parse import parse_qs

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from .auth import TTL_SECONDS, table_from_details
from .scenarios import ScenarioError, UnknownScenario
from .schema import RecordRejected, SchemaError, validate_record


def _error(code: str, message: str, status: int) -> JSONResponse:
    return JSONResponse({"error_code": code, "message": message}, status_code=status)


def _control(fn: Callable[[], Any]) -> Response:
    """One control call: 404 for an unknown scenario, 400 for bad parameters."""
    try:
        fn()
    except UnknownScenario as exc:
        return _error("NOT_FOUND", str(exc), 404)
    except (ScenarioError, SchemaError, TypeError, ValueError) as exc:
        return _error("INVALID_PARAMETER_VALUE", str(exc), 400)
    return JSONResponse({"ok": True})


def create_rest_app(mock) -> FastAPI:
    app = FastAPI(title="Zerobus Ingest mock server (test double)", docs_url=None, redoc_url=None, openapi_url=None)

    @app.post("/oidc/v1/token")
    async def token(request: Request):
        form = {k: v[-1] for k, v in parse_qs((await request.body()).decode("utf-8", "replace"),
                                              keep_blank_values=True).items()}
        oauth = lambda error, text, status=401: JSONResponse(  # noqa: E731
            {"error": error, "error_description": text}, status_code=status)
        if not (form.get("client_id") or request.headers.get("authorization", "").lower().startswith("basic ")):
            return oauth("invalid_client", "client authentication failed")
        try:
            table = table_from_details(form.get("authorization_details", ""), mock.tables.names())
        except ValueError as exc:
            return oauth("invalid_authorization_details", str(exc))
        for rule in mock.scenarios.fire("token", table):
            if rule.action == "invalid_client":
                return oauth("invalid_client", "client authentication failed (injected)")
            return _error("UNAVAILABLE", "injected: the server is unavailable", 503)
        return JSONResponse({"access_token": mock.tokens.issue(table), "token_type": "Bearer",
                             "expires_in": TTL_SECONDS})

    @app.post("/zerobus/v1/tables/{table}/insert")
    async def insert(table: str, request: Request):
        ctype = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
        if ctype != "application/json":
            return _error("INVALID_PARAMETER_VALUE",
                          f"unsupported Content-Type {ctype or '(none)'}; use application/json", 415)
        if not mock.tables.has(table):
            return _error("NOT_FOUND", f'Table "{table}" cannot be found.', 404)
        for rule in mock.scenarios.fire("insert", table):
            if rule.action == "add_columns":
                mock.add_schema_change(table)
            elif rule.action == "not_found":
                return _error("NOT_FOUND", f'Table "{table}" cannot be found. (injected)', 404)
            else:
                return _error("UNAVAILABLE", "injected: the server is unavailable", 503)
        auth = request.headers.get("authorization", "")
        check = mock.tokens.check(auth[7:].strip() if auth.lower().startswith("bearer ") else None, table)
        if check == "UNAUTHENTICATED":
            return _error("UNAUTHENTICATED", "missing, unknown or expired access token", 401)
        if check == "PERMISSION_DENIED":
            return _error("PERMISSION_DENIED", f"the token was not issued for {table}", 403)
        try:  # a JSON array of objects, or one JSON object
            parsed = json.loads((await request.body()).decode("utf-8"))
            records = parsed if isinstance(parsed, list) else [parsed]
            if not all(isinstance(r, dict) for r in records):
                raise ValueError
        except (UnicodeDecodeError, ValueError):
            return _error("INVALID_PARAMETER_VALUE", "the body must be a JSON object or an array of JSON objects", 400)

        snap = mock.tables.snapshot(table)
        limit = mock.cfg.max_payload_bytes
        rows = []
        for i, record in enumerate(records):
            for rule in mock.scenarios.fire("record", table):
                if rule.action == "too_large":
                    return _error("OUT_OF_RANGE", f"record {i} exceeds the maximum message size (injected)", 400)
                return _error("INVALID_PARAMETER_VALUE",
                              f"record {i} does not fit {table} (injected: FIELD_NOT_IN_TABLE)", 400)
            size = len(json.dumps(record, separators=(",", ":")).encode("utf-8"))
            if size > limit:
                return _error("OUT_OF_RANGE",
                              f"record {i} is {size} bytes, over the maximum message size of {limit}", 400)
            try:
                rows.append(validate_record(snap.columns, table, record))
            except RecordRejected as exc:
                return _error("INVALID_PARAMETER_VALUE", f"record {i} does not fit {table}: {exc}", 400)
        mock.tables.append(table, snap, rows, protocol="rest", stream_id=None)
        return JSONResponse({}, status_code=200)

    # ------------------------------------------------------------ control API

    @app.get("/_mock/health")
    async def health():
        return {"status": "ok", "tables": mock.tables.names()}

    @app.post("/_mock/fail-next")
    async def fail_next(body: Dict[str, Any]):
        return _control(lambda: mock.scenarios.fail_next(
            str(body.get("scenario", "")), count=int(body.get("count", 1)), after=int(body.get("after", 0)),
            table=body.get("table", "*"), params=body.get("params") or {}))

    @app.post("/_mock/expire-token")
    async def expire_token(body: Optional[Dict[str, Any]] = None):
        count = int((body or {}).get("count", 1))
        if count < 1:
            return _error("INVALID_PARAMETER_VALUE", "count must be >= 1", 400)
        mock.tokens.expire_next(count)
        return {"ok": True}

    @app.post("/_mock/scenario")
    async def set_scenario(body: Dict[str, Any]):
        return _control(lambda: mock.scenarios.set_scenario(
            str(body.get("scenario", "")), table=body.get("table", "*"), params=body.get("params") or {}))

    @app.delete("/_mock/scenario")
    async def clear_scenario():
        mock.scenarios.clear_scenario()
        return {"cleared": True}

    @app.post("/_mock/reset")
    async def reset(body: Optional[Dict[str, Any]] = None):
        """Clears rows, scenarios, issued tokens and the expiry budget. ``schema`` also restores the columns."""
        body = body or {}
        mock.reset(body.get("tables"), schema=bool(body.get("schema", False)))
        return {"reset": True, "tables": body.get("tables") or "all"}

    @app.get("/_mock/tables/{table}/rows")
    async def table_rows(table: str, destination: str = "table", limit: int = 1000):
        if not mock.tables.has(table):
            return _error("NOT_FOUND", f"no table {table}", 404)
        if destination not in ("table", "fallback", "all"):
            return _error("INVALID_PARAMETER_VALUE", "destination must be table, fallback or all", 400)
        recs = mock.tables.read(table, destination, limit=max(1, min(limit, 10000)))
        return {"table": table, "count": len(recs), "rows": [r.as_json() for r in recs]}

    @app.get("/_mock/tables/{table}/coverage")
    async def table_coverage(table: str, expected: int, key: str):
        if not mock.tables.has(table):
            return _error("NOT_FOUND", f"no table {table}", 404)
        if expected < 0:
            return _error("INVALID_PARAMETER_VALUE", "expected must be >= 0", 400)
        try:
            return mock.tables.coverage(table, expected, key)
        except SchemaError as exc:
            return _error("INVALID_PARAMETER_VALUE", str(exc), 400)

    return app
