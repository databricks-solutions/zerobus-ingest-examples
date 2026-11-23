"""Inject each fault and print what the client sees.

Runs an in-process mock on free local ports, then for every fault type:
injects it, drives the real Python SDK (gRPC faults) or a REST call (REST faults),
and prints the client-visible result, the rows that landed, and the logged events.

    python scripts/fault_matrix.py            # inside the test image (it has the SDK)

It writes nothing outside a temporary directory. Output is Markdown.
"""

from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import time
from pathlib import Path

import certifi
import httpx

from zerobus_mock_server import MockZerobus, from_dict

TABLE = "main.default.air_quality"
DRIFT = "main.default.drift"
TOKEN_RESOURCE = "api://databricks/workspaces/localhost/zerobusDirectWriteApi"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def details(table: str) -> str:
    return json.dumps([{"type": "unity_catalog_privileges", "privileges": ["SELECT", "MODIFY"],
                        "object_type": "TABLE", "object_full_path": table, "operations": ["zerobuswrite"]}])


def rest_token(client: httpx.Client, table: str):
    return client.post("/oidc/v1/token", data={
        "grant_type": "client_credentials", "scope": "all-apis", "resource": TOKEN_RESOURCE,
        "authorization_details": details(table)}, auth=("mock-client-id", "mock-client-secret"))


def rest_insert(client: httpx.Client, table: str, token: str, body):
    return client.post(f"/zerobus/v1/tables/{table}/insert", content=json.dumps(body),
                       headers={"authorization": f"Bearer {token}", "content-type": "application/json"})


def short(text: str, n: int = 150) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= n else text[: n - 3] + "..."


def main() -> int:
    base = Path(tempfile.mkdtemp(prefix="zb-fault-matrix-"))
    cfg = from_dict({
        "server": {"host": "127.0.0.1", "public_hosts": ["localhost"],
                   "rest_port": free_port(), "grpc_port": free_port()},
        "tls": {"cert_dir": str(base / "tls")},
        "event_log": str(base / "events.jsonl"),
        "fault_seed": 1,
        "tables": [
            {"name": TABLE, "columns": [
                {"name": "device_name", "type": "STRING", "nullable": False},
                {"name": "temp", "type": "INT"}, {"name": "humidity", "type": "LONG"}]},
            {"name": DRIFT, "columns": [
                {"name": "device_name", "type": "STRING", "nullable": False},
                {"name": "temp", "type": "INT"}]},
        ],
    }, base_dir=base)
    m = MockZerobus(cfg).start()
    bundle = base / "trust.pem"
    bundle.write_text(Path(certifi.where()).read_text() + "\n" + m.ca_path.read_text())
    os.environ["SSL_CERT_FILE"] = str(bundle)

    from zerobus.sdk.shared import TableProperties
    from zerobus.sdk.sync import ZerobusSdk

    sdk = ZerobusSdk(m.grpc_endpoint, m.unity_catalog_url)
    rest_client = httpx.Client(base_url=m.rest_base_url, verify=str(m.ca_path), timeout=30)
    rows_out: list[str] = []

    def sdk_create(table: str):
        return sdk.create_stream("mock-client-id", "mock-client-secret", TableProperties(table))

    def sdk_run(name: str, table: str, records, *, pre=None, expect: str):
        m.reset(schema=True)
        if pre:
            pre()
        started = time.monotonic()
        try:
            stream = sdk_create(table)
        except Exception as exc:  # noqa: BLE001
            return report(name, expect, f"create_stream raised: {short(type(exc).__name__ + ': ' + str(exc))}", table, started)
        observed = []
        error = None
        try:
            for rec in records:
                stream.ingest_record_offset(rec)
            stream.flush()
        except Exception as exc:  # noqa: BLE001
            error = exc
            observed.append(f"ingest/flush raised: {short(type(exc).__name__ + ': ' + str(exc))}")
        if error is None:
            try:
                stream.close()
                observed.append("stream closed cleanly")
            except Exception as exc:  # noqa: BLE001
                observed.append(f"close raised: {short(str(exc))}")
        return report(name, expect, "; ".join(observed) or "no error", table, started)

    def report(name, expect, observed, table, started):
        rows = m.tables.stats(table)
        injected = [e["fault_type"] for e in m.faults.events()]
        rows_out.append(
            f"| {name} | {expect} | {observed} | {rows['rows']} stored, {rows['fallback_rows']} fallback | "
            f"{', '.join(sorted(set(injected))) or '-'} | {time.monotonic() - started:.1f} |"
        )

    def rest_case(name, table, *, pre, body, expect, token_fault=False, repeat=1):
        """Token first (unless the fault targets the token), then the fault, then the insert(s)."""
        m.reset(schema=True)
        started = time.monotonic()
        if token_fault:
            pre()
            tok_resp = rest_token(rest_client, table)
            observed = f"token {tok_resp.status_code} {tok_resp.json().get('error')}"
        else:
            tok_resp = rest_token(rest_client, table)
            token = tok_resp.json()["access_token"]
            pre()
            calls = []
            for _ in range(repeat):
                resp = rest_insert(rest_client, table, token, body)
                calls.append(f"HTTP {resp.status_code} {short(resp.text, 90)}")
            observed = "; then ".join(calls)
        rows = m.tables.stats(table)
        injected = [e["fault_type"] for e in m.faults.events()]
        rows_out.append(
            f"| {name} | {expect} | {observed} | {rows['rows']} stored, {rows['fallback_rows']} fallback | "
            f"{', '.join(sorted(set(injected))) or '-'} | {time.monotonic() - started:.1f} |"
        )

    def add(**spec):
        m.add_fault(spec)

    one = [{"device_name": "a", "temp": 1}]
    recs3 = [{"device_name": f"s-{i}", "temp": i} for i in range(3)]
    recs10 = [{"device_name": f"s-{i}", "temp": i} for i in range(10)]

    # ---------------- REST
    rest_case("token_invalid_client", TABLE, pre=lambda: add(type="token_invalid_client", table=TABLE),
              body=one, expect="token 401 invalid_client", token_fault=True)
    rest_case("token_invalid_authorization_details", TABLE,
              pre=lambda: add(type="token_invalid_authorization_details", table=TABLE),
              body=one, expect="token 401 invalid_authorization_details", token_fault=True)
    for status in (429, 500, 503):
        rest_case(f"http_error {status} (insert)", TABLE,
                  pre=lambda s=status: add(type="http_error", protocol="rest", table=TABLE, params={"status": s}),
                  body=one, expect=f"HTTP {status}, error_code per docs table")
    rest_case("http_error 503 once, then retry", TABLE,
              pre=lambda: add(type="http_error", protocol="rest", table=TABLE, params={"status": 503}, trigger={"mode": "once"}),
              body=one, expect="first call 503; the retry succeeds", repeat=2)
    rest_case("unauthorized (insert)", TABLE, pre=lambda: add(type="unauthorized", protocol="rest", table=TABLE),
              body=one, expect="HTTP 401 UNAUTHENTICATED")
    rest_case("permission_denied (insert)", TABLE, pre=lambda: add(type="permission_denied", protocol="rest", table=TABLE),
              body=one, expect="HTTP 403 PERMISSION_DENIED")
    rest_case("table_not_found (insert)", TABLE, pre=lambda: add(type="table_not_found", protocol="rest", table=TABLE),
              body=one, expect="HTTP 404 NOT_FOUND")
    rest_case("schema_mismatch (record)", TABLE, pre=lambda: add(type="schema_mismatch", protocol="rest", table=TABLE),
              body=one, expect="HTTP 400 INVALID_PARAMETER_VALUE, causes FIELD_NOT_IN_TABLE, 4044")
    rest_case("record_too_large (record)", TABLE, pre=lambda: add(type="record_too_large", protocol="rest", table=TABLE),
              body=one, expect="HTTP 400 OUT_OF_RANGE")
    rest_case("slow_response 400 ms", TABLE, pre=lambda: add(type="slow_response", protocol="rest", table=TABLE, params={"delay_ms": 400}),
              body=one, expect="HTTP 200 after about 0.4 s")
    rest_case("schema_change adds nullable column (insert)", TABLE,
              pre=lambda: add(type="schema_change", protocol="rest", table=TABLE, trigger={"mode": "once"},
                              params={"add_columns": [{"name": "note", "type": "STRING", "nullable": True}]}),
              body=one, expect="HTTP 200; table moves to schema v2")

    # ---------------- gRPC via the SDK
    sdk_run("token_invalid_client (SDK create)", TABLE,
            [], pre=lambda: add(type="token_invalid_client", table=TABLE), expect="create raises (non-retriable)")
    sdk_run("grpc unauthorized (SDK create)", TABLE, [],
            pre=lambda: add(type="unauthorized", protocol="grpc", table=TABLE), expect="create raises UNAUTHENTICATED")
    sdk_run("grpc permission_denied (SDK create)", TABLE, [],
            pre=lambda: add(type="permission_denied", protocol="grpc", table=TABLE), expect="create raises PERMISSION_DENIED")
    sdk_run("grpc table_not_found (SDK create)", TABLE, [],
            pre=lambda: add(type="table_not_found", protocol="grpc", table=TABLE), expect="create raises NOT_FOUND")
    sdk_run("grpc_status UNAVAILABLE once (SDK create)", TABLE, recs3,
            pre=lambda: add(type="grpc_status", protocol="grpc", table=TABLE, trigger={"mode": "once"},
                            params={"code": "UNAVAILABLE"}), expect="SDK retries; stream opens; 3 rows")
    sdk_run("schema_mismatch on record 5 (SDK)", TABLE, recs10,
            pre=lambda: add(type="schema_mismatch", protocol="grpc", table=TABLE, trigger={"mode": "after", "after": 4, "times": 1}),
            expect="records 0-3 stored; error on the 5th (offset 4)")
    sdk_run("record_too_large on record 2 (SDK)", TABLE, recs3,
            pre=lambda: add(type="record_too_large", protocol="grpc", table=TABLE, trigger={"mode": "after", "after": 1, "times": 1}),
            expect="record 0 stored; offset 1 rejected (OUT_OF_RANGE)")
    sdk_run("stream_abort retryable, persist_unacked (SDK)", TABLE, recs10,
            pre=lambda: add(type="stream_abort", protocol="grpc", table=TABLE,
                            trigger={"mode": "after", "after": 5, "times": 1}, params={"retryable": True, "persist_unacked": True}),
            expect="SDK reconnects and resends; rows > 10 (duplicates)")
    sdk_run("stream_abort terminal (SDK)", TABLE, recs10,
            pre=lambda: add(type="stream_abort", protocol="grpc", table=TABLE,
                            trigger={"mode": "after", "after": 5, "times": 1}, params={"retryable": False, "persist_unacked": False}),
            expect="error surfaces; only the 5 acked rows stored")
    sdk_run("close_stream_signal 300 ms (SDK)", TABLE, recs10,
            pre=lambda: add(type="close_stream_signal", protocol="grpc", table=TABLE,
                            trigger={"mode": "after", "after": 3, "times": 1}, params={"duration_ms": 300}),
            expect="SDK reconnects after the signal; 10 distinct rows")
    sdk_run("slow_ack 400 ms (SDK)", TABLE, recs3,
            pre=lambda: add(type="slow_ack", protocol="grpc", table=TABLE, params={"delay_ms": 400}),
            expect="flush takes at least 0.4 s; 3 rows")

    # drop_acks: flush must block while acks are dropped
    m.reset(schema=True)
    add(type="drop_acks", protocol="grpc", table=TABLE, params={"count": -1})
    started = time.monotonic()
    stream = sdk_create(TABLE)
    for rec in recs3:
        stream.ingest_record_offset(rec)
    import threading
    flush_state = {}

    def do_flush():
        try:
            stream.flush()
            flush_state["result"] = "returned"
        except Exception as exc:  # noqa: BLE001
            flush_state["result"] = f"raised {type(exc).__name__}: {short(exc)}"

    t = threading.Thread(target=do_flush, daemon=True)
    t.start()
    t.join(3)
    blocked = t.is_alive()
    rows_before = m.tables.stats(TABLE)["rows"]
    m.clear_faults()
    stream.ingest_record_offset({"device_name": "s-3", "temp": 3})
    t.join(10)
    stream.close()
    rows_out.append(
        f"| drop_acks every ack (SDK flush) | flush waits for an ack; rows stored anyway | "
        f"flush still waiting after 3 s: {blocked}; {rows_before} rows at that point; after clearing the fault and one more ingest: "
        f"flush {flush_state.get('result', 'still waiting after 10 s')} | {m.tables.stats(TABLE)['rows']} stored | "
        f"drop_acks | {time.monotonic() - started:.1f} |"
    )

    # schema change while a stream is open
    m.reset(schema=True)
    stream_a = sdk_create(DRIFT)
    m.state.apply_schema_change(DRIFT, [{"name": "note", "type": "STRING", "nullable": True}])
    try:
        stream_a.ingest_record_offset({"device_name": "old", "temp": 1, "note": "hi"})
        stream_a.flush()
        a_result = "accepted (unexpected)"
    except Exception as exc:  # noqa: BLE001
        a_result = f"rejected: {short(exc)}"
    stream_b = sdk_create(DRIFT)
    stream_b.ingest_record_offset({"device_name": "new", "temp": 2, "note": "hi"})
    stream_b.flush()
    stream_b.close()
    rows_out.append(
        f"| schema_change (nullable column) while stream A is open | open stream keeps v1; new stream sees v2 | "
        f"stream A (opened before the change): {a_result}; stream B (opened after): accepted | "
        f"{m.tables.stats(DRIFT)['rows']} stored | none (control API schema change, not a fault) | - |"
    )

    m.reset(schema=True)
    stream_c = sdk_create(DRIFT)
    stream_c.ingest_record_offset({"device_name": "x", "temp": 0})
    stream_c.flush()
    m.state.apply_schema_change(DRIFT, [{"name": "region", "type": "STRING", "nullable": False}])
    stream_c.ingest_record_offset({"device_name": "y", "temp": 1})
    stream_c.flush()
    stream_c.close()
    st = m.tables.stats(DRIFT)
    rows_out.append(
        f"| breaking change (non-null column added) | rows from the old stream go to the fallback bucket | "
        f"old stream: accepted (acked); its rows are in fallback | {st['rows']} stored, {st['fallback_rows']} fallback | - | - |"
    )

    rest_client.close()
    m.stop()
    print("| Fault | Expected | What the client saw | Rows | Injected (logged) | Seconds |")
    print("|---|---|---|---|---|---|")
    for line in rows_out:
        print(line)
    print()
    print(f"Event log lines written: {len((base / 'events.jsonl').read_text().splitlines())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
