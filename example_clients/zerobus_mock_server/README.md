# Zerobus Ingest mock server

A test double for the Zerobus Ingest REST and gRPC write APIs, for integration tests of client code. It is not the Zerobus service. Where the public docs are silent, the mock states its assumption (see "Differences from the real service").

## Two ways to use it

- **Mock server, for integration tests** (this folder): a local process that serves the Zerobus REST and gRPC APIs, so a real SDK or connector can connect to it.
- **Mock libraries, for unit tests** ([`mock_lib/`](mock_lib/)): in-process fakes of the SDK for Python, Java, TypeScript, Go and Rust. They need no network or server.

Both share the same eleven failure scenarios and the same controls, so a test can make the next call fail, expire the token, or hold a failure until it's cleared, with no waiting.

## What it serves

- **REST:** `POST /oidc/v1/token` (client credentials, one table per token) and `POST /zerobus/v1/tables/<table>/insert` (a JSON array or one JSON object, atomic).
- **gRPC:** `EphemeralStream` for JSON, protobuf and Avro records, and Arrow Flight `DoPut`, on one port.
- **In memory:** rows are kept with their stream, offset and protocol. A restart starts empty.
- **Deterministic failures:** eleven named scenarios (`scenarios.json`), each started by one call: the next N matching calls fail (`fail_next`), or every call fails until cleared (`set_scenario`). No timers.
- **Read-back:** rows, the fallback bucket and a missing/duplicate coverage check, over HTTP and in process.

The scenario outcomes are shared with the mock libraries: `src/zerobus_mock_server/scenarios.json` is a copy of [`mock_lib/scenarios.json`](mock_lib/scenarios.json), and the server refuses to start if its mechanics and that file disagree.

Tested with the Python SDK `databricks-zerobus-ingest-sdk` 1.11.0. The language matrix (`tests/sdk/`) runs the other SDKs against the same server.

## Running it

The server needs TLS, so clients trust the mock's local CA, written to `ca.pem` in `tls.cert_dir` on first start. Add it to a copy of the system bundle and point `SSL_CERT_FILE` at that copy.

```bash
# the test suite, in Docker (PIP_INDEX_URL is the package index used at build time)
export PIP_INDEX_URL=https://pypi.org/simple
docker compose --profile test build tests && docker compose --profile test run --rm tests

# the server, for a client on the same host
docker compose up zerobus-mock          # REST on 127.0.0.1:8443, gRPC on :50051
curl -s --cacert ca.pem https://localhost:8443/_mock/health
```

Without Docker: `PYTHONPATH=src python -m zerobus_mock_server --config config.example.yaml`.

## Configuration

One YAML file. Relative paths resolve from the file's directory. `config.example.yaml` has every key.

| Key | Default | Meaning |
|---|---|---|
| `server.host` | `127.0.0.1` | Bind address for both listeners (`0.0.0.0` in a container). |
| `server.rest_port` | `8443` | HTTPS: token, insert, control API. |
| `server.grpc_port` | `50051` | TLS port for gRPC and Arrow Flight. |
| `server.public_hosts` | `[localhost]` | Host names in the server certificate. The first DNS label of the gRPC endpoint names the workspace. |
| `tls.cert_dir` | `./data/tls` | Local CA (`ca.pem`, `ca.key`) and server certificate. |
| `limits.max_record_bytes` | `10485760` | Record size limit; the payload limit is this minus the 19-byte header. |
| `tables` | required | Each: `name` (catalog.schema.table) and `columns` (`name`, `type`, `nullable`). |

Types are the base Delta names: `STRING`, `INT`, `SHORT`, `BYTE`, `LONG`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `BINARY`, `DECIMAL(p,s)`, `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `VARIANT`, `ARRAY<T>`, `MAP<K,V>`, `STRUCT<...>`. Nested parts are not checked.

## Control API

Plain HTTP on the REST listener, for test code only. Bind to `127.0.0.1`. The API has no authentication.

| Call | Effect |
|---|---|
| `POST /_mock/fail-next` `{scenario, count=1, after=0, table="*", params}` | The next `count` matching calls, after `after` of them, fail. |
| `POST /_mock/scenario` `{scenario, table="*", params}` | Every matching call fails until cleared. |
| `DELETE /_mock/scenario` | Clears the held scenario. |
| `POST /_mock/expire-token` `{count=1}` | The next `count` authenticated calls find their token expired. |
| `POST /_mock/reset` `{tables, schema}` | Clears rows, scenarios and tokens. `schema: true` also restores the configured columns. |
| `GET /_mock/tables/<t>/rows?destination=table\|fallback\|all&limit=` | Stored rows. |
| `GET /_mock/tables/<t>/coverage?expected=N&key=col` | Missing and duplicate keys `0..N-1`. |
| `GET /_mock/health` | Liveness. |

`MockControl` (`zerobus_mock_server.control`) wraps these calls. `expire_token` and `fail_next` take the same arguments.

## Scenarios

What the caller sees is the Python SDK's behavior, measured with SDK 1.11.0. "Retryable" means the SDK's own recovery handles it. The text in `scenarios.json` (`message`) is what the SDK error must contain.

| Scenario | Fires at | Caller sees | SDK test |
|---|---|---|---|
| `auth-failure` | token endpoint: 401 `invalid_client` | `create_stream` raises at once | yes |
| `table-not-found` | create and insert: NOT_FOUND / 404 | `create_stream` raises at once | yes (gRPC) |
| `schema-mismatch` | record: a field the table lacks (4044) | `ingest` or `flush` raises `does not fit`; earlier records stored | yes |
| `record-too-large` | record: over the size limit (OUT_OF_RANGE) | terminal error from `ingest` or `flush` | yes |
| `server-unavailable` | token, insert, create, in-stream: UNAVAILABLE / 503 | retried by the SDK, then raises; held until cleared | yes |
| `stream-abort-retryable` | ingest: UNAVAILABLE after a stored, unacked submission | no error; resend gives duplicates | yes (gRPC, Arrow, Avro) |
| `stream-abort-fatal` | ingest: INVALID_ARGUMENT, terminal stream error | error from `ingest`/`flush`; only acked rows stored | yes |
| `graceful-close` | ingest: CloseStreamSignal, then `duration_ms` | pause, drain, reconnect; no error | yes |
| `no-acks` | ack: acknowledgements withheld | `flush` times out (`timed out`); rows stored | yes |
| `schema-change-while-open` | insert or ingest: non-nullable `region` added | success; the open stream's rows are in `fallback` | yes |
| `token-expired` | `expire_token`: the next authenticated call | at create the SDK refreshes and succeeds; mid-stream the stream fails with `expired` | yes |

## Testing patterns

```python
# conftest.py
from zerobus_mock_server.pytest_plugin import zerobus_mock, zerobus_mock_server  # noqa: F401

# test_ingest.py
def test_retries_after_abort(zerobus_mock, zerobus_mock_server):
    zerobus_mock.fail_next("stream-abort-retryable", after=4)   # the 5th submission is aborted, once
    stream = make_sdk_stream(zerobus_mock_server)               # your code under test
    ingest_and_flush(stream, records)
    assert zerobus_mock.coverage("main.default.events", expected=10, key="device_name")["missing"] == 0
```

`zerobus_mock` resets the server before each test. Override the `zerobus_mock_config` fixture to change the tables or the record limit.

## Differences from the real service

- In memory. A restart starts empty.
- Token expiry is simulated by `expire_token`, and tokens also expire after a fixed one-hour lifetime.
- Failures are exact (the Nth call), not probabilistic.
- The control API has no authentication. Bind it to 127.0.0.1.
- Assumptions where the docs are silent: offsets run 0, 1, 2 per stream; REST inserts are atomic; a breaking schema change (a non-nullable column added while a stream is open) sends that stream's rows to an in-memory fallback bucket; any non-empty client credentials are accepted.
- Records are checked by name and basic type only. Nested values, decimal precision, timestamp zones, Avro logical types beyond date, timestamp and decimal, and protobuf maps are not checked.

## Layout

- `src/zerobus_mock_server/` the server (`runner.py` starts both listeners; `rest.py`, `grpc_server.py` and `flight.py` are the protocols; `scenarios.py` and `faults.py` the failures; `tables.py` and `schema.py` the rows and checks).
- `tests/` the suite. `tests/sdk/` the SDK language matrix (`run_all.sh`, one lane per language).
