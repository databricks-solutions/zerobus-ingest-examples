# Zerobus Ingest mock server

## Overview

This is a test double for the Zerobus Ingest REST and gRPC write APIs. It is not the Zerobus service, and it does not claim to be. Use it to test a client's behaviour, including its error handling, retries and reconnects, without a workspace, a service principal or a network path to Zerobus.

It implements:

- **REST.** The OAuth token endpoint (`POST /oidc/v1/token`) and the insert endpoint (`POST /zerobus/v1/tables/<catalog.schema.table>/insert`). Request and response shapes follow the Zerobus REST docs.
- **gRPC.** The `EphemeralStream` service from the SDK repository's `zerobus_service.proto`, so the Python and Java SDKs can connect with their normal configuration. Records can be JSON, protobuf, Avro, or a batch of any of them. `PersistentStream` and `RetireStream` work for JSON and protobuf. Arrow Flight (`DoPut` on the same port) carries Arrow batches. See the format table below.
- **Tables in memory.** Each accepted record is stored with its stream, offset, receive time and schema version. Nothing is written to disk. A restart starts empty.
- **Fault injection.** A catalogue of client-visible failures, each with a scope (protocol, table, stream) and a trigger (`always`, `once`, `every_nth`, `probability` or `after`). Every injected fault is written to an event log.
- **Control API.** Under `/_mock/`: fault management, row and statistics reads, schema changes, reset, and the CA certificate. The control API is not part of Zerobus, and it has no authentication. Bind it to `127.0.0.1` (the default).

Tested with the Python SDK `databricks-zerobus-ingest-sdk` 1.11.0 and the Java SDK `com.databricks:zerobus-ingest-sdk` 1.8.1.

## Prerequisites

- Docker 20.10 or later, with Compose v2 for `docker compose`. Build and run inside Docker. On hosts whose Python or glibc is old, use `--network host` (as the commands below do).
- A pip index for building the image. Pass it as the `PIP_INDEX_URL` build argument. The default is the public PyPI index.
- No Databricks credentials, workspace, or service principal. The mock accepts any non-empty client ID and secret unless `auth.require_known_clients` is set.
- To run the examples: the Python SDK (`databricks-zerobus-ingest-sdk==1.11.0`) or the Java SDK (`com.databricks:zerobus-ingest-sdk:1.8.1`). The SDKs only connect over TLS, so the mock's local CA must be trusted (see Quick start).

## Configuration reference

The mock reads one YAML file (`--config`). Relative paths resolve from the file's directory. `config.example.yaml` has every key with placeholder values.

| Key | Default | Meaning |
|---|---|---|
| `server.host` | `127.0.0.1` | Bind address for both listeners. Use `0.0.0.0` inside a container. |
| `server.rest_port` | `8443` | HTTPS port: token endpoint, insert endpoint, control API. |
| `server.grpc_port` | `50051` | TLS port for gRPC `EphemeralStream`. |
| `server.public_hosts` | `[localhost]` | Host names clients use. They go into the server certificate. The SDK takes the first DNS label of the gRPC endpoint as the workspace ID. |
| `server.workspace_id` | unset | When set, token requests must name this workspace ID. Unset accepts any. |
| `tls.cert_dir` | `./data/tls` | Where the local CA (`ca.pem`, `ca.key`) and the server certificate are written. Generated on first start. |
| `auth.token_ttl_seconds` | `3600` | Token lifetime. The docs say tokens last one hour. |
| `auth.require_known_clients` | `false` | When `true`, only the `auth.clients` pairs get tokens. |
| `auth.clients` | `[]` | `client_id` / `client_secret` pairs. |
| `limits.max_record_bytes` | `10485760` | Record size limit (10 MB, from the docs). Lower it in tests. |
| `limits.message_header_bytes` | `19` | Header bytes that count inside the size limit (from the docs). The payload limit is `max_record_bytes - message_header_bytes`. |
| `fault_seed` | `0` | Seed for probability triggers, so a run is repeatable. |
| `event_log` | unset | Path of a JSON Lines file. Each injected fault is appended as one line. Unset keeps events in memory only. |
| `tables` | required | List of tables (see below). |
| `faults` | `[]` | Faults active from startup (same shape as `POST /_mock/faults`). |

Each table:

```yaml
tables:
  - name: main.default.sensor_events        # catalog.schema.table, letters, digits and underscores
    rescue_column: rescue                   # optional; must be nullable and VARIANT
    columns:
      - {name: device_name, type: STRING, nullable: false}
      - {name: temp, type: INT}
      - {name: rescue, type: VARIANT}
```

Column types are the Delta names used by the docs: `STRING`, `INT`, `LONG`, `SHORT`, `BYTE`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `BINARY`, `DECIMAL(p,s)`, `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `VARIANT`, `ARRAY<T>`, `MAP<K,V>`, and `STRUCT<name: T, ...>` (a field can end in `NOT NULL`). Aliases such as `INTEGER`, `BIGINT` and `TINYINT` are accepted.

Config validation rejects two column names that collide after underscore folding (for example `order_id` and `orderId`), which the docs say Zerobus can't write (error 4044).

## Quick start

Build the image (the test target includes the test suite):

```bash
PIP_INDEX_URL=https://pypi-proxy.cloud.databricks.com/simple docker compose build zerobus-mock   # or your index
```

Start the mock with the example configuration:

```bash
docker compose up -d zerobus-mock
docker compose exec -T zerobus-mock cat /config/data/tls/ca.pem > ca.pem     # the CA the SDKs must trust
curl -s --cacert ca.pem https://localhost:8443/_mock/health
```

Point an SDK at it. The gRPC endpoint and the workspace (Unity Catalog) URL are both local. The SDKs use the system trust store, so add the mock's CA to a bundle and set `SSL_CERT_FILE` to it.

```python
# Python SDK 1.11.0. Set SSL_CERT_FILE to a bundle of system roots plus ca.pem first.
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties

sdk = ZerobusSdk("https://localhost:50051", "https://localhost:8443")
stream = sdk.create_stream("mock-client-id", "mock-client-secret",
                           TableProperties("main.default.air_quality"))
for i in range(10):
    stream.ingest_record_offset({"device_name": f"sensor-{i}", "temp": 20 + i, "humidity": 50})
stream.flush()
stream.close()
```

```java
// Java SDK 1.8.1. Run with SSL_CERT_FILE set to the same bundle.
ZerobusSdk sdk = new ZerobusSdk("https://localhost:50051", "https://localhost:8443");
ZerobusJsonStream stream = sdk.streamBuilder()
    .table("main.default.air_quality")
    .oauth("mock-client-id", "mock-client-secret")
    .json()
    .build()
    .join();
stream.ingestRecordOffset("{\"device_name\": \"sensor-1\", \"temp\": 22, \"humidity\": 55}");
stream.flush();
stream.close();
```

Read what landed:

```bash
curl -s --cacert ca.pem "https://localhost:8443/_mock/tables/main.default.air_quality/rows?limit=5"
curl -s --cacert ca.pem "https://localhost:8443/_mock/tables/main.default.air_quality/stats?key=device_name"
```

Run the test suite (it starts its own mock on free ports):

```bash
docker compose --profile test run --rm tests
```

To use the mock in the same process as a Python test, start it with `MockZerobus.from_config_file(path).start()`. Each table is then available as a handle, `mock.tables["main.default.air_quality"]`, with `.rows` (landed rows, in write order), `.fallback`, `.records` (with stream, offset and receive time), `.stats(key=...)` and `.reset()`. Faults can be added with `mock.add_fault({...})`.

## Record formats and SDK support

What the mock accepts, and which SDK clients were run against it (tests/sdk, see "Running the SDK matrix").

| Format | Mock path | Notes |
|---|---|---|
| JSON | `EphemeralStream`, record type JSON | One object per record; rows in the table's columns. |
| Protobuf | `EphemeralStream`, record type PROTO | The `DescriptorProto` (the message) is sent as `descriptor_proto`. A file descriptor is also accepted. Checked against the table at create. |
| Arrow | Flight `DoPut` on the gRPC port | Table and token in the headers (no FlightDescriptor, as the Rust SDK sends it). The first FlightData is the schema; each batch carries `{"offset_id": n}` in `app_metadata`. Schema must match the table: names, order, types, nullability (`Utf8`/`LargeUtf8`, `Binary`/`LargeBinary` accepted). VARIANT columns over Arrow are not accepted by the mock. |
| Avro | `EphemeralStream`, record type AVRO | Writer schema in `avro_schema_json`, fixed for the stream, and checked against the table. Records are raw datums (or `AvroRecordBatch`). Container files, framed (schema-registry) records, and trailing bytes are refused. Named-type references are refused (the docs require inline types). Unions are limited to `["null", T]`, `int`/`long`, and `float`/`double`. |

Persistent streams (`PersistentStream`): JSON and protobuf only. The Python and Rust SDKs call it for those two formats. Arrow and Avro persistent streams are skipped, because no SDK calls them.

### SDK language x format matrix

Client versions are pinned in each `tests/sdk/clients/<lang>/` image. "not supported (SDK)" means the SDK's public API has no such format, checked in the SDK source; the cell is not run.

| Language (SDK, version) | JSON | Protobuf | Arrow | Avro |
|---|---|---|---|---|
| Python (`databricks-zerobus-ingest-sdk` 1.11.0, `[arrow,avro]`) | PASS | PASS | PASS | PASS |
| Java (`com.databricks:zerobus-ingest-sdk` 1.8.1) | PASS | PASS | PASS | PASS |
| TypeScript (`@databricks/zerobus-ingest-sdk` 1.5.0, npm; Avro is Beta) | PASS | PASS | PASS | PASS |
| Rust (`databricks-zerobus-ingest-sdk`, repo `rust/sdk`, built from source) | PASS | PASS | PASS | PASS |
| Go, pure (`github.com/databricks/zerobus-sdk/purego`, repo `purego/`, built from source; no cgo) | PASS | PASS | not supported (SDK) | not supported (SDK) |
| Go, cgo (`github.com/databricks/zerobus-sdk/go`, repo `go/`, built from source over the Rust core) | PASS | PASS | PASS | not supported (SDK) |
| C++ (repo `cpp/`, built from source over the Rust core; compiled with g++) | PASS | PASS | PASS | not supported (SDK) |

Each cell runs four scenarios: `normal` (N=20 records, all stored), `abort` (a retryable mid-stream abort after two submissions; the SDK reconnects and resends), `token401` (the token endpoint answers 401 invalid_client; nothing stored), and `mismatch` (a field that is not a column; 4044). A cell is PASS only when all four pass. The per-run verdicts are in `tests/sdk/check.py`.

Notes on individual SDKs:
- Python needs the `[arrow,avro]` extras (the Avro encoder is `fastavro`).
- Java: Arrow needs `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`; the client's `run.sh` passes it.
- TypeScript: a failed ingest can surface as a generic "Stream closed" error; the client flushes one time after a failed ingest to report the server's cause. Arrow schema fields must set `nullable` to match the table.
- Rust: pre-encoded Avro datums are wrapped in `AvroBytes`.
- C++: `StreamOptions.record_type` defaults to Proto, so the JSON client sets `RecordType::Json`. Arrow takes IPC bytes (the client reads fixtures generated by pyarrow, since there is no Arrow C++ library in the image).
- Go (cgo) Arrow takes IPC bytes too; the fixtures come from the same generator.

### Running the SDK matrix

Requirements: Docker (run with `--network host`), a checkout of the zerobus-sdk monorepo in `ZEROBUS_SDK_SRC` (a sparse checkout needs `rust`, `go`, `purego`, `cpp`, `typescript`, `java`, `python`, `conformance`), and access to package proxies at build time.

```bash
cd zerobus_mock_server
tests/sdk/run_all.sh                 # every language in tests/sdk/matrix.conf
tests/sdk/run_all.sh python rust     # a subset, in the order given
```

Run one language at a time. Every lane starts the mock container `zb-sdk-matrix-mock` on ports 18543 (REST) and 15551 (gRPC); two lanes in parallel collide on those names and ports.

What a lane does: builds the mock test image if needed; starts the mock with `tests/sdk/mock-config.yaml` (CA and results in `$ZB_RUN`); builds the client image; runs the client's build step where the SDK is compiled from the monorepo (Rust, cgo, C++); generates fixtures where needed (Arrow IPC from pyarrow); then for each supported format and scenario it resets the table, injects the scenario's fault through the control API, runs the client, and checks the verdict. The matrix is written to `$ZB_RUN/matrix.md` and printed.

Environment variables:

| Variable | Default | Used for |
|---|---|---|
| `ZEROBUS_SDK_SRC` | none (required) | a checkout of https://github.com/databricks/zerobus-sdk, mounted into the build steps |
| `ZB_RUN` | `/tmp/zb-sdk-matrix` | scratch for the CA, results, logs and build caches |
| `PIP_INDEX_URL` | the public PyPI index | Python packages (mock image, Python client) |
| `NPM_CONFIG_REGISTRY` | the public npm registry | TypeScript client |
| `GOPROXY` | the public Go module proxy | Go modules (pure Go and cgo clients) |
| `MAVEN_MIRROR` | the public Maven Central mirror | Java client dependencies |

Rust and C++ vendor their crates with `tests/sdk/clients/rust/vendor.py` (stdlib only, run in the Python image): each registry package in the SDK's `Cargo.lock` is downloaded from `static.crates.io`, checked against its checksum, and cargo then builds offline. The vendor step forces IPv4 because IPv6 stalls on some hosts.

Each lane's build log is `$ZB_RUN/build-<lang>.log`; each cell's client output is `$ZB_RUN/res/<lang>-<format>-<scenario>.out`.

## Schema

Tables are defined in the config. The mock does not create tables in Unity Catalog and does not read UC metadata.

A record must hold every non-nullable column. Nullable columns can be omitted and are stored as NULL. Fields that aren't columns are rejected, unless the table has a rescue column. Then they go into the rescue column as a JSON object. The rescue rules follow the docs:

- A field that matches a column is written to it.
- A field that isn't a column goes to the rescue column.
- A value whose type doesn't fit a nullable column goes to the rescue column. A value that doesn't fit a non-nullable column is rejected, because the rescue column can't supply a required value.
- A JSON `null` is the same as an absent field.
- The record can't set the rescue column itself.

Protobuf streams fix their fields when the stream is created. The descriptor is checked one time, at creation: unknown fields, incompatible types and missing non-nullable columns are refused. Records are then validated with the same rules, and the rescue column is not used (the docs limit it to JSON).

Protobuf type mapping (from the docs' type-support table): `STRING`, `VARIANT` and `DECIMAL` map to `string`; `INT`, `SHORT`, `BYTE` and `DATE` map to `int32`; `LONG`, `TIMESTAMP` and `TIMESTAMP_NTZ` map to `int64`; `FLOAT`, `DOUBLE`, `BOOLEAN` and `BINARY` map to `float`, `double`, `bool` and `bytes`; `ARRAY<T>` to `repeated`, `MAP` to `map`, and `STRUCT` to a nested message.

Stored rows are returned by the read API as JSON: a `DATE` as an ISO date, a `TIMESTAMP` as an ISO timestamp, a `DECIMAL` as text, `BINARY` as base64.

## Fault injection

A fault is a JSON object (YAML in the config, JSON on the control API):

```json
{"type": "http_error", "protocol": "rest", "table": "main.default.air_quality",
 "trigger": {"mode": "once"}, "params": {"status": 503}}
```

| Field | Values | Notes |
|---|---|---|
| `type` | see the table below | Required. |
| `protocol` | `any` (default), `rest`, `grpc` | Must be one the type supports. |
| `table` | `*` (default) or a table name | Scope. |
| `stream` | `*` (default) or a stream ID such as `mock-stream-3` | gRPC only. Stream IDs appear in the create response and in the read API. |
| `trigger` | `{"mode": "always"}`, `{"mode": "once"}`, `{"mode": "every_nth", "n": 3}`, `{"mode": "probability", "probability": 0.25}`, `{"mode": "after", "after": 5, "times": 1}` | `after` fires after `after` matching events have passed, for `times` firings (no limit if omitted). |
| `params` | per type | Unknown params are rejected. |

Each fault type applies at one or more points: `token` (token request), `create` (gRPC create-stream), `insert` (REST request), `record` (one record, before validation), `ingest` (one accepted gRPC message, before it is stored), `ack` (one acknowledgement about to be sent).

| Type | Protocol | Point | What the client sees | Params (default) |
|---|---|---|---|---|
| `token_invalid_client` | rest | token | HTTP 401 `invalid_client`. The SDK raises a non-retriable error. | |
| `token_invalid_authorization_details` | rest | token | HTTP 401 `invalid_authorization_details`. | |
| `http_error` | rest | token, insert | The given HTTP status with a docs-style error body. 429 is `RESOURCE_EXHAUSTED`, 500 is `INTERNAL_ERROR`, 503 is `UNAVAILABLE`. | `status` (503), `message`, `retry_after_seconds` |
| `slow_response` | rest | insert | The response is delayed. A very large delay acts like no response. | `delay_ms` (1000) |
| `unauthorized` | rest, grpc | insert, create | HTTP 401 `UNAUTHENTICATED`, or gRPC `UNAUTHENTICATED`. | |
| `permission_denied` | rest, grpc | insert, create | HTTP 403 `PERMISSION_DENIED`, or gRPC `PERMISSION_DENIED`. | |
| `table_not_found` | rest, grpc | insert, create | HTTP 404 `NOT_FOUND`, or gRPC `NOT_FOUND`. | |
| `schema_mismatch` | rest, grpc | record | The record is rejected with the given causes. Over gRPC the stream fails with `SCHEMA_VALIDATION_FAILED` details. | `causes` (`[FIELD_NOT_IN_TABLE]`), `error_code` (`"4044"`) |
| `record_too_large` | rest, grpc | record | HTTP 400 `OUT_OF_RANGE`, or gRPC `OUT_OF_RANGE`. | |
| `grpc_status` | grpc | create | Stream creation fails with the given gRPC status. | `code` (`UNAVAILABLE`), `message` |
| `stream_abort` | grpc | ingest | The stream ends mid-flight. `retryable: true` gives `UNAVAILABLE`, so the SDK reconnects and resends what it hasn't seen acknowledged. `false` gives `INVALID_ARGUMENT`. With `persist_unacked: true` the triggering batch is stored before the stream ends, without an acknowledgement. | `retryable` (true), `persist_unacked` (true) |
| `close_stream_signal` | grpc | ingest | The server sends `CloseStreamSignal`, then ends the stream OK after the duration. | `duration_ms` (1000) |
| `slow_ack` | grpc | ack | One acknowledgement is delayed. | `delay_ms` (1000) |
| `drop_acks` | grpc | ack | Acknowledgements are dropped: `count` of them, or `-1` for every ack while the fault is active. Records are still stored. A flush waits until an ack arrives. | `count` (-1) |
| `schema_change` | rest, grpc | insert, ingest | Adds columns to the table now. Open gRPC streams keep the schema they were created with. New streams and REST requests see the change. | `add_columns` (list of `{name, type, nullable}`) |

What the SDKs do with these errors is set by the SDK, not by the mock. Checked with SDK 1.11.0: a `create_stream` that fails with `UNAUTHENTICATED` or `PERMISSION_DENIED` is retried one time after about 2 s, then raised. A `grpc_status` of `UNAVAILABLE` is retried, and the stream opens on the next attempt. A mid-stream `UNAVAILABLE` makes the SDK reconnect and resend. Mid-stream `INVALID_ARGUMENT` and `OUT_OF_RANGE` end the stream and raise from `ingest`, `flush` or `close`. Failures at create (`NOT_FOUND`, `PERMISSION_DENIED`, `UNAUTHENTICATED`, token errors) raise from `create_stream`. `scripts/fault_matrix.py` prints the observed result for each fault.

Control API:

| Method and path | Purpose |
|---|---|
| `GET /_mock/health` | Status, table names, fault count. |
| `GET /_mock/ca.pem` | The CA certificate clients must trust. |
| `GET /_mock/faults`, `POST /_mock/faults`, `DELETE /_mock/faults/{id}`, `DELETE /_mock/faults` | List, add (201), remove one, or clear all faults. |
| `GET /_mock/events?since=<seq>&fault_id=&table=` | Injected faults, in order. |
| `GET /_mock/tables`, `GET /_mock/tables/{table}` | Schema version, columns, row counts. |
| `GET /_mock/tables/{table}/rows?stream=&offset_min=&offset_max=&destination=table\|fallback\|all&since_seq=&limit=` | Stored records, in write order (`limit` up to 10000). |
| `GET /_mock/tables/{table}/stats?key=<column>` | Row counts by stream and protocol, rejections, injected faults by ID, and duplicate counts for `key`. |
| `POST /_mock/tables/{table}/alter` | Add columns now: `{"add_columns": [{"name": ..., "type": ..., "nullable": ...}]}`. |
| `POST /_mock/reset` | Clear rows (for `tables` or all), faults and events. `"schema": true` restores configured schemas, and `"faults": false` keeps faults. Tokens are cleared too. |

## Differences from the real service

The mock is a test double. Behaviour that the public docs don't specify is an assumption, and each assumption is listed here.

**Storage and durability**

- Tables are in memory. Nothing is written to Delta or to disk, and a restart starts empty. "Durable" in the mock means stored in the mock's memory before the acknowledgement is sent.
- The docs give a time to durability of about 150 ms and a time to table of about 5 seconds (quotas page). The mock acknowledges as soon as it has stored a group of submissions, and it doesn't model either latency.
- A breaking schema change (a non-nullable column added after a stream opened) sends that stream's rows to an in-memory fallback bucket (`destination=fallback`). The docs describe a Parquet fallback directory, `_zerobus/table_rejected_parquets/`, instead.
- Rows are not stored in Unity Catalog, and there's no table feature handling (partitioning, liquid clustering, column mapping).
- `VARIANT` columns hold a JSON string. The docs describe a VARIANT column, and the mock has no VARIANT type.

**Authentication and authorization**

- The mock accepts any non-empty client ID and secret, unless `require_known_clients` is set. It doesn't check secrets against a store and doesn't check UC grants.
- Token requests are checked for shape: `grant_type`, `scope=all-apis`, the `resource` format, and `authorization_details` naming exactly one table with `SELECT`/`MODIFY` privileges. Tokens are bound to one table and expire after `token_ttl_seconds`.
- The `invalid_authorization_details` error and its 401 status follow the test specification for this mock. The public docs don't name this error, so a real service may answer with a different status.
- Every configured table is granted to every client. Permission failures come from injected faults: `permission_denied` (REST and gRPC) and `token_invalid_authorization_details` (token request).

**Protocol**

- Stream offsets must be 0, 1, 2, … on each connection. The mock enforces this and fails the stream otherwise. The public docs don't state an offset rule. The conformance mock in the SDK repository enforces the same rule.
- A batch message is one submission with one offset, which is consistent with the docs' description that a batch "is assigned a logical offset".
- Within a stream, a validation failure stops the stream after the records before it are stored and acknowledged. The docs don't specify this.
- REST insert requests are atomic: if any record fails, none are stored. The docs don't say whether a request is atomic.
- The SDK core waits for response headers before it sends `create_stream`, so the mock sends initial metadata at the start of the call. A server that waits for the create message first stalls the SDK (observed with Python SDK 1.11.0).
- `record_type` unset defaults to PROTO (per the proto field comment). The `RECORD_TYPE_UNSPECIFIED` enum comment says it returns an error. The mock follows the field comment for unset and refuses an explicit UNSPECIFIED.
- Protobuf descriptors: the SDK sends a serialized `DescriptorProto` (the message) as `descriptor_proto`, even when it is given a serialized `FileDescriptorProto` or a generated `Descriptor` (observed with SDK 1.11.0). The docs' Python example uses `DESCRIPTOR.file.serialized_pb`, which is a file. The mock accepts both and uses the single message they describe. The Rust core requires the descriptor to be self-contained, so nested message types must be nested inside that message and not defined in another file.
- Limits: the payload limit is `max_record_bytes - message_header_bytes`. The docs say "maximum size of 10 MB" with "19 bytes of header", which can also be read as 10,485,760 for the whole message. The mock uses the stricter reading.
- JSON encodings not in the docs (assumptions): `DATE` as days since the epoch and `TIMESTAMP` as microseconds since the epoch, both as JSON integers, matching the protobuf mapping. `BINARY` as base64 text. `DECIMAL` as decimal text. `MAP` keys are JSON object keys, converted to the key type. Nested struct fields that aren't in the struct are rejected.
- Error codes: the REST error body uses the docs' codes (`INVALID_PARAMETER_VALUE`, `OUT_OF_RANGE`, `NOT_FOUND`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, `INTERNAL_ERROR`). The gRPC codes follow the docs' table. Schema failures over gRPC carry an `ErrorInfo` detail with reason `SCHEMA_VALIDATION_FAILED`, domain `zerobus.databricks.com`, and `causes` and `error_code` metadata, which is how the Rust core reads them. The `4044` code appears in the docs for a column-name collision. It was also observed for an unrecognised field when testing against the service, and the mock uses it for both.
- Retry behaviour: the docs say `INTERNAL_ERROR` (500) should not be retried. The SDK's Rust core retries every gRPC code except `INVALID_ARGUMENT`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, `OUT_OF_RANGE`, `UNIMPLEMENTED` and `NOT_FOUND`. The 500 fault is provided because the test brief asks for it, and it is a test of client handling, not of the real service.
- `PersistentStream` (create, resume, retire) is implemented for JSON and protobuf; the proto marks it "in development". Only the Python and Rust SDKs call it, and only for those two formats. Arrow and Avro persistent streams are not implemented, because no SDK calls them.
- Not implemented: OpenTelemetry (OTLP), MQTT, the Kafka-compatible APIs, and persistent Arrow or Avro streams (see the format table).

**Operations and quotas**

- Quotas (throughput per stream, REST requests per second, concurrent streams) are not enforced. Only injected faults produce throttling. Stream queues are bounded at 10,000 messages, so a slow reader makes the reader wait.
- There is no per-stream in-flight limit on the mock side.
- The mock uses a self-signed local CA for TLS. The real service uses publicly trusted certificates. Plaintext gRPC is not supported, because the SDKs don't expose a way to turn TLS off.
- The workspace ID is the first DNS label of the gRPC endpoint, as in the SDK. The docs' endpoint format is `<workspace-id>.zerobus.<region>...`. The mock accepts any workspace ID unless `server.workspace_id` is set.
- The control API and fault injection have no equivalent in the real service and no authentication. Keep them on `127.0.0.1` or behind a network boundary.

## How to extend

- **A new fault.** Add an entry to `FAULT_TYPES` in `src/zerobus_mock_server/faults.py` (protocols, points, defaults, description), add its handling where the point is evaluated (`rest.py` for `token` and `insert`, `grpc_server.py` for `create`, `record`, `ingest` and `ack`), and add a test in `tests/test_rest.py` or `tests/test_sdk_faults.py`. Then add a row to `scripts/fault_matrix.py` so the client view is recorded.
- **A new column type.** Add the kind to `_ALIASES` and `_INT_RANGES` (if integer) in `schema.py`, implement its branch in `coerce`, and add the protobuf mapping to `_PROTO_FOR_TYPE` in `validation.py`. Then add a test in `tests/test_schema.py`.
- **A new protocol listener.** Add a servicer or app, and start it in `runner.py` next to the existing listeners. Both listeners share `MockState` (tables, tokens, faults), so new faults and reads work across protocols automatically.
- **Regenerate the gRPC stubs** from `proto/zerobus_service.proto` with `scripts/gen_proto.sh` (needs `grpcio-tools`). The generated files are committed under `src/zerobus_mock_server/pb/`.

## File tree

```
zerobus_mock_server/
  README.md                  this file
  config.example.yaml        placeholder configuration (every key)
  Dockerfile                 base (runtime) and test images
  docker-compose.yml         zerobus-mock service, tests service (profile: test)
  requirements.txt           runtime dependencies
  requirements-test.txt      test dependencies (Python SDK 1.11.0, httpx, pytest)
  pytest.ini
  .dockerignore
  proto/zerobus_service.proto   copied from the SDK repository (Apache-2.0 licensed upstream)
  scripts/
    gen_proto.sh             regenerate the gRPC stubs
    fault_matrix.py          inject each fault and print what the client sees
    e2e_check.py             Python SDK (gRPC) and REST end-to-end check against a running mock
    java/ZerobusMockCheck.java   Java SDK (1.8.1) gRPC check against a running mock (compile with the SDK jar)
  src/zerobus_mock_server/
    __init__.py, __main__.py   package and `python -m zerobus_mock_server --config ...`
    config.py                YAML loading and validation
    schema.py                type strings, coercion, schema snapshots
    validation.py            record rules, rescue routing, protobuf descriptor checks
    tables.py                in-memory table store, reads, stats, schema-change routing
    auth.py                  token endpoint rules and token store
    faults.py                fault catalogue, triggers, scope, event log
    state.py                 shared state for both listeners
    rest.py                  REST listener and control API
    grpc_server.py           gRPC EphemeralStream service
    runner.py                runs both listeners (process or background thread)
    certs.py                 local CA and server certificate
    pb/                      generated gRPC stubs
  tests/
    conftest.py, helpers.py, sdk_helpers.py
    test_schema.py, test_validation.py, test_config.py, test_faults.py,
    test_tables.py, test_auth.py                 unit tests (no server)
    test_rest.py, test_grpc_raw.py               live REST and gRPC tests
    test_sdk_e2e.py, test_sdk_faults.py          the real Python SDK against the mock
    test_limits.py                               the documented 10 MB limit
```
