# Testing with the Zerobus Ingest mock

> Unofficial: this mock is a community testing tool, not a Databricks product, and it isn't supported by Databricks. It doesn't reproduce every Zerobus Ingest behavior. Test against a real workspace before production.

This guide covers the ways to use the mock, how to pick one, the failure scenarios, and where the mock stops. The server and its configuration are in the [server README](README.md). The in-process libraries are in the [library README](mock_lib/README.md).

## Requirements

- Mock library: the toolchain for the language you test in. The Python library runs without the SDK installed. The Java library takes the SDK as a `provided` dependency, so your test classpath needs the SDK's exception classes.
- pytest plugin: Python, with pytest and the SDK installed. The plugin imports `certifi`.
- Docker Compose: Docker with Compose, for the server and for the test suite.
- Curl examples: `curl` and `python3`.

## Choose a path

Each row is a situation. ✅ means the path fits. ⚠️ means it fits with a limit, given in the cell. ❌ means it doesn't fit.

| Situation | Mock library | Mock server, in process (pytest plugin) | Mock server (Docker Compose) | Real Zerobus workspace |
|---|---|---|---|---|
| Unit-testing your retry logic | ✅ Fast, no network. Raises the error your code has to handle. | ✅ Python only. Runs the real SDK, with faults set by call count. | ⚠️ Works, but needs Docker and a running service. | ❌ Can't schedule a failure on the Nth call. |
| Testing a Kafka Connect, Vector or other connector end to end | ❌ The fake lives in your test process, not in the connector's process. | ❌ The plugin serves only the Python test process. | ⚠️ Works if the connector can set a custom endpoint and trust a CA. Not tested in this repo. | ✅ The real service, with its real auth and behavior. |
| CI without Docker | ✅ Needs only the language toolchain. | ✅ Needs Python, pytest and the SDK package. | ❌ Needs Docker on the runner. | ⚠️ Needs credentials and network access from CI. |
| CI with Docker | ✅ `mock_lib/run_tests.sh` runs each language in Docker. | ✅ Runs in your Python test image. | ✅ Start the service beside your tests. | ⚠️ Same credential limits as above. |
| A non-Python language | ⚠️ Java, TypeScript, Go, Rust, C++ and .NET only. | ❌ Python only. | ✅ Any client that can trust a CA and take an endpoint. The SDK matrix in `tests/sdk/` drives Java, Rust, Go, C++, .NET and TypeScript clients this way. | ✅ |
| Testing a real SDK upgrade | ❌ Fakes the SDK's surface, so it can't show a change inside the SDK. | ✅ Runs the SDK pinned in `requirements-test.txt`. | ✅ Runs the SDK pinned in each client image. | ✅ The final check against the real service. |
| Checking that no rows are missing | ⚠️ `stored` keeps the resend duplicates. There is no coverage helper, so compare the lists yourself. | ✅ `coverage()` reports missing and duplicate keys. | ✅ `GET /_mock/tables/<table>/coverage`. | ⚠️ Query the table yourself. Slower, and you have to control the keys. |
| Schema evolution | ⚠️ Models `schema-mismatch` and the fallback route. No table schema. | ✅ Checks record fields by name and basic type. Nested fields aren't checked. | ✅ Same server, driven over the control API. | ✅ The final check. The mock's breaking-change rule is an assumption. |
| Reproducing a customer error message | ⚠️ Returns the scenario's message substring, not the customer's exact text. | ⚠️ Same substring rule. | ⚠️ Same substring rule. | ✅ The only source of the exact wording. |
| Load or throughput | ❌ Not suited. In-memory fake, no network. | ❌ Not suited. In memory, one process. | ❌ Not suited. In memory, one container. | ✅ Use a real workspace and a non-production table. |

Rules of thumb:

- Start with the mock library when your test is in Java, TypeScript, Go, Rust, C++, .NET or Python and checks how your code handles a failure. It is the fastest path and needs no network.
- Start with the in-process pytest plugin when your test is Python and should run the real SDK against the mock.
- Use Docker Compose when the client isn't Python, or when a connector runs as its own process.
- Before production, run the same path against a real workspace. Throughput and exact error text need a real workspace.

## Ways to use it

### (a) Mock library in a unit test

Each library has the SDK's method names, the same controls as the server, and the caller-visible outcomes from `mock_lib/scenarios.json`. The Python example below matches `test_fail_next_after_skips_then_fails_once` in `mock_lib/python/tests/test_fake.py`. Put `mock_lib/python` on `PYTHONPATH`. The library runs without the SDK installed.

```python
import pytest
from zerobus_mock_lib import FakeMock, FakeZerobusSdk, ZerobusException

TABLE = "main.default.events"


def stream_for(mock=None):
    mock = mock or FakeMock()
    return FakeZerobusSdk(mock).create_stream("id", "secret", TABLE), mock


def test_fail_next_after_skips_then_fails_once():
    s, mock = stream_for()
    mock.fail_next("schema-mismatch", after=2)  # skip two records, then fail the next one
    s.ingest_record_offset("a")
    s.ingest_record_offset("b")
    with pytest.raises(ZerobusException, match="does not fit"):
        s.ingest_record_offset("c")
    assert s.ingest_record_offset("d") == 2  # the failure has fired; this call succeeds
    assert s.ingested == ["a", "b", "d"]
```

The Java example matches `failNextAfterSkipsThenFailsOnce` in `mock_lib/java/src/test/java/com/databricks/zerobus/mock/FakeLibTest.java`.

```java
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.mock.FakeMock;
import com.databricks.zerobus.mock.FakeStream;
import com.databricks.zerobus.mock.FakeZerobusSdk;
import java.util.List;
import org.junit.jupiter.api.Test;

class RetryLogicTest {
  private static final String TABLE = "main.default.events";

  private static FakeStream stream(FakeMock mock) throws Exception {
    return new FakeZerobusSdk(mock).createJsonStream(TABLE, "id", "secret").get();
  }

  @Test
  void failNextAfterSkipsThenFailsOnce() throws Exception {
    FakeMock mock = new FakeMock();
    FakeStream s = stream(mock);
    mock.failNext("schema-mismatch", 1, 2);
    s.ingestRecordOffset("a");
    s.ingestRecordOffset("b");
    ZerobusException e = assertThrows(ZerobusException.class, () -> s.ingestRecordOffset("c"));
    assertInstanceOf(NonRetriableException.class, e);
    assertTrue(e.getMessage().contains("does not fit"));
    assertEquals(2L, s.ingestRecordOffset("d"));
    assertEquals(List.of("a", "b", "d"), s.ingested());
  }
}
```

For TypeScript, Go, Rust, C++ and .NET, see [Usage in tests](mock_lib/README.md#usage-in-tests) in the library README. The matching tests are `mock_lib/typescript/test/fake.test.ts`, `mock_lib/go/zerobusmock_test.go`, `mock_lib/rust/tests/fake.rs`, `mock_lib/cpp/tests/zerobus_mock_test.cpp` and `mock_lib/dotnet/Zerobus.Mock.Tests/ZerobusMockTests.cs`. The [controls table](mock_lib/README.md#the-controls-all-languages) lists the same controls in each language.

### (b) Mock server in process with the pytest plugin

The plugin provides two fixtures. `zerobus_mock_server` starts the server for the test session on free ports and sets `SSL_CERT_FILE` to a copy of the certifi bundle plus the mock's CA. `zerobus_mock` is a control client, and the server is reset before and after each test. The conftest must import `zerobus_mock_config` as well, even when you keep the default. Its default table is `main.default.events`. To change the tables or the record limit, define your own `zerobus_mock_config` fixture in the conftest.

```python
# conftest.py
from zerobus_mock_server.pytest_plugin import zerobus_mock, zerobus_mock_config, zerobus_mock_server  # noqa: F401
```

```python
# test_ingest.py
from zerobus.sdk.shared import TableProperties
from zerobus.sdk.sync import ZerobusSdk

TABLE = "main.default.events"


def test_retries_after_abort(zerobus_mock, zerobus_mock_server):
    zerobus_mock.fail_next("stream-abort-retryable", after=4)  # the 5th submission is stored unacked, then the stream ends UNAVAILABLE
    sdk = ZerobusSdk(zerobus_mock_server.grpc_endpoint, zerobus_mock_server.unity_catalog_url)
    stream = sdk.create_stream("mock-client-id", "mock-client-secret", TableProperties(TABLE))
    for i in range(10):
        stream.ingest_record_offset({"device_name": f"ex-{i}", "temp": i})
    stream.flush()
    stream.close()
    cov = zerobus_mock.coverage(TABLE, expected=10, key="device_name")
    assert cov["missing"] == 0  # at-least-once: duplicates are allowed, missing rows are not
```

This matches the README's "Testing patterns" example, with the SDK call filled in. The server's suite runs the README version as `test_readme_pytest_example` in `tests/test_control_v3.py`. The SDK-level version of the same behavior is `test_retryable_abort_is_recovered_with_duplicates` in `tests/test_sdk_faults.py`.

### (c) Mock server in Docker Compose, with a real SDK

Start the service from the server folder. It listens on `127.0.0.1` with REST on port 8443 and gRPC on port 50051, as set in `config.example.yaml`.

```bash
docker compose up -d zerobus-mock
```

The mock's CA is written to the `mock-data` volume, not to your folder. Copy it out, then check the health endpoint. The server README's quick start uses `ca.pem` from the current folder, which needs this step under Compose.

```bash
docker compose exec -T zerobus-mock cat /config/data/tls/ca.pem > ca.pem
curl -s --cacert ca.pem https://localhost:8443/_mock/health
```

The health check returns `{"status":"ok","tables":["main.default.air_quality"]}` with the example config.

Point the SDK at these values:

| Setting | Value |
|---|---|
| gRPC endpoint | `https://localhost:50051` |
| Unity Catalog and REST URL | `https://localhost:8443` |
| Table | `main.default.air_quality` (from `config.example.yaml`) |
| Client ID and secret | Any non-empty strings, for example `mock-client-id` and `mock-client-secret` |
| Trusted CA | `SSL_CERT_FILE` set to the copied `ca.pem` |

The SDK takes the first DNS label of the gRPC host as the workspace ID, which is `localhost` here. Keep `localhost` in the endpoints, because the server certificate names it. The SDK test matrix sets `SSL_CERT_FILE` to the CA alone because its clients talk only to the mock. If a test also calls other HTTPS hosts, use a bundle that holds the system roots and `ca.pem`.

```python
# client.py, the same calls as tests/sdk/clients/python/client.py
from zerobus.sdk.shared import TableProperties
from zerobus.sdk.sync import ZerobusSdk

sdk = ZerobusSdk("https://localhost:50051", "https://localhost:8443")
stream = sdk.create_stream("mock-client-id", "mock-client-secret",
                           TableProperties("main.default.air_quality"))
for i in range(5):
    stream.ingest_record_offset({"device_name": f"sensor-{i}", "temp": 20 + i})
stream.flush()
stream.close()
```

```bash
SSL_CERT_FILE=$PWD/ca.pem python client.py
curl -s --cacert ca.pem "https://localhost:8443/_mock/tables/main.default.air_quality/rows?destination=table&limit=10"
```

To run the server's own suite in Docker, build the test image and run it. In the tested setup, the suite reports 84 passed and 1 skipped (the skipped test needs `mock_lib/`, which the image doesn't copy).

```bash
export PIP_INDEX_URL=https://pypi.org/simple
docker compose --profile test build tests && docker compose --profile test run --rm tests
```

The CA is kept in the volume, so the same `ca.pem` works after a restart until the volume is removed. Stop the service with `docker compose down`.

### (d) Driving failures with curl

The control API is plain HTTP on the REST listener, with the same CA. It has no authentication, so keep it on `127.0.0.1`. Each call returns `{"ok":true}` (or `{"cleared":true}` or `{"reset":true,...}`), or an error with `error_code` and `message`. Unknown scenario names return 404, and bad parameters return 400.

Insert calls need a token. Request one for the table, then send records as JSON.

```bash
TOKEN=$(curl -s --cacert ca.pem -X POST https://localhost:8443/oidc/v1/token \
  -u mock-client-id:mock-client-secret \
  --data-urlencode grant_type=client_credentials --data-urlencode scope=all-apis \
  --data-urlencode "resource=api://databricks/workspaces/localhost/zerobusDirectWriteApi" \
  --data-urlencode 'authorization_details=[{"type":"unity_catalog_privileges","privileges":["USE CATALOG"],"object_type":"CATALOG","object_full_path":"main"},{"type":"unity_catalog_privileges","privileges":["SELECT","MODIFY"],"object_type":"TABLE","object_full_path":"main.default.air_quality","operations":["zerobuswrite"]}]' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')

curl -s --cacert ca.pem -X POST https://localhost:8443/zerobus/v1/tables/main.default.air_quality/insert \
  -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '[{"device_name":"sensor-0","temp":21}]'
```

A request body with another content type returns 415. A request for a table that isn't configured returns 404.

The control calls, all against `https://localhost:8443`:

```bash
# Fail the next insert to this table with 404. Count defaults to 1.
curl -s --cacert ca.pem -X POST https://localhost:8443/_mock/fail-next \
  -H 'content-type: application/json' \
  -d '{"scenario":"table-not-found","table":"main.default.air_quality"}'

# Skip one record, then fail the next record with a schema mismatch.
# REST inserts are atomic, so the whole request is refused and nothing from it is stored.
curl -s --cacert ca.pem -X POST https://localhost:8443/_mock/fail-next \
  -H 'content-type: application/json' -d '{"scenario":"schema-mismatch","after":1}'

# Hold a scenario until it is cleared. Every insert returns 503 meanwhile.
curl -s --cacert ca.pem -X POST https://localhost:8443/_mock/scenario \
  -H 'content-type: application/json' -d '{"scenario":"server-unavailable"}'
curl -s --cacert ca.pem -X DELETE https://localhost:8443/_mock/scenario

# The next authenticated call finds its token expired and returns 401.
curl -s --cacert ca.pem -X POST https://localhost:8443/_mock/expire-token \
  -H 'content-type: application/json' -d '{"count":1}'
```

Read back what landed, and check coverage of expected keys:

```bash
# destination is table, fallback or all
curl -s --cacert ca.pem "https://localhost:8443/_mock/tables/main.default.air_quality/rows?destination=table&limit=100"

# Missing and duplicate keys for 0..7. The key is an integer, or the last run of digits in a string, so sensor-7 counts as 7.
curl -s --cacert ca.pem "https://localhost:8443/_mock/tables/main.default.air_quality/coverage?expected=8&key=device_name"
```

The `coverage` response has `missing`, `missing_keys`, `duplicate_rows` and `fallback_rows`, among other fields. The full list of calls is in the [server README](README.md#control-api).

Clear the rows, held scenarios and pending expiries before the next run. `"schema": true` also restores the configured columns.

```bash
curl -s --cacert ca.pem -X POST https://localhost:8443/_mock/reset \
  -H 'content-type: application/json' -d '{"tables":["main.default.air_quality"],"schema":true}'
```

### (e) CI recipes

These are sketches. They haven't been run in a CI system. Replace the paths and the test commands with yours.

Compose path, with the SDK tests in your repo:

```yaml
# Sketch: not run.
name: zerobus-mock-sdk-tests
on: [pull_request]
jobs:
  sdk-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install test dependencies
        run: pip install pytest databricks-zerobus-ingest-sdk==1.11.0
      - name: Start the mock
        working-directory: path/to/zerobus_mock_server
        run: |
          docker compose up -d zerobus-mock
          docker compose exec -T zerobus-mock cat /config/data/tls/ca.pem > "$RUNNER_TEMP/ca.pem"
          for i in $(seq 30); do
            curl -sf --cacert "$RUNNER_TEMP/ca.pem" https://localhost:8443/_mock/health && break
            sleep 1
          done
      - name: Run SDK tests
        env:
          SSL_CERT_FILE: ${{ runner.temp }}/ca.pem
        run: pytest tests/zerobus
      - name: Stop the mock
        if: always()
        working-directory: path/to/zerobus_mock_server
        run: docker compose down
```

Library path, with unit tests against the in-process fake and no server:

```yaml
# Sketch: not run.
name: zerobus-mock-lib-tests
on: [pull_request]
jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install pytest
        run: pip install pytest
      - name: Run unit tests
        env:
          PYTHONPATH: path/to/zerobus_mock_server/mock_lib/python
        run: pytest tests/unit
```

## Scenario cookbook

These are the eleven scenarios in `mock_lib/scenarios.json`, which is the shared contract. Library calls are shown in Python. The other libraries use their own spelling of the same controls. `after` skips that many matching calls. In the examples each call carries one record.

| Scenario | What it simulates | Trigger, library (Python) | Trigger, control API | What the caller sees |
|---|---|---|---|---|
| `auth-failure` | The token request is rejected as an invalid client. | `fail_next("auth-failure")` | `POST /_mock/fail-next` `{"scenario":"auth-failure"}` | `create_stream` raises, with `invalid_client` in the message. The SDK doesn't retry it. |
| `table-not-found` | The table is missing at create or at insert. | `fail_next("table-not-found")` | `POST /_mock/fail-next` `{"scenario":"table-not-found"}` | `create_stream` raises with `cannot be found`. A REST insert returns 404. |
| `schema-mismatch` | A record has a field the table doesn't have. | `fail_next("schema-mismatch", after=3)` | `POST /_mock/fail-next` `{"scenario":"schema-mismatch","after":3}` | `ingest` or `flush` raises with `does not fit`. Over gRPC, earlier records stay stored. Over REST, the whole request is refused. |
| `record-too-large` | A record is over the size limit. | `fail_next("record-too-large", after=1)` | `POST /_mock/fail-next` `{"scenario":"record-too-large","after":1}` | A terminal error from `ingest` or `flush`, with `maximum message size`. |
| `server-unavailable` | The service is unavailable for a period. | `set_scenario("server-unavailable")`, then `clear_scenario()` | `POST /_mock/scenario` `{"scenario":"server-unavailable"}`, then `DELETE /_mock/scenario` | The SDK retries, then `create_stream` raises with `UNAVAILABLE`. A REST call returns 503 while the scenario is held. |
| `stream-abort-retryable` | The stream fails after a record was stored without an acknowledgment. | `fail_next("stream-abort-retryable", after=4)` | `POST /_mock/fail-next` `{"scenario":"stream-abort-retryable","after":4}` | No error. The SDK resends the record, so it can appear twice. |
| `stream-abort-fatal` | The stream ends with a terminal `INVALID_ARGUMENT`. | `fail_next("stream-abort-fatal", after=4)` | `POST /_mock/fail-next` `{"scenario":"stream-abort-fatal","after":4}` | `ingest` or `flush` raises with `terminal stream error`. Only acknowledged rows are kept. A new stream works. |
| `graceful-close` | The server sends a close signal with a drain window. | `fail_next("graceful-close", after=3)`. The Python library takes no parameters. | `POST /_mock/fail-next` `{"scenario":"graceful-close","after":3,"params":{"duration_ms":1000}}`. The default is 500 ms. | No error. The SDK pauses, drains, and reconnects. |
| `no-acks` | Acknowledgments are withheld. | `set_scenario("no-acks")`, then `clear_scenario()`. `fail_next("no-acks")` withholds one flush. | `POST /_mock/scenario` `{"scenario":"no-acks"}`, then `DELETE /_mock/scenario` | `flush` raises with `timed out`. The rows are stored but not acknowledged. |
| `schema-change-while-open` | A non-nullable `region` column is added while a stream is open. | `fail_next("schema-change-while-open")` | `POST /_mock/fail-next` `{"scenario":"schema-change-while-open"}` | No error. The open stream's rows go to the fallback bucket. Read them with `destination=fallback`. |
| `token-expired` | The access token expires. This scenario can't be held. | `expire_token()` | `POST /_mock/expire-token` `{"count":1}`. `fail-next` with this name also works. | At `create_stream` the SDK refreshes the token and succeeds. Mid-stream, the stream fails with `expired`. |

For the exact text each scenario produces, see the [server README's Scenarios section](README.md#scenarios). Its table reflects the SDK version tested there.

## Limits

Most of the differences from the real service are listed in the [server README](README.md#differences-from-the-real-service). The main ones:

- The mock is in memory. A restart starts empty.
- Failures are exact counts (the Nth call), not rates or timers.
- The control API has no authentication. Bind it to `127.0.0.1`.
- Any non-empty client credentials are accepted.
- Records are checked by name and basic type only. Nested values, decimal precision, timestamp zones and protobuf maps aren't checked.
- Where the public docs are silent, the mock makes an assumption. Examples: offsets run 0, 1, 2 per stream, REST inserts are atomic, and a breaking schema change sends that stream's rows to a fallback bucket.

Move to a real workspace when:

- You measure throughput, latency or memory use.
- Your test asserts exact error wording, or an error the mock doesn't model.
- Your test relies on one of the assumptions listed above.
- You are preparing a production release, or the SDK or service has changed in a way the mock doesn't cover.
