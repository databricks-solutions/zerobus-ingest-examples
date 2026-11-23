# Zerobus mock libraries

In-process fakes of the Zerobus SDK entry point and stream, for unit tests of client code. No network and no server. Each library has the SDK's method names, the same controls as the server (`failNext`/`fail_next`, `expireToken`, `setScenario`, `clearScenario`, `reset`), recorded ingests, and the caller-visible outcomes in `scenarios.json`.

Languages: Python, Java, TypeScript, Go and Rust, each tested with `./run_tests.sh`.

## Layout

```
mock_lib/
  scenarios.json        shared contract: what the caller sees for each scenario
  run_tests.sh          runs every language's tests in Docker, prints a pass/fail table
  python/               zerobus_mock_lib/__init__.py (FakeZerobusSdk, FakeStream, FakeMock), tests/test_fake.py
  java/                 Maven: src/main/java/com/databricks/zerobus/mock/, src/test/... FakeLibTest.java (JUnit 5)
  typescript/           src/fake.ts (no build step: Node 22 strips types), test/fake.test.ts (node --test)
  go/                   zerobusmock.go, zerobusmock_test.go (go test)
  rust/                 src/lib.rs, tests/fake.rs (cargo test)
```

Each language directory has a `Dockerfile` (pinned base image). Build context is this directory, so the tests can read `scenarios.json`.

## Running the tests

```bash
./run_tests.sh              # all languages, one after another
./run_tests.sh java go      # some of them
```

The script prints a table with the result and the test count per language, and writes each log to a new temp directory. Proxies are read from the environment when set: `MAVEN_MIRROR` (else `MVNW_REPOURL/maven2`, else Maven Central), `NPM_CONFIG_REGISTRY`, `GOPROXY`, `PIP_INDEX_URL` or `UV_DEFAULT_INDEX`. Each image runs with `--network host`.

Rust builds offline from a vendored copy of `rust/Cargo.lock` (crates fetched from static.crates.io and checked against the lock), because some hosts cannot reach `index.crates.io`.

## The controls (all languages)

| Control | Meaning |
|---|---|
| `failNext(scenario, count, after)` | After skipping `after` matching calls, the next `count` fail. One-shot. |
| `setScenario(name)` / `clearScenario()` | Every matching call fails until cleared. |
| `expireToken(count)` | The next `count` authenticated calls find the token expired. At create the SDK refreshes and succeeds; mid-stream the stream fails. |
| `reset()` | Clears budgets, the held scenario and the expiry budget. |
| `ingested` / `stored` / `fallback` | Records the stream accepted; rows that landed in the table (a row resent after `stream-abort-retryable` appears twice); rows routed to the fallback bucket. |

Unknown scenario names are rejected: Python raises `ValueError`, Java `IllegalArgumentException`, TypeScript `Error`, and Go and Rust return an error. `token-expired` cannot be held: use `expireToken`.

Each library keeps a copy of the scenario table and a test checks it against `scenarios.json`, so a change to the contract fails the suite until the copy is updated.

## Usage in tests

The same scenario in each language, excerpted from the test that runs in that language's suite.

Python (`python/tests/test_fake.py`, `test_fail_next_after_skips_then_fails_once`, pytest):

```python
from zerobus_mock_lib import FakeMock, FakeZerobusSdk, ZerobusException

def test_fail_next_after_skips_then_fails_once():
    s, mock = stream_for()
    mock.fail_next("schema-mismatch", after=2)
    s.ingest_record_offset("a")
    s.ingest_record_offset("b")
    with pytest.raises(ZerobusException, match="does not fit"):
        s.ingest_record_offset("c")
    assert s.ingest_record_offset("d") == 2  # one-shot: the next call is fine
    assert s.ingested == ["a", "b", "d"]
```

Java (`java/src/test/java/com/databricks/zerobus/mock/FakeLibTest.java`, `failNextAfterSkipsThenFailsOnce`, JUnit 5):

```java
FakeMock mock = new FakeMock();
FakeStream s = stream(mock);
mock.failNext("schema-mismatch", 1, 2);
s.ingestRecordOffset("a");
s.ingestRecordOffset("b");
ZerobusException e = assertThrows(ZerobusException.class, () -> s.ingestRecordOffset("c"));
assertInstanceOf(NonRetriableException.class, e);
assertTrue(e.getMessage().contains("does not fit"));
assertEquals(2L, s.ingestRecordOffset("d"), "one-shot: the next call succeeds");
assertEquals(List.of("a", "b", "d"), s.ingested());
```

TypeScript (`typescript/test/fake.test.ts`, node:test):

```ts
test('failNext with after skips matching calls, then fails once (schema-mismatch)', async () => {
  const { mock, stream } = await streamFor();
  mock.failNext('schema-mismatch', { after: 2 });
  await stream.ingestRecordOffset('a');
  await stream.ingestRecordOffset('b');
  await assert.rejects(stream.ingestRecordOffset('c'), /does not fit/);
  assert.equal(await stream.ingestRecordOffset('d'), 2n, 'one-shot: the next call succeeds');
  assert.deepEqual(stream.ingested(), ['a', 'b', 'd']);
});
```

Go (`go/zerobusmock_test.go`, `TestFailNextAfterSkipsThenFailsOnce`):

```go
mock := NewFakeMock()
st := streamFor(t, mock)
if err := mock.FailNext("schema-mismatch", 1, 2); err != nil {
	t.Fatal(err)
}
st.IngestRecordOffset("a")
st.IngestRecordOffset("b")
_, err := st.IngestRecordOffset("c")
mustErr(t, err, "does not fit")
```

Rust (`rust/tests/fake.rs`, `fail_next_after_skips_then_fails_once`, tokio):

```rust
let mock = Arc::new(FakeMock::new());
let s = stream_for(&mock).await;
mock.fail_next("schema-mismatch", 1, 2).unwrap();
s.ingest_record_offset("a").await.unwrap();
s.ingest_record_offset("b").await.unwrap();
let e = s.ingest_record_offset("c").await.unwrap_err();
assert!(msg(&e).contains("does not fit"), "{e}");
```

## Choices and deviations

- **Exceptions.** Java throws the SDK's own `ZerobusException` and `NonRetriableException` (the SDK jar is a `provided` dependency; the fake never loads its native library). Python uses the SDK's exception when installed. TypeScript throws `Error` with `retriedBySdk`, which is what the SDK throws. Go's `ZerobusError` and Rust's `ZerobusError` are lib-local copies, because the real Go package needs cgo and the Rust crate pulls in tonic. Both keep the SDK's fields, methods and Display texts.
- **Payloads.** Java and Rust take JSON strings. TypeScript and Go accept any value. Proto and Arrow inputs are not modeled.
- **Naming.** Classes use the `Fake` prefix (`FakeZerobusSdk`, `FakeStream`, `FakeMock`) as in Python. Method names follow each SDK (`ingestRecordOffset`, `ingest_record_offset`, `IngestRecordOffset`).
- **Stream-level behavior.** `stream-abort-fatal` and a mid-stream token expiry make the stream terminal: later calls on it fail, and a new stream works. `stream-abort-retryable` raises nothing and stores the record twice. `schema-change-while-open` raises nothing and routes rows to `fallback`. `graceful-close` raises nothing. Acks are immediate unless `no-acks` is held, and flush or waitForOffset fail while the hold is on.
- **Scenario table.** Each library embeds a copy of the scenario table, and its tests check that copy against `scenarios.json`.

## Scenarios

The table of what the caller sees is in [the server README](../README.md). `scenarios.json` is the machine-readable version and is the source of truth for the outcome and the message substring.
