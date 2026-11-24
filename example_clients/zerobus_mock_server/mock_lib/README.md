# Zerobus mock libraries (unofficial)

> Unofficial: this mock is a community testing tool, not a Databricks product, and it isn't supported by Databricks. It doesn't reproduce every Zerobus Ingest behavior. Test against a real workspace before production.

In-process fakes of the Zerobus SDK entry point and stream, for unit tests of client code. No network and no server. Each library has the SDK's method names, the same controls as the server (`failNext`/`fail_next`, `expireToken`, `setScenario`, `clearScenario`, `reset`), recorded ingests, and the caller-visible outcomes in `scenarios.json`.

Languages: Python, Java, TypeScript, Go (cgo and pure-Go), Rust, C++ and .NET, each tested with `./run_tests.sh`. C is not covered. The Pure C SDK (`purec/`) has no mock, and it is not in the SDK list in the [zerobus-sdk](https://github.com/databricks/zerobus-sdk) README.

## Layout

```
mock_lib/
  scenarios.json        shared contract: what the caller sees for each scenario
  run_tests.sh          runs every language's tests in Docker, prints a pass/fail table
  check_contract_version.py  each library's declared contract_version must match scenarios.json (run first by run_tests.sh)
  python/               zerobus_mock_lib/__init__.py (FakeZerobusSdk, FakeStream, FakeMock), tests/test_fake.py
  java/                 Maven: src/main/java/com/databricks/zerobus/mock/, src/test/... FakeLibTest.java (JUnit 5)
  typescript/           src/fake.ts (no build step: Node 22 strips types), test/fake.test.ts (node --test)
  go/                   zerobusmock.go, zerobusmock_test.go (go test); purego/ is the pure-Go SDK's API over the same fake
  rust/                 src/lib.rs, tests/fake.rs (cargo test)
  cpp/                  include/zerobusmock/zerobus_mock.hpp, src/, tests/zerobus_mock_test.cpp (GoogleTest, CMake)
  dotnet/               Zerobus.Mock/ (library), Zerobus.Mock.Tests/ (xUnit, net8.0)
```

Each language directory has a `Dockerfile` (pinned base image). Build context is this directory, so the tests can read `scenarios.json`.

## Running the tests

```bash
./run_tests.sh              # all languages, one after another
./run_tests.sh java go      # some of them
```

The script prints a table with the result and the test count per language, and writes each log to a new temp directory. Proxies are read from the environment when set: `MAVEN_MIRROR` (else `MVNW_REPOURL/maven2`, else Maven Central), `NPM_CONFIG_REGISTRY`, `GOPROXY`, `PIP_INDEX_URL` or `UV_DEFAULT_INDEX`. Each image runs with `--network host`.

Rust builds offline from a vendored copy of `rust/Cargo.lock` (crates fetched from static.crates.io and checked against the lock), because some hosts cannot reach `index.crates.io`.

The C++ image takes cmake from the PyPI wheel (`cmake==3.31.6`) and fetches its two test dependencies through the first `GOPROXY` entry, pinned by SHA-256: GoogleTest v1.15.2 and nlohmann/json v3.11.3. The .NET image restores NuGet packages from nuget.org.

<a id="the-controls-all-languages"></a>

## The controls (all languages)

| Control | Meaning |
|---|---|
| `failNext(scenario, count, after)` | After skipping `after` matching calls, the next `count` fail. One-shot. |
| `setScenario(name)` / `clearScenario()` | Every matching call fails until cleared. |
| `expireToken(count)` | The next `count` authenticated calls find the token expired. At create the SDK refreshes and succeeds; mid-stream the stream fails. |
| `reset()` | Clears budgets, the held scenario and the expiry budget. |
| `ingested` / `stored` / `fallback` | Records the stream accepted; rows that landed in the table (a row resent after `stream-abort-retryable` appears twice); rows routed to the fallback bucket. |

Spelling: C++ uses snake_case (`fail_next`, `set_scenario`, `clear_scenario`, `expire_token`, `reset`, `ingested()`), and .NET uses PascalCase (`FailNext`, `SetScenario`, `ClearScenario`, `ExpireToken`, `Reset`, `Ingested()`).

Unknown scenario names are rejected: Python raises `ValueError`, Java `IllegalArgumentException`, TypeScript `Error`, C++ `std::invalid_argument`, .NET `ArgumentException`, and Go and Rust return an error. `token-expired` cannot be held: use `expireToken`.

Each library keeps a copy of the scenario table and a test checks it against `scenarios.json`, so a change to the contract fails the suite until the copy is updated.

<a id="usage-in-tests"></a>

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

C++ (`cpp/tests/zerobus_mock_test.cpp`, `FailNextAfterSkipsThenFailsOnce`, GoogleTest; `stream_for` and `fails_with` are small helpers in the same file):

```cpp
auto mock = std::make_shared<zm::FakeMock>();
auto st = stream_for(mock);
bool retryable = true;
mock->fail_next("schema-mismatch", 1, 2);
st.ingest_json_record("a");
st.ingest_json_record("b");
EXPECT_TRUE(fails_with([&] { st.ingest_json_record("c"); }, "does not fit", &retryable));
EXPECT_FALSE(retryable);
EXPECT_EQ(2, st.ingest_json_record("d"));  // one-shot: the next call is fine
EXPECT_EQ((std::vector<std::string>{"a", "b", "d"}), st.ingested());
```

.NET (`dotnet/Zerobus.Mock.Tests/ZerobusMockTests.cs`, `FailNextAfterSkipsThenFailsOnce`, xUnit):

```csharp
var mock = new FakeMock();
var stream = StreamFor(mock);  // new FakeZerobusSdk(mock).CreateJsonStream(Table, "id", "secret")
mock.FailNext("schema-mismatch", count: 1, after: 2);
stream.IngestRecord("a");
stream.IngestRecord("b");
var ex = AssertFails(() => stream.IngestRecord("c"), "does not fit");
Assert.False(ex.IsRetryable);
Assert.Equal(2, stream.IngestRecord("d")); // one-shot: the next call succeeds
Assert.Equal(new[] { "a", "b", "d" }, stream.Ingested());
```

Pure-Go adapter (`go/purego/zerobus_test.go`, `TestSchemaMismatchIsOneShotAndNotRetryable`):

```go
mock := zerobusmock.NewFakeMock()
st := streamFor(t, mock) // CreateStream(ctx, table, id, secret, WithJSON()) on a New(..., WithMock(mock))
mock.FailNext("schema-mismatch", 1, 0)
off, err := st.IngestRecordOffset([]byte("bad"))
// err wraps *Error{Op: "IngestRecordOffset"}, off == -1, Retryable(err) == false
if off, err := st.IngestRecordOffset([]byte("good")); err != nil || off != 0 {
	t.Fatalf("one-shot: next call should succeed, got %d %v", off, err)
}
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

- **Exceptions.** Java throws the SDK's own `ZerobusException` and `NonRetriableException` (the SDK jar is a `provided` dependency; the fake never loads its native library). Python uses the SDK's exception when installed. TypeScript throws `Error` with `retriedBySdk`, which is what the SDK throws. Go's `ZerobusError` and Rust's `ZerobusError` are lib-local copies, because the real Go package needs cgo and the Rust crate pulls in tonic. Both keep the SDK's fields, methods and Display texts. C++ and .NET define their own `ZerobusException` in the fake's namespace (`zerobusmock`, `Databricks.Zerobus.Mock`), with the SDK's constructor, `is_retryable()` or `IsRetryable`, and message.
- **Payloads.** Java, Rust, C++ and .NET take JSON strings. TypeScript and Go accept any value. Proto and Arrow inputs are not modeled in any library.
- **Naming.** Classes use the `Fake` prefix (`FakeZerobusSdk`, `FakeStream`, `FakeMock`) as in Python. C++ and .NET do the same, with `FakeSdk` and `FakeJsonZerobusStream` for the .NET JSON stream. Method names follow each SDK (`ingestRecordOffset`, `ingest_record_offset`, `IngestRecordOffset`, `ingest_json_record`, `IngestRecord`).
- **Options.** C++ `create_stream` and .NET `CreateJsonStream` take no stream options. The `StreamOptions` and `StreamConfigurationOptions` values are not modeled.
- **Batches.** C++ `ingest_json_records`, .NET `IngestRecords` and Go `IngestRecordsOffset` ingest record by record, so a failing record leaves the ones before it accepted. The .NET SDK documents a batch as one unit that succeeds or fails together. The fake does not model that.
- **Pure-Go SDK.** The cgo fake's method names match the pure-Go SDK's (`IngestRecordOffset`, `Flush`, `WaitForOffset`, `GetUnackedRecords`, `Close`), but its constructor, its `CreateStream` arguments, its payload type and its error type do not. `go/purego` (import path `zerobusmock/purego`, package `zerobus`) is a second package that exposes the pure-Go shape over the same fake: `New`, `SDK.CreateStream(ctx, table, id, secret, WithJSON() | WithProto(desc))` with proto as the default and an error when no descriptor is given, `[]byte` payloads, `GetUnackedRecords() ([][]byte, error)`, `IsClosed`, and `*Error` with `Op` and `Retryable()`. It shares the scenario table, so a change in `zerobusmock` reaches both. Its tests check the shape against the pure-Go SDK's signatures at commit 857391c as compile-time interface assertions.
- **Stream-level behavior.** `stream-abort-fatal` and a mid-stream token expiry make the stream terminal: later calls on it fail, and a new stream works. `stream-abort-retryable` raises nothing and stores the record twice. `schema-change-while-open` raises nothing and routes rows to `fallback`. `graceful-close` raises nothing. Acks are immediate unless `no-acks` is held, and flush or waitForOffset fail while the hold is on.
- **Scenario table.** Each library embeds a copy of the scenario table, and its tests check that copy against `scenarios.json`. The C++ and .NET copies are checked the same way.
- **Test runners.** C++ uses GoogleTest and CMake; .NET uses xUnit. C has no mock. The Pure C SDK (`purec/` in zerobus-sdk) is not in the SDK list in the zerobus-sdk README.

## Scenarios

The table of what the caller sees is in [the server README](../README.md). `scenarios.json` is the machine-readable version and is the source of truth for the outcome and the message substring.
