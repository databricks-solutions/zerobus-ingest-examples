# Changelog

Unofficial: this is a community test double for the Zerobus Ingest APIs. It is not a Databricks product, it is not supported by Databricks, and it is not affiliated with Databricks.

The server, the Python, Java, TypeScript, Go, Rust, C++ and .NET libraries, and the scenario contract share this file. The package version and the contract version are described in the [server README](README.md#versioning-and-sharing).

## Unreleased

- Mock libraries for C++ (`mock_lib/cpp`, GoogleTest, 15 tests) and .NET (`mock_lib/dotnet`, xUnit, 15 tests), both at version 0.1.0 and contract version 1.
- Pure-Go adapter `mock_lib/go/purego` (import path `zerobusmock/purego`): the pure-Go SDK's `New`, `CreateStream`, `[]byte` ingest and `*Error` shape over the cgo fake's scenario table. 11 tests.
- SDK matrix: a `dotnet` lane (JSON and protobuf) in `tests/sdk/run_all.sh`.
- `mock_lib/run_tests.sh` runs `cpp` and `dotnet` in addition to the existing languages. `check_contract_version.py` checks their declared contract versions.
- No mock for the Pure C SDK (`purec/`), which is not in the SDK list of the [zerobus-sdk](https://github.com/databricks/zerobus-sdk) README.

## 0.1.0

- Contract version: `1` (`contract_version` in `scenarios.json`, the same value in the server and in each library).
- Server: REST and gRPC write APIs, eleven named failure scenarios, the control API, and the pytest plugin.
- Mock libraries: in-process fakes for Python, Java, TypeScript, Go and Rust, each with its own tests.
- Contract check: `mock_lib/check_contract_version.py` runs first in `mock_lib/run_tests.sh`.
- SDK versions tested against the mock:
  - Python `databricks-zerobus-ingest-sdk` 1.11.0
  - Java `com.databricks:zerobus-ingest-sdk` 1.8.1
  - TypeScript `@databricks/zerobus-ingest-sdk` 1.5.0
  - Go `github.com/databricks/zerobus-sdk/go` 1.8.0
  - Rust crate `databricks-zerobus-ingest-sdk` 3.2.0
  - .NET `Databricks.Zerobus.Ingest.Sdk` 0.1.0 (matrix lane, built from [databricks/zerobus-sdk](https://github.com/databricks/zerobus-sdk) at commit 857391c)
- Shared only as source in this repo. No container image or package registry release is published.
