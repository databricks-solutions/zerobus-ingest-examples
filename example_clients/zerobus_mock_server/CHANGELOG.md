# Changelog

UNOFFICIAL: this is a community test double for the Zerobus Ingest APIs. It is not a Databricks product, it is not supported by Databricks, and it is not affiliated with Databricks.

The server, the Python, Java, TypeScript, Go and Rust libraries, and the scenario contract share this file. The package version and the contract version are described in the [server README](README.md#versioning-and-sharing).

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
- Shared only as source in this repo. No container image or package registry release is published.
