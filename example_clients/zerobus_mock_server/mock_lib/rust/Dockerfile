# Rust unit tests for the Zerobus mock lib. Build context is the mock_lib directory.
#   docker build --network host -f rust/Dockerfile -t zbmock-rust:test .
#   docker run --rm --network host zbmock-rust:test
#
# crates.io's index is not reachable from every host, so every package in Cargo.lock is vendored from
# static.crates.io (sha256 checked against the lock) and cargo builds with --locked --offline.
FROM python:3.12-slim AS vendor
COPY rust/vendor.py rust/Cargo.lock /src/
RUN python -I /src/vendor.py /src/Cargo.lock /vendor

FROM rust:1.94-slim-bookworm
COPY --from=vendor /vendor /cache/vendor
RUN printf '[source.crates-io]\nreplace-with = "vendored-sources"\n\n[source.vendored-sources]\ndirectory = "/cache/vendor"\n' > "$CARGO_HOME/config.toml"
WORKDIR /zbmock/rust
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
COPY rust/tests ./tests
# The tests read ../scenarios.json, which is /zbmock/scenarios.json here.
COPY scenarios.json /zbmock/scenarios.json
RUN cargo test --no-run --locked --offline
CMD ["cargo", "test", "--locked", "--offline", "--quiet"]
