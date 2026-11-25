# SDK matrix client: Rust. Toolchain 1.94 (the SDK's avro feature needs Rust 1.88 or later).
# The SDK is compiled from the monorepo rust/sdk by build.sh (crates vendored by vendor.py).
FROM rust:1.94-slim-bookworm
WORKDIR /client
COPY Cargo.toml.in /client/Cargo.toml.in
COPY src /client/src
COPY build.sh run.sh /client/
RUN chmod +x /client/build.sh /client/run.sh
ENTRYPOINT ["/client/run.sh"]
