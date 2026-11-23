#!/bin/bash
# Build the Rust matrix client against the monorepo's rust/sdk (mounted at /sdk).
# The crates are vendored beforehand by vendor.py (into /cache/vendor); cargo then builds offline.
# Output: /cache/client. Everything lives under /cache (a volume), so a second run reuses it.
set -euo pipefail
CACHE=/cache
export CARGO_HOME="$CACHE/cargo-home"
export CARGO_TARGET_DIR="$CACHE/target"
mkdir -p "$CARGO_HOME" "$CACHE/vendor"
[ -f "$CACHE/vendor/.complete" ] || { echo "vendor step missing"; exit 1; }

cat > "$CARGO_HOME/config.toml" <<CFG
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "$CACHE/vendor"
CFG

rm -rf /tmp/build && mkdir -p /tmp/build/src
sed "s#@SDK@#/sdk#" /client/Cargo.toml.in > /tmp/build/Cargo.toml
cp /client/src/main.rs /tmp/build/src/main.rs
cp /sdk/rust/Cargo.lock /tmp/build/Cargo.lock
cd /tmp/build
cargo build --release --offline
cp "$CARGO_TARGET_DIR/release/sdk-matrix-rust" /cache/client
echo "built /cache/client"
