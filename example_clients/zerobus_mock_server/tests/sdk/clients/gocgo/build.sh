#!/bin/bash
# Build the cgo Go client. The cgo binding links the Rust core's static library, which is built here
# with cargo (offline, from the vendored crates) and placed where the binding's #cgo LDFLAGS expect it.
set -euo pipefail
CACHE=/cache
export CARGO_HOME="$CACHE/cargo-home"
export CARGO_TARGET_DIR="$CACHE/target"
mkdir -p "$CARGO_HOME"
cat > "$CARGO_HOME/config.toml" <<CFG
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "$CACHE/vendor"
CFG
(cd /sdk/rust && cargo build --release --offline --locked -p zerobus-ffi)
rm -rf /tmp/gosdk && cp -r /sdk/go /tmp/gosdk
mkdir -p /tmp/gosdk/lib/linux_amd64
cp "$CARGO_TARGET_DIR/release/libzerobus_ffi.a" /tmp/gosdk/lib/linux_amd64/
rm -rf /tmp/build && mkdir -p /tmp/build && cp /client/main.go /tmp/build/
cat > /tmp/build/go.mod <<'MOD'
module sdkmatrix/gocgo

go 1.25

require github.com/databricks/zerobus-sdk/go v1.8.0

replace github.com/databricks/zerobus-sdk/go => /tmp/gosdk
MOD
cd /tmp/build
GOFLAGS=-mod=mod GOSUMDB=off go build -o "$CACHE/client" .
echo "built $CACHE/client"
