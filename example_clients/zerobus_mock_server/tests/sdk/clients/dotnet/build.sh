#!/bin/bash
# Build the .NET matrix client: the Rust core FFI (zerobus-ffi, from the monorepo's rust/ffi) with the vendored
# crates, then the .NET SDK from the monorepo's dotnet/ (copied to the cache, with the FFI library placed where
# the SDK loads it), then the client. The SDK's own native build step is skipped (SkipNativeBuild).
set -euo pipefail
CACHE=/cache
export CARGO_HOME="$CACHE/cargo-home"
export CARGO_TARGET_DIR="$CACHE/target"
mkdir -p "$CARGO_HOME" "$CACHE/obj"
cat > "$CARGO_HOME/config.toml" <<CFG
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "$CACHE/vendor"
CFG
cd /sdk/rust
cargo build --release --offline --locked -p zerobus-ffi
LIB="$CARGO_TARGET_DIR/release/libzerobus_ffi.so"
echo "ffi library: $LIB"

# The SDK's project files reach up to the repo root (../../../LICENSE), so copy dotnet/ and LICENSE together.
ROOT="$CACHE/sdk-root"
rm -rf "$ROOT" && mkdir -p "$ROOT"
cp -r /sdk/dotnet "$ROOT/dotnet"
cp /sdk/LICENSE "$ROOT/LICENSE"
NATIVE="$ROOT/dotnet/src/Zerobus/runtimes/linux-x64/native"
mkdir -p "$NATIVE" && cp "$LIB" "$NATIVE/libzerobus_ffi.so"

APP="$CACHE/app"
rm -rf "$APP" && mkdir -p "$APP"
cp /client/SdkMatrixClient.csproj /client/Program.cs "$APP/"
cd "$APP"
dotnet publish -c Release -o "$CACHE/publish" -p:SkipNativeBuild=true -p:TreatWarningsAsErrors=false
# The SDK looks for its FFI library next to the assembly (or under runtimes/<rid>/native), so copy it beside the app.
cp "$LIB" "$CACHE/publish/libzerobus_ffi.so"
echo "built $CACHE/publish"
