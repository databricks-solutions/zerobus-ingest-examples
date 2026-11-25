#!/bin/bash
# Build the C++ matrix client: the Rust core FFI (zerobus-ffi, from the monorepo's rust/ffi) with the
# vendored crates, then the C++ SDK sources from cpp/src compiled with g++ (no cmake in the image).
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
LIB=$(ls "$CARGO_TARGET_DIR"/release/libzerobus_ffi.a)
echo "ffi library: $LIB"
SRC=/sdk/cpp/src
INC="-I/sdk/cpp/include -I/sdk/cpp/src -I/sdk/rust/ffi"
for f in "$SRC"/*.cpp; do
  g++ -std=c++17 -O1 -c $INC "$f" -o "$CACHE/obj/$(basename "$f" .cpp).o"
done
g++ -std=c++17 -O1 -c $INC /client/client.cpp -o "$CACHE/obj/client.o"
g++ -o "$CACHE/client" "$CACHE"/obj/*.o "$LIB" -lpthread -ldl -lm -lresolv
echo "built $CACHE/client"
