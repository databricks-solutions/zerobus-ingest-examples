#!/bin/sh
# Build the pure-Go client against the monorepo's purego module (mounted at /sdk). Output: /cache/client.
set -eu
cd /client
mkdir -p /cache
cp -r /client/. /tmp/build 2>/dev/null || true
rm -rf /tmp/build && mkdir -p /tmp/build && cp /client/main.go /client/go.mod /tmp/build/
cd /tmp/build
go mod edit -replace github.com/databricks/zerobus-sdk/purego=/sdk/purego
GOFLAGS=-mod=mod GOSUMDB=off go mod tidy
GOFLAGS=-mod=mod GOSUMDB=off go build -o /cache/client .
echo "built /cache/client"
