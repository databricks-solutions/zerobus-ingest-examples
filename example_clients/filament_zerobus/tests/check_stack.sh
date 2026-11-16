#!/bin/sh
# Checks that the Filament server, worker and control plane answer /readyz.
# Usage: tests/check_stack.sh [server-port worker-port control-plane-port]   (defaults: 8080 8081 8082)
set -eu

SERVER_PORT=${1:-8080}
WORKER_PORT=${2:-8081}
CONTROL_PORT=${3:-8082}

for port in "$SERVER_PORT" "$WORKER_PORT" "$CONTROL_PORT"; do
  curl -fsS "http://127.0.0.1:${port}/readyz" > /dev/null
  echo "ok  http://127.0.0.1:${port}/readyz"
done
