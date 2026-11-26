#!/usr/bin/env bash
# Registers a connector with the Connect worker's REST API.
# Usage: scripts/register.sh [config.json]   (default: config.json if present, else config.example.json)
# Refuses a config that still has <placeholders>. Secrets are not in the file: the worker resolves ${env:...} from .env.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
F="${1:-}"
if [[ -z "$F" ]]; then
  if [[ -f "$ROOT/config.json" ]]; then F="$ROOT/config.json"; else F="$ROOT/config.example.json"; fi
fi
if grep -q '<[a-z-]*>' "$F"; then
  echo "refusing $F: it still has <placeholders>. Replace <catalog>, <schema>, <workspace-url> and the others first." >&2
  exit 1
fi
NAME=$(python3 -I -c 'import json,sys;print(json.load(open(sys.argv[1]))["name"])' "$F")
CODE=$(curl -s -o /tmp/zerobus-register-$$.json -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
  --data @"$F" "$CONNECT_URL/connectors")
echo "register $NAME -> HTTP $CODE"
head -c 400 /tmp/zerobus-register-$$.json; echo
rm -f /tmp/zerobus-register-$$.json
echo "status: curl -s $CONNECT_URL/connectors/$NAME/status"
