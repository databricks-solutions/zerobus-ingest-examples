#!/usr/bin/env bash
# Smoke test for a running stack (docker compose up -d). It checks that:
#   1. the app returns a narration through the gateway (a success row),
#   2. the failing model returns an error to the app (a failure row),
#   3. the gateway logged no zerobus callback error after one flush interval.
# Then run tests/check_rows.sql in Databricks SQL to see the rows.
set -euo pipefail
cd "$(dirname "$0")/.."
APP="${APP_URL:-http://localhost:8000}"

ok=$(curl -sf "$APP/rolldice?player=smoke-test")
echo "success call: $ok"
echo "$ok" | grep -q '"gateway_status":200' || { echo "FAIL: expected a narration from the gateway"; exit 1; }

bad=$(curl -sf "$APP/rolldice?player=smoke-test&fail=true")
echo "failure call: $bad"
echo "$bad" | grep -q '"narration":null' || { echo "FAIL: expected the failing model to return an error"; exit 1; }

echo "waiting 10 seconds for the callback to flush..."
sleep 10
if docker compose logs litellm 2>&1 | grep -E "zerobus: (dropping|.*failed)"; then
  echo "FAIL: the zerobus callback logged an error (see the line above and the Troubleshooting section)"
  exit 1
fi
echo "PASS: both requests went through the gateway and the callback logged no error"
