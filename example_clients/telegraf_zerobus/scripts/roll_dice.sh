#!/bin/bash
# Usage: scripts/roll_dice.sh [count] [player]
# Rolls the die through the dice-roll app once a second. Each roll reaches Telegraf and then the Zerobus Ingest table.
set -u
count="${1:-10}"
player="${2:-demo}"
app="${DICE_APP_URL:-http://127.0.0.1:8000}"
ok=0
failed=0
for _ in $(seq 1 "$count"); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$app/roll?player=$player")
  if [ "$code" = "200" ]; then ok=$((ok + 1)); else failed=$((failed + 1)); fi
  sleep 1
done
echo "rolls=$count ok=$ok failed=$failed"
[ "$failed" -eq 0 ]
