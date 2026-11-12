#!/bin/sh
# Validates both configs with Vector's own checker (no credentials needed),
# then, with --live, runs the dead-letter test against your table using .env.
set -eu
cd "$(dirname "$0")/.."
IMG=timberio/vector:0.61.0-debian
echo "== vector validate (config only) =="
docker run --rm -v "$PWD/vector:/etc/vector:ro" "$IMG" validate --no-environment /etc/vector/vector.yaml
docker run --rm -v "$PWD/vector:/etc/vector:ro" -v "$PWD/tests:/etc/vector-tests:ro" "$IMG" \
  validate --no-environment /etc/vector-tests/vector-dead-letter.yaml
if [ "${1:-}" = "--live" ]; then
  echo "== dead-letter test (writes 2 rows to the table in vector-dead-letter.yaml, prints 1 dead-lettered event) =="
  timeout 30 docker run --rm --env-file .env -v "$PWD/vector:/etc/vector:ro" -v "$PWD/tests:/etc/vector-tests:ro" "$IMG" \
    --config /etc/vector-tests/vector-dead-letter.yaml || true
  echo "Now run: SELECT event_id, player, result, message FROM <catalog>.<schema>.dice_logs WHERE event_id LIKE 'test-good-%'"
fi
