#!/usr/bin/env bash
# Creates a topic and produces sample records (JSON with a schema, key = record number) through the Kafka container.
# Usage: scripts/produce.sh <topic> [records=200] [partitions=3]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TOPIC="$1"; N="${2:-200}"; PARTS="${3:-3}"
cd "$ROOT/compose"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic "$TOPIC" --partitions "$PARTS" > /dev/null
python3 -I - "$N" <<'PY' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC" \
  --property parse.key=true --property key.separator=$'\t'
import json, sys
schema = {"type": "struct", "optional": False, "name": "demo_event", "fields": [
    {"type": "int64", "optional": True, "field": "id"},
    {"type": "string", "optional": True, "field": "name"},
    {"type": "int32", "optional": True, "field": "amount"}]}
for i in range(1, int(sys.argv[1]) + 1):
    value = {"schema": schema, "payload": {"id": i, "name": "event-%d" % i, "amount": i % 1000}}
    print("k%d\t%s" % (i, json.dumps(value, separators=(",", ":"))))
PY
echo "produced $N records to $TOPIC"
