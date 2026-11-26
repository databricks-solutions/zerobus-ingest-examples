#!/usr/bin/env python3
"""At-least-once check for a Kafka topic sunk to a Databricks table and its dead-letter table.

Every (partition, offset) below the topic's end offset must be in the target table or in the dead-letter table
(missing = 0). Duplicates are allowed and reported. Uses the SQL Statement Execution API with your own credentials.

Usage:
  python3 tests/at_least_once_check.py --topic demo_events --table <catalog>.<schema>.demo_events \
      --dlq <catalog>.<schema>.demo_events_dlq --end 0:70,1:65,2:65

  --end: end offset per partition ("partition:end,..."). Or --kafka-container kafka (reads them with docker compose).

Environment (no secret is printed):
  DATABRICKS_HOST                    https://<workspace-url>
  SQL_WAREHOUSE_ID                   warehouse to run the query on
  DATABRICKS_TOKEN                   a personal access token, or
  DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET   OAuth client credentials (all-apis scope)
Exit code 0 when missing = 0 and every row is a known coordinate.
"""
import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request


def token(host):
    if os.environ.get("DATABRICKS_TOKEN"):
        return os.environ["DATABRICKS_TOKEN"]
    cid, sec = os.environ.get("DATABRICKS_CLIENT_ID"), os.environ.get("DATABRICKS_CLIENT_SECRET")
    if not (cid and sec):
        sys.exit("set DATABRICKS_TOKEN, or DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET")
    basic = base64.b64encode(("%s:%s" % (cid, sec)).encode()).decode()
    req = urllib.request.Request(host + "/oidc/v1/token", method="POST",
                                 data=urllib.parse.urlencode({"grant_type": "client_credentials", "scope": "all-apis"}).encode(),
                                 headers={"Authorization": "Basic " + basic,
                                          "Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())["access_token"]


def run_sql(host, tok, warehouse, statement):
    body = json.dumps({"warehouse_id": warehouse, "statement": statement, "wait_timeout": "50s",
                       "on_wait_timeout": "CONTINUE", "disposition": "INLINE", "format": "JSON_ARRAY"}).encode()
    req = urllib.request.Request(host + "/api/2.0/sql/statements", data=body, method="POST",
                                 headers={"Authorization": "Bearer " + tok, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        j = json.loads(r.read().decode())
    while j.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(2)
        req = urllib.request.Request(host + "/api/2.0/sql/statements/%s" % j["statement_id"],
                                     headers={"Authorization": "Bearer " + tok})
        with urllib.request.urlopen(req, timeout=60) as r:
            j = json.loads(r.read().decode())
    state = j.get("status", {}).get("state")
    if state != "SUCCEEDED":
        sys.exit("SQL failed (%s): %s" % (state, j.get("status", {}).get("error", {}).get("message", "")[:300]))
    return j.get("result", {}).get("data_array", [])


def ends_from_kafka(container):
    out = subprocess.run(["docker", "compose", "exec", "-T", container, "/opt/kafka/bin/kafka-get-offsets.sh",
                          "--bootstrap-server", "localhost:9092", "--topic", ARGS.topic],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                         cwd=ARGS.compose_dir).stdout
    ends = {}
    for line in out.splitlines():
        parts = line.split(":")
        if len(parts) == 3:
            ends[int(parts[1])] = int(parts[2])
    return ends


def main():
    global ARGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--dlq", required=True)
    ap.add_argument("--end", help="end offset per partition, e.g. 0:70,1:65")
    ap.add_argument("--kafka-container", help="read the end offsets from this compose service instead")
    ap.add_argument("--compose-dir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "compose"))
    ARGS = ap.parse_args()
    if ARGS.end:
        ends = {int(p): int(e) for p, e in (x.split(":") for x in ARGS.end.split(","))}
    elif ARGS.kafka_container:
        ends = ends_from_kafka(ARGS.kafka_container)
    else:
        sys.exit("give --end or --kafka-container")
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    warehouse = os.environ.get("SQL_WAREHOUSE_ID")
    if not host or not warehouse:
        sys.exit("set DATABRICKS_HOST and SQL_WAREHOUSE_ID")
    tok = token(host)
    expected = " UNION ALL ".join("SELECT %d AS p, o FROM (SELECT explode(sequence(0, %d)) AS o)" % (p, e - 1)
                                  for p, e in sorted(ends.items()))
    union = ("SELECT _kafka.partition AS p, _kafka.offset AS o FROM %s WHERE _kafka.offset IS NOT NULL "
             "UNION ALL SELECT `partition` AS p, `offset` AS o FROM %s") % (ARGS.table, ARGS.dlq)
    counts = run_sql(host, tok, warehouse,
                     "SELECT (SELECT count(*) FROM (%s) e LEFT ANTI JOIN (%s) u ON e.p = u.p AND e.o = u.o), "
                     "(SELECT count(*) FROM (%s) u LEFT ANTI JOIN (%s) e ON e.p = u.p AND e.o = u.o), "
                     "(SELECT count(*) FROM (%s) x), "
                     "(SELECT count(*) FROM (SELECT DISTINCT p, o FROM (%s) y) z), "
                     "(SELECT count(*) FROM %s), (SELECT count(*) FROM %s), "
                     "(SELECT count(_kafka.timestamp) FROM %s)"
                     % (expected, union, union, expected, union, union, ARGS.table, ARGS.dlq, ARGS.table))[0]
    missing, outside, total, distinct, t_rows, d_rows, ts_rows = [int(x) for x in counts]
    expected_n = sum(ends.values())
    print("expected coordinates : %d" % expected_n)
    print("in target table      : %d rows" % t_rows)
    print("in dead-letter table : %d rows" % d_rows)
    print("missing              : %d" % missing)
    print("duplicates           : %d" % (total - distinct))
    print("outside the range    : %d" % outside)
    print("_kafka.timestamp set : %d of %d target rows" % (ts_rows, t_rows))
    ok = missing == 0 and outside == 0
    print("RESULT at-least-once: %s" % ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
