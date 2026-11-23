"""End-to-end check against a running mock (any process): the real Python SDK over gRPC, and REST.

    python scripts/e2e_check.py --rest-url https://localhost:8443 \
        --grpc-endpoint https://localhost:50051 --ca /path/to/ca.pem \
        --table main.default.air_quality --grpc-count 1000 --rest-count 100

It resets the table first, ingests the records, then reads the counts back through
the mock's control API (GET /_mock/tables/<table>/stats). Needs the Python SDK
(databricks-zerobus-ingest-sdk) and httpx. The SDK trusts the mock's CA through
SSL_CERT_FILE, which this script sets to a bundle of the system roots plus the CA.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

import certifi
import httpx


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rest-url", required=True)
    ap.add_argument("--grpc-endpoint", required=True)
    ap.add_argument("--ca", required=True)
    ap.add_argument("--table", default="main.default.air_quality")
    ap.add_argument("--grpc-count", type=int, default=1000)
    ap.add_argument("--rest-count", type=int, default=100)
    args = ap.parse_args()

    bundle = Path(tempfile.mkdtemp(prefix="zb-e2e-")) / "trust.pem"
    bundle.write_text(Path(certifi.where()).read_text() + "\n" + Path(args.ca).read_text())
    os.environ["SSL_CERT_FILE"] = str(bundle)

    from zerobus.sdk.shared import TableProperties
    from zerobus.sdk.sync import ZerobusSdk

    client = httpx.Client(base_url=args.rest_url, verify=str(bundle), timeout=60)
    client.post("/_mock/reset", json={"tables": [args.table]})

    # gRPC: the real SDK
    sdk = ZerobusSdk(args.grpc_endpoint, args.rest_url)
    stream = sdk.create_stream("mock-client-id", "mock-client-secret", TableProperties(args.table))
    for i in range(args.grpc_count):
        stream.ingest_record_offset({"device_name": f"py-grpc-{i}", "temp": 20 + i % 15, "humidity": 50 + i % 40})
    stream.flush()
    stream.close()

    # REST: token, then one insert per record
    token_resp = client.post(
        "/oidc/v1/token",
        data={
            "grant_type": "client_credentials",
            "scope": "all-apis",
            "resource": "api://databricks/workspaces/localhost/zerobusDirectWriteApi",
            "authorization_details": json.dumps([{
                "type": "unity_catalog_privileges", "privileges": ["SELECT", "MODIFY"],
                "object_type": "TABLE", "object_full_path": args.table, "operations": ["zerobuswrite"],
            }]),
        },
        auth=("mock-client-id", "mock-client-secret"),
    )
    token_resp.raise_for_status()
    token = token_resp.json()["access_token"]
    statuses = {}
    for i in range(args.rest_count):
        resp = client.post(
            f"/zerobus/v1/tables/{args.table}/insert",
            content=json.dumps([{"device_name": f"py-rest-{i}", "temp": i, "humidity": 1}]),
            headers={"authorization": f"Bearer {token}", "content-type": "application/json"},
        )
        statuses[resp.status_code] = statuses.get(resp.status_code, 0) + 1

    stats = client.get(f"/_mock/tables/{args.table}/stats", params={"key": "device_name"}).json()
    print(json.dumps({
        "table": args.table,
        "grpc_records_sent": args.grpc_count,
        "rest_records_sent": args.rest_count,
        "rest_status_counts": statuses,
        "rows_in_table": stats["rows"],
        "rows_by_protocol": stats["rows_by_protocol"],
        "distinct_device_names": stats["distinct_keys"],
        "duplicate_rows": stats["duplicate_rows"],
    }, indent=2))
    ok = stats["rows"] == args.grpc_count + args.rest_count and statuses == {200: args.rest_count}
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
