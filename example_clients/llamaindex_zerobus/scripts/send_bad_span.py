#!/usr/bin/env python3
"""Send one valid span and one bad span to the Collector's OTLP/HTTP endpoint, then print the response.

The bad span has a trace ID of 6 hex characters instead of 32, which is not valid OTLP. Use it to see
how the Collector handles a bad record in a batch (see the README, Troubleshooting).

  python3 scripts/send_bad_span.py [http://127.0.0.1:4318/v1/traces] [--valid-only]

--valid-only sends the valid span alone, as the control for the bad-record run.
"""
import json
import sys
import time
import urllib.error
import urllib.request

args = [a for a in sys.argv[1:] if not a.startswith("--")]
valid_only = "--valid-only" in sys.argv
endpoint = args[0] if args else "http://127.0.0.1:4318/v1/traces"
now = time.time_ns()


def span(name, trace_id, span_id):
    return {
        "traceId": trace_id,
        "spanId": span_id,
        "name": name,
        "kind": 1,
        "startTimeUnixNano": str(now),
        "endTimeUnixNano": str(now + 1_000_000),
    }


body = {
    "resourceSpans": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "bad-record-check"}}]},
        "scopeSpans": [{
            "scope": {"name": "bad-record-check"},
            "spans": [span("valid.span", "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")]
            + ([] if valid_only else [span("bad.span", "abc123", "b7ad6b7169203332")]),
        }],
    }]
}
req = urllib.request.Request(endpoint, data=json.dumps(body).encode(),
                             headers={"Content-Type": "application/json"}, method="POST")
try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        print("status", resp.status, resp.read().decode()[:500])
except urllib.error.HTTPError as err:
    print("status", err.code, err.read().decode()[:500])
