#!/usr/bin/env python3
"""Send one OTLP/HTTP trace request with one good span and one bad span.

The bad span has a 3-byte trace_id (must be 16 bytes) and no start time. Use it to see how the
Collector and Zerobus treat a record the table contract rejects.

Modes:
  zero-start        (default) the bad span has a valid trace_id and span_id but start_time_unix_nano 0.
                    The table contract needs a start time, so the record is rejected by Zerobus.
  bad-trace-id      the bad span has a 3-byte trace_id. The Collector rejects the whole request at
                    protobuf decode (HTTP 400), so the good span in the same request is lost too.

Usage: send_bad_span.py [endpoint] [zero-start|bad-trace-id]   (default http://127.0.0.1:4318 zero-start)
"""

import sys
import time
import urllib.request

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span

endpoint = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:4318").rstrip("/")
mode = sys.argv[2] if len(sys.argv) > 2 else "zero-start"
if mode not in ("zero-start", "bad-trace-id"):
    sys.exit("mode must be zero-start or bad-trace-id")
now = time.time_ns()

good = Span(
    trace_id=bytes(range(1, 17)),
    span_id=bytes(range(1, 9)),
    name=f"bad-record-test good span ({mode})",
    kind=Span.SPAN_KIND_INTERNAL,
    start_time_unix_nano=now,
    end_time_unix_nano=now + 1_000_000,
)
bad = Span(
    trace_id=b"\x01\x02\x03" if mode == "bad-trace-id" else bytes(range(17, 33)),
    span_id=bytes(range(9, 17)),
    name=f"bad-record-test bad span ({mode})",
    kind=Span.SPAN_KIND_INTERNAL,
    start_time_unix_nano=0 if mode == "zero-start" else now,
    end_time_unix_nano=now,
)
request = ExportTraceServiceRequest(
    resource_spans=[
        ResourceSpans(
            resource=Resource(
                attributes=[KeyValue(key="service.name", value=AnyValue(string_value="bad-record-test"))]
            ),
            scope_spans=[ScopeSpans(spans=[good, bad])],
        )
    ]
)
req = urllib.request.Request(
    f"{endpoint}/v1/traces",
    data=request.SerializeToString(),
    headers={"Content-Type": "application/x-protobuf"},
    method="POST",
)
try:
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = resp.read()
        print(f"HTTP {resp.status}; response bytes {len(body)}; {body[:200]!r}")
except urllib.error.HTTPError as e:
    print(f"HTTP {e.code}: {e.read()[:300]!r}")
