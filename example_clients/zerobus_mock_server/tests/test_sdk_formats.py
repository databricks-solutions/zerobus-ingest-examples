"""The real Python SDK over every record format: JSON, protobuf, Arrow, Avro, and persistent streams.

Each format ingests records, the rows are read back through the mock's control API, and
the counts are checked. Each format also runs the three fault scenarios that the language
matrix uses: a retryable mid-stream abort (the SDK reconnects), a token 401, and a schema
mismatch (4044).
"""

import json

import pyarrow as pa
import pytest
from zerobus.sdk.shared import TableProperties
from zerobus.sdk.sync import ZerobusSdk

from helpers import TABLES
from sdk_helpers import create, drive, sdk_for

AQ = TABLES["air_quality"]
ARROW_SCHEMA = pa.schema([pa.field("device_name", pa.string(), nullable=False),
                          pa.field("temp", pa.int32()), pa.field("humidity", pa.int64())])
AVRO = {"type": "record", "name": "AirQuality", "fields": [
    {"name": "device_name", "type": "string"},
    {"name": "temp", "type": ["null", "int"], "default": None},
    {"name": "humidity", "type": ["null", "long"], "default": None}]}


def add(mock, **spec):
    return mock.add_fault(spec)


def fired(mock):
    return [e["fault_type"] for e in mock.faults.events()]


def arrow_stream(mock):
    return sdk_for(mock).create_arrow_stream(AQ, ARROW_SCHEMA, "mock-client-id", "mock-client-secret")


def arrow_batches(n_batches, rows, prefix="a"):
    return [pa.record_batch({"device_name": [f"{prefix}-{b}-{i}" for i in range(rows)],
                             "temp": list(range(rows)), "humidity": [1] * rows}, schema=ARROW_SCHEMA)
            for b in range(n_batches)]


def avro_stream(mock, table=AQ):
    return sdk_for(mock).create_stream(
        "mock-client-id", "mock-client-secret", TableProperties(table, avro_schema=json.dumps(AVRO)))


def test_json_protobuf_and_arrow_and_avro_land_the_expected_counts(mock, fresh):
    s = create(mock, AQ)
    assert drive(s, [{"device_name": f"j-{i}", "temp": i} for i in range(100)]) is None
    a = arrow_stream(mock)
    for b in arrow_batches(4, 250, "x"):
        a.ingest_batch(b)
    a.flush()
    a.close()
    s = avro_stream(mock)
    assert drive(s, [{"device_name": f"v-{i}", "temp": i, "humidity": 3} for i in range(500)]) is None
    st = mock.tables.stats(AQ, key="device_name")
    assert st["rows"] == 100 + 1000 + 500
    assert st["rows_by_protocol"] == {"grpc": 600, "arrow": 1000}
    assert st["distinct_keys"] == st["rows"]  # no duplicates when nothing fails


def test_arrow_retryable_abort_reconnects_and_resends(mock, fresh):
    add(mock, type="stream_abort", protocol="grpc", table=AQ, trigger={"mode": "after", "after": 1, "times": 1},
        params={"retryable": True, "persist_unacked": True})
    a = arrow_stream(mock)
    for b in arrow_batches(4, 10, "r"):
        a.ingest_batch(b)
    a.flush()
    a.close()
    st = mock.tables.stats(AQ, key="device_name")
    assert st["distinct_keys"] == 40 and st["rows"] > 40  # at-least-once: the aborted batch was stored unacked
    assert fired(mock) == ["stream_abort"]
    assert len({r.stream_id for r in mock.tables.read(AQ, limit=1000)}) == 2


def test_avro_retryable_abort_reconnects_and_resends(mock, fresh):
    add(mock, type="stream_abort", protocol="grpc", table=AQ, trigger={"mode": "after", "after": 3, "times": 1},
        params={"retryable": True, "persist_unacked": False})
    s = avro_stream(mock)
    assert drive(s, [{"device_name": f"v-{i}", "temp": i} for i in range(10)]) is None
    st = mock.tables.stats(AQ, key="device_name")
    assert st["distinct_keys"] == 10
    assert fired(mock) == ["stream_abort"]


@pytest.mark.parametrize("which", ["arrow", "avro"])
def test_token_401_is_raised_by_the_sdk(mock, fresh, which):
    add(mock, type="token_invalid_client", table=AQ)
    with pytest.raises(Exception) as e:
        arrow_stream(mock) if which == "arrow" else avro_stream(mock)
    assert "invalid_client" in str(e.value)


def test_arrow_schema_mismatch_is_refused_at_stream_creation(mock, fresh):
    # 4044-style: a field that is not a column of the table
    bad = ARROW_SCHEMA.append(pa.field("region", pa.string()))
    with pytest.raises(Exception) as e:
        sdk_for(mock).create_arrow_stream(AQ, bad, "mock-client-id", "mock-client-secret")
    assert "FIELD_NOT_IN_TABLE" in str(e.value)


def test_avro_schema_mismatch_is_refused_at_stream_creation(mock, fresh):
    bad = dict(AVRO, fields=AVRO["fields"] + [{"name": "region", "type": "string"}])
    with pytest.raises(Exception) as e:
        sdk_for(mock).create_stream("mock-client-id", "mock-client-secret",
                                    TableProperties(AQ, avro_schema=json.dumps(bad)))
    assert "FIELD_NOT_IN_TABLE" in str(e.value)


def test_arrow_record_mismatch_injected_by_4044_fault_surfaces_on_ingest(mock, fresh):
    add(mock, type="schema_mismatch", protocol="grpc", table=AQ, trigger={"mode": "after", "after": 1, "times": 1})
    a = arrow_stream(mock)
    failed = None
    try:
        for b in arrow_batches(3, 5, "m"):
            a.ingest_batch(b)
        a.flush()
    except Exception as exc:  # noqa: BLE001
        failed = exc
    assert failed is not None and "does not fit" in str(failed)
    assert mock.tables.stats(AQ)["rows"] == 5  # the batch before the fault is kept


def test_persistent_json_stream_is_created_and_resumed_through_the_sdk(mock, fresh):
    sdk = sdk_for(mock)
    b = sdk.persistent_stream_builder().table(AQ).oauth("mock-client-id", "mock-client-secret").json()
    s = b.build()
    sid = s.stream_id
    for i in range(5):
        s.ingest_record_offset({"device_name": f"p-{i}", "temp": i})
    s.flush()
    s.close()
    r = sdk.persistent_stream_builder().table(AQ).oauth("mock-client-id", "mock-client-secret").json().resume(sid)
    assert r.resume_watermark == 4
    for i in range(5, 8):
        r.ingest_record_offset({"device_name": f"p-{i}", "temp": i})
    r.flush()
    r.close()
    assert [x.offset for x in mock.tables.read(AQ)] == [0, 1, 2, 3, 4, 5, 6, 7]
