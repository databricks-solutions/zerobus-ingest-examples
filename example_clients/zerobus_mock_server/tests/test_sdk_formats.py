"""The real Python SDK over Arrow Flight DoPut and Avro: normal ingest, refusal at create, token and abort."""

import json

import pyarrow as pa
import pytest
from zerobus.sdk.shared import TableProperties
from zerobus.sdk.sync import ZerobusSdk

from helpers import AQ, CLIENT_ID, CLIENT_SECRET
from sdk_helpers import contract, drive, sdk_for

ARROW_SCHEMA = pa.schema([pa.field("device_name", pa.string(), nullable=False),
                          pa.field("temp", pa.int32()), pa.field("humidity", pa.int64())])
AVRO = {"type": "record", "name": "AirQuality", "fields": [
    {"name": "device_name", "type": "string"},
    {"name": "temp", "type": ["null", "int"], "default": None},
    {"name": "humidity", "type": ["null", "long"], "default": None}]}


def arrow_stream(mock, schema=ARROW_SCHEMA):
    return sdk_for(mock).create_arrow_stream(AQ, schema, CLIENT_ID, CLIENT_SECRET)


def avro_stream(mock, schema=AVRO):
    return sdk_for(mock).create_stream(CLIENT_ID, CLIENT_SECRET,
                                       TableProperties(AQ, avro_schema=json.dumps(schema)))


def arrow_batches(n_batches, rows, prefix="a"):
    return [pa.record_batch({"device_name": [f"{prefix}-{b * rows + i}" for i in range(rows)],
                             "temp": list(range(rows)), "humidity": [1] * rows}, schema=ARROW_SCHEMA)
            for b in range(n_batches)]


def test_arrow_batches_land_with_their_protocol_label(mock):
    stream = arrow_stream(mock)
    for batch in arrow_batches(4, 250, "x"):
        stream.ingest_batch(batch)
    stream.flush()
    stream.close()
    rows = mock.tables.read(AQ, limit=5000)
    assert len(rows) == 1000 and {r.protocol for r in rows} == {"arrow"}


def test_avro_records_land_against_the_writer_schema(mock):
    records = [{"device_name": f"v-{i}", "temp": i, "humidity": 3} for i in range(500)]
    assert drive(avro_stream(mock), records) is None
    assert [r["device_name"] for r in mock.tables.rows(AQ)] == [f"v-{i}" for i in range(500)]


def test_arrow_retryable_abort_reconnects_and_resends(mock):
    mock.scenarios.fail_next("stream-abort-retryable", after=1)
    stream = arrow_stream(mock)
    for batch in arrow_batches(4, 10, "r"):
        stream.ingest_batch(batch)
    stream.flush()
    stream.close()
    cov = mock.tables.coverage(AQ, expected=40, key="device_name")
    assert cov["missing"] == 0 and cov["duplicate_rows"] >= 1


def test_avro_retryable_abort_reconnects_and_resends(mock):
    mock.scenarios.fail_next("stream-abort-retryable", after=3)
    records = [{"device_name": f"v-{i}", "temp": i} for i in range(10)]
    assert drive(avro_stream(mock), records) is None
    assert mock.tables.coverage(AQ, expected=10, key="device_name")["missing"] == 0


@pytest.mark.parametrize("which", ["arrow", "avro"])
def test_token_failure_is_raised_by_the_sdk_for_each_format(mock, which):
    mock.scenarios.fail_next("auth-failure")
    make = arrow_stream if which == "arrow" else avro_stream
    with pytest.raises(Exception) as info:
        make(mock)
    assert contract("auth-failure") in str(info.value)


def test_schemas_that_do_not_fit_are_refused_at_create(mock):
    extra_arrow = ARROW_SCHEMA.append(pa.field("region", pa.string()))
    with pytest.raises(Exception) as arrow_info:
        arrow_stream(mock, extra_arrow)
    assert "does not fit" in str(arrow_info.value) and "region" in str(arrow_info.value)

    extra_avro = dict(AVRO, fields=AVRO["fields"] + [{"name": "region", "type": "string"}])
    with pytest.raises(Exception) as avro_info:
        avro_stream(mock, extra_avro)
    assert "does not fit" in str(avro_info.value) and "region" in str(avro_info.value)
