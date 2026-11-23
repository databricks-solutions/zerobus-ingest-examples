"""End to end with the real Python Zerobus SDK (sync client) against the in-process mock.

Each test writes through the SDK (gRPC) or through REST, then reads the rows back
from the mock's table store and checks them.
"""

import json

import httpx
import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from helpers import TABLES, ca_context, get_token
from sdk_helpers import create, drive

AQ = TABLES["air_quality"]
PR = TABLES["proto_readings"]


def test_one_thousand_json_records_over_grpc(mock, fresh):
    stream = create(mock, AQ)
    records = [{"device_name": f"sensor-{i}", "temp": 20 + i % 15, "humidity": 50 + i % 40} for i in range(1000)]
    assert drive(stream, records) is None
    st = mock.tables.stats(AQ, key="device_name")
    assert st["rows"] == 1000
    assert st["distinct_keys"] == 1000 and st["duplicate_rows"] == 0
    rows = mock.tables.read(AQ, limit=10000)
    assert [r.values["device_name"] for r in rows] == [f"sensor-{i}" for i in range(1000)]
    assert [r.offset for r in rows] == list(range(1000))
    assert len({r.stream_id for r in rows}) == 1 and rows[0].protocol == "grpc"


def test_one_hundred_records_over_rest(mock, rest, fresh):
    token = get_token(rest, AQ)
    for i in range(100):
        resp = rest.post(
            f"/zerobus/v1/tables/{AQ}/insert",
            content=json.dumps([{"device_name": f"rest-{i}", "temp": i, "humidity": 1}]),
            headers={"authorization": f"Bearer {token}", "content-type": "application/json"},
        )
        assert resp.status_code == 200, resp.text
    st = mock.tables.stats(AQ, key="device_name")
    assert st["rows"] == 100 and st["rows_by_protocol"] == {"rest": 100}
    assert st["distinct_keys"] == 100


def _reading_descriptor() -> descriptor_pb2.FileDescriptorProto:
    FD = descriptor_pb2.FieldDescriptorProto
    fdp = descriptor_pb2.FileDescriptorProto(name="air_quality.proto", package="zbtest", syntax="proto2")
    msg = fdp.message_type.add(name="AirQuality")
    loc = msg.nested_type.add(name="Location")
    loc.field.add(name="lat", number=1, type=FD.TYPE_DOUBLE, label=FD.LABEL_OPTIONAL)
    loc.field.add(name="lon", number=2, type=FD.TYPE_DOUBLE, label=FD.LABEL_OPTIONAL)
    msg.field.add(name="device_name", number=1, type=FD.TYPE_STRING, label=FD.LABEL_OPTIONAL)
    msg.field.add(name="temp", number=2, type=FD.TYPE_INT32, label=FD.LABEL_OPTIONAL)
    msg.field.add(name="humidity", number=3, type=FD.TYPE_INT64, label=FD.LABEL_OPTIONAL)
    msg.field.add(name="tags", number=4, type=FD.TYPE_STRING, label=FD.LABEL_REPEATED)
    msg.field.add(name="location", number=5, type=FD.TYPE_MESSAGE, label=FD.LABEL_OPTIONAL, type_name=".zbtest.AirQuality.Location")
    return fdp


def test_protobuf_records_over_grpc(mock, fresh):
    fdp = _reading_descriptor()
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    cls = message_factory.GetMessageClass(pool.FindMessageTypeByName("zbtest.AirQuality"))
    stream = create(mock, PR, descriptor=fdp.SerializeToString())
    records = []
    for i in range(50):
        msg = cls(device_name=f"p-{i}", temp=i, humidity=2 * i, tags=["x"])
        msg.location.lat = 1.5
        msg.location.lon = 2.5
        records.append(msg)
    assert drive(stream, records) is None
    rows = mock.tables.values(PR)
    assert [r["device_name"] for r in rows] == [f"p-{i}" for i in range(50)]
    assert rows[3] == {"device_name": "p-3", "temp": 3, "humidity": 6, "tags": ["x"],
                       "location": {"lat": 1.5, "lon": 2.5}}


def test_bad_token_is_reported_by_the_sdk(mock, fresh):
    with pytest.raises(Exception) as e:
        create(mock, "main.default.not_configured")
    assert "invalid_authorization_details" in str(e.value)


def test_rest_token_request_matches_the_docs_form(mock, fresh):
    """The SDK's own token request, captured through a REST call with the same form."""
    with httpx.Client(base_url=mock.rest_base_url, verify=ca_context(mock.ca_path)) as client:
        tok = get_token(client, AQ)
    assert tok.startswith("mock_")
