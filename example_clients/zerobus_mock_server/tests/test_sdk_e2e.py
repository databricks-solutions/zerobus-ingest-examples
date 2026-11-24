"""Normal ingest through the real Python SDK (gRPC EphemeralStream), read back from the mock's tables."""

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from helpers import AQ, PR
from sdk_helpers import create, create_error, drive


def _file_descriptor(temp_type=descriptor_pb2.FieldDescriptorProto.TYPE_INT32) -> descriptor_pb2.FileDescriptorProto:
    """The file descriptor the SDK takes (SDK 1.11 needs message types in it), with a nested Location message."""
    FD = descriptor_pb2.FieldDescriptorProto
    dp = descriptor_pb2.DescriptorProto(name="AirQuality")
    loc = dp.nested_type.add(name="Location")
    loc.field.add(name="lat", number=1, type=FD.TYPE_DOUBLE, label=FD.LABEL_OPTIONAL)
    loc.field.add(name="lon", number=2, type=FD.TYPE_DOUBLE, label=FD.LABEL_OPTIONAL)
    dp.field.add(name="device_name", number=1, type=FD.TYPE_STRING, label=FD.LABEL_OPTIONAL)
    dp.field.add(name="temp", number=2, type=temp_type, label=FD.LABEL_OPTIONAL)
    dp.field.add(name="humidity", number=3, type=FD.TYPE_INT64, label=FD.LABEL_OPTIONAL)
    dp.field.add(name="tags", number=4, type=FD.TYPE_STRING, label=FD.LABEL_REPEATED)
    dp.field.add(name="location", number=5, type=FD.TYPE_MESSAGE, label=FD.LABEL_OPTIONAL,
                 type_name=".zbtest.AirQuality.Location")
    return descriptor_pb2.FileDescriptorProto(name="zbtest.proto", package="zbtest", message_type=[dp])


def test_one_thousand_json_records_over_grpc(mock):
    records = [{"device_name": f"sensor-{i}", "temp": 20 + i % 15, "humidity": 50 + i % 40} for i in range(1000)]
    assert drive(create(mock, AQ), records) is None
    rows = mock.tables.read(AQ, limit=5000)
    assert [r.values["device_name"] for r in rows] == [f"sensor-{i}" for i in range(1000)]
    assert [r.offset for r in rows] == list(range(1000))
    assert {r.protocol for r in rows} == {"grpc"} and len({r.stream_id for r in rows}) == 1


def test_protobuf_records_with_a_nested_struct_land_typed(mock):
    fdp = _file_descriptor()
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    cls = message_factory.GetMessageClass(pool.FindMessageTypeByName("zbtest.AirQuality"))
    records = []
    for i in range(50):
        msg = cls(device_name=f"p-{i}", temp=i, humidity=2 * i, tags=["x"])
        msg.location.lat = 1.5
        msg.location.lon = 2.5
        records.append(msg)
    assert drive(create(mock, PR, descriptor=fdp.SerializeToString()), records) is None
    rows = mock.tables.rows(PR)
    assert [r["device_name"] for r in rows] == [f"p-{i}" for i in range(50)]
    assert rows[3] == {"device_name": "p-3", "temp": 3, "humidity": 6, "tags": ["x"],
                       "location": {"lat": 1.5, "lon": 2.5}}


def test_protobuf_descriptor_with_a_wrong_field_type_is_refused_at_create(mock):
    fdp = _file_descriptor(temp_type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    exc = create_error(mock, PR, descriptor=fdp.SerializeToString())
    assert "does not fit" in str(exc) and "temp" in str(exc)
