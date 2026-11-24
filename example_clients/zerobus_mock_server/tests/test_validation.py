"""Record checks: every problem reported at once, nulls as absent fields, and protobuf descriptors."""

import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from zerobus_mock_server.schema import (FIELD_NOT_IN_TABLE, MISSING_REQUIRED_COLUMN, TYPE_INCOMPATIBLE,
                                        Column, RecordRejected, check_fields, validate_record)
from zerobus_mock_server.validation import ProtoStream

COLUMNS = (Column("device_name", "STRING", False), Column("temp", "INT", True))


def test_every_problem_in_a_record_is_reported_together():
    with pytest.raises(RecordRejected) as info:
        validate_record(COLUMNS, "t", {"temp": "hot", "extra": 1})
    tokens = {c.token for c in info.value.causes}
    assert tokens == {MISSING_REQUIRED_COLUMN, TYPE_INCOMPATIBLE, FIELD_NOT_IN_TABLE}


def test_a_null_is_the_same_as_an_absent_field():
    assert validate_record(COLUMNS, "t", {"device_name": "a", "temp": None, "extra": None}) == \
        {"device_name": "a", "temp": None}


def test_check_fields_names_wrong_kinds_and_missing_required_columns():
    causes = check_fields(COLUMNS, "t", [("temp", "STRING"), ("extra", "INT")])
    assert {c.token for c in causes} == {TYPE_INCOMPATIBLE, FIELD_NOT_IN_TABLE, MISSING_REQUIRED_COLUMN}


def _reading_descriptor() -> descriptor_pb2.DescriptorProto:
    FD = descriptor_pb2.FieldDescriptorProto
    dp = descriptor_pb2.DescriptorProto(name="AirQuality")
    loc = dp.nested_type.add(name="Location")
    loc.field.add(name="lat", number=1, type=FD.TYPE_DOUBLE, label=FD.LABEL_OPTIONAL)
    loc.field.add(name="lon", number=2, type=FD.TYPE_DOUBLE, label=FD.LABEL_OPTIONAL)
    dp.field.add(name="device_name", number=1, type=FD.TYPE_STRING, label=FD.LABEL_OPTIONAL)
    dp.field.add(name="temp", number=2, type=FD.TYPE_INT32, label=FD.LABEL_OPTIONAL)
    dp.field.add(name="tags", number=3, type=FD.TYPE_STRING, label=FD.LABEL_REPEATED)
    dp.field.add(name="location", number=4, type=FD.TYPE_MESSAGE, label=FD.LABEL_OPTIONAL,
                 type_name=".zbtest.AirQuality.Location")
    return dp


def test_a_nested_descriptor_loads_its_field_kinds_and_decodes_records():
    dp = _reading_descriptor()
    stream = ProtoStream(dp.SerializeToString())
    assert ("location", "STRUCT") in stream.fields and ("tags", "ARRAY") in stream.fields
    assert ("temp", "INT") in stream.fields

    # A record encoded with the same message, in its own package.
    pool = descriptor_pool.DescriptorPool()
    pool.Add(descriptor_pb2.FileDescriptorProto(name="zbtest.proto", package="zbtest", message_type=[dp]))
    cls = message_factory.GetMessageClass(pool.FindMessageTypeByName("zbtest.AirQuality"))
    msg = cls(device_name="a", temp=3, tags=["x"])
    msg.location.lat = 1.5
    assert stream.decode(msg.SerializeToString()) == {
        "device_name": "a", "temp": 3, "tags": ["x"], "location": {"lat": 1.5}}
