"""Record validation rules from the Zerobus schema-management and rescue docs."""

import json

import pytest

from zerobus_mock_server.schema import SchemaSnapshot, make_column
from zerobus_mock_server.validation import RecordRejected, validate_object


def snap(rescue=None, cols=None):
    columns = cols or (
        make_column("device_name", "STRING", False),
        make_column("temp", "INT", True),
        make_column("humidity", "LONG", True),
    )
    if rescue:
        columns = columns + (make_column("rescue", "VARIANT", True),)
    return SchemaSnapshot("main.default.air_quality", 1, tuple(columns), rescue)


def test_all_nullable_columns_may_be_omitted():
    row = validate_object(snap(), {"device_name": "sensor-1"})
    assert row == {"device_name": "sensor-1", "temp": None, "humidity": None}


def test_missing_non_nullable_column_is_rejected():
    with pytest.raises(RecordRejected) as e:
        validate_object(snap(), {"temp": 22})
    assert [c.token for c in e.value.causes] == ["MISSING_REQUIRED_COLUMN"]


def test_unknown_field_is_rejected_without_rescue_column():
    with pytest.raises(RecordRejected) as e:
        validate_object(snap(), {"device_name": "s", "region": "us"})
    cause = e.value.causes[0]
    assert cause.token == "FIELD_NOT_IN_TABLE" and cause.error_code == "4044"


def test_type_mismatch_is_rejected_without_rescue_column():
    with pytest.raises(RecordRejected) as e:
        validate_object(snap(), {"device_name": "s", "temp": "72F"})
    assert e.value.causes[0].token == "TYPE_INCOMPATIBLE"


def test_docs_example_routes_mismatch_and_extra_field_to_rescue():
    # Docs example: temp INT gets "72F", extra_field is not a column.
    record = {"device_name": "sensor-1", "temp": "72F", "humidity": 87, "extra_field": "some value"}
    row = validate_object(snap(rescue="rescue"), record)
    assert row["device_name"] == "sensor-1"
    assert row["temp"] is None
    assert row["humidity"] == 87
    assert json.loads(row["rescue"]) == {"temp": "72F", "extra_field": "some value"}


def test_type_mismatch_on_non_nullable_column_is_not_rescued():
    s = snap(rescue="rescue")
    with pytest.raises(RecordRejected) as e:
        validate_object(s, {"device_name": 5})
    assert e.value.causes[0].token == "TYPE_INCOMPATIBLE"


def test_null_is_treated_as_absent_and_not_rescued():
    row = validate_object(snap(rescue="rescue"), {"device_name": "s", "unknown": None, "temp": None})
    assert row["rescue"] is None


def test_record_cannot_set_the_rescue_column():
    with pytest.raises(RecordRejected) as e:
        validate_object(snap(rescue="rescue"), {"device_name": "s", "rescue": "{}"})
    assert e.value.causes[0].token == "RESCUE_COLUMN_RESERVED"
    row = validate_object(snap(rescue="rescue"), {"device_name": "s", "rescue": None})
    assert row["rescue"] is None


def test_several_problems_are_reported_together():
    with pytest.raises(RecordRejected) as e:
        validate_object(snap(), {"temp": "x", "nope": 1})
    tokens = sorted(c.token for c in e.value.causes)
    assert tokens == ["FIELD_NOT_IN_TABLE", "MISSING_REQUIRED_COLUMN", "TYPE_INCOMPATIBLE"]
