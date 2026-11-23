"""Type parsing and value coercion (unit tests, no server)."""

import base64
import datetime as dt
from decimal import Decimal

import pytest

from zerobus_mock_server.schema import (
    FIELD_NOT_IN_TABLE,
    MISSING_REQUIRED_COLUMN,
    TYPE_INCOMPATIBLE,
    CoerceError,
    SchemaError,
    coerce,
    make_column,
    parse_type,
    to_json_value,
)


@pytest.mark.parametrize("text, kind", [
    ("STRING", "STRING"), ("varchar", "STRING"), ("INTEGER", "INT"), ("BIGINT", "LONG"),
    ("SMALLINT", "SHORT"), ("TINYINT", "BYTE"), ("REAL", "FLOAT"), ("BOOL", "BOOLEAN"),
    ("TIMESTAMPNTZ", "TIMESTAMP_NTZ"), ("VARIANT", "VARIANT"), ("DATE", "DATE"),
])
def test_scalar_aliases(text, kind):
    assert parse_type(text).kind == kind


def test_decimal_and_nested_types():
    d = parse_type("DECIMAL(12, 3)")
    assert (d.kind, d.precision, d.scale) == ("DECIMAL", 12, 3)
    arr = parse_type("ARRAY<ARRAY<INT>>")
    assert arr.kind == "ARRAY" and arr.value.kind == "ARRAY" and arr.value.value.kind == "INT"
    mp = parse_type("MAP<STRING, LONG>")
    assert mp.key.kind == "STRING" and mp.value.kind == "LONG"
    st = parse_type("STRUCT<lat: DOUBLE NOT NULL, lon: DOUBLE>")
    assert [f.name for f in st.fields] == ["lat", "lon"]
    assert st.fields[0].nullable is False and st.fields[1].nullable is True


@pytest.mark.parametrize("bad", ["", "FOO", "ARRAY<INT", "DECIMAL(40,2)", "STRUCT<a INT>", "MAP<STRING>", "INT extra"])
def test_bad_types_are_rejected(bad):
    with pytest.raises(SchemaError):
        parse_type(bad)


def test_column_names_must_be_plain_identifiers():
    with pytest.raises(SchemaError):
        make_column("order-id", "INT")


def test_int_range_and_bool_is_not_int():
    assert coerce(parse_type("INT"), 2**31 - 1) == 2**31 - 1
    with pytest.raises(CoerceError) as e:
        coerce(parse_type("INT"), 2**31)
    assert e.value.token == TYPE_INCOMPATIBLE
    with pytest.raises(CoerceError):
        coerce(parse_type("INT"), True)
    with pytest.raises(CoerceError):
        coerce(parse_type("INT"), "72")


def test_decimal_quantizes_and_checks_precision():
    d = parse_type("DECIMAL(5,2)")
    assert coerce(d, "123.456") == Decimal("123.46")
    with pytest.raises(CoerceError):
        coerce(d, "12345.67")  # 7 digits > precision 5
    with pytest.raises(CoerceError):
        coerce(d, 1.5)  # decimal text only


def test_date_and_timestamp_use_epoch_units():
    assert coerce(parse_type("DATE"), 1) == dt.date(1970, 1, 2)
    ts = coerce(parse_type("TIMESTAMP"), 1_000_000)
    assert ts == dt.datetime(1970, 1, 1, 0, 0, 1, tzinfo=dt.timezone.utc)
    assert coerce(parse_type("TIMESTAMP_NTZ"), 0) == dt.datetime(1970, 1, 1)


def test_binary_accepts_base64_and_bytes():
    assert coerce(parse_type("BINARY"), base64.b64encode(b"ab").decode()) == b"ab"
    assert coerce(parse_type("BINARY"), b"ab") == b"ab"
    with pytest.raises(CoerceError):
        coerce(parse_type("BINARY"), "not base64!!")


def test_variant_must_be_a_json_encoded_string():
    assert coerce(parse_type("VARIANT"), '{"a": 1}') == '{"a": 1}'
    with pytest.raises(CoerceError):
        coerce(parse_type("VARIANT"), "{not json")
    with pytest.raises(CoerceError):
        coerce(parse_type("VARIANT"), {"a": 1})


def test_struct_unknown_and_missing_fields():
    st = parse_type("STRUCT<lat: DOUBLE NOT NULL, lon: DOUBLE>")
    assert coerce(st, {"lat": 1, "lon": None}) == {"lat": 1.0, "lon": None}
    with pytest.raises(CoerceError) as e:
        coerce(st, {"lat": 1, "alt": 3})
    assert e.value.token == FIELD_NOT_IN_TABLE
    with pytest.raises(CoerceError) as e:
        coerce(st, {"lon": 2})
    assert e.value.token == MISSING_REQUIRED_COLUMN


def test_array_and_map_coercion():
    assert coerce(parse_type("ARRAY<INT>"), [1, None, 3]) == [1, None, 3]
    assert coerce(parse_type("MAP<STRING,INT>"), {"a": 1}) == {"a": 1}
    assert coerce(parse_type("MAP<INT,STRING>"), {"7": "x"}) == {7: "x"}
    with pytest.raises(CoerceError):
        coerce(parse_type("ARRAY<INT>"), [1, "2"])


def test_json_rendering_of_stored_values():
    row = {"d": dt.date(2020, 1, 2), "m": {"k": 1}, "b": b"\x00\x01", "n": Decimal("1.50")}
    assert to_json_value(row) == {"d": "2020-01-02", "m": {"k": 1}, "b": "AAE=", "n": "1.50"}
