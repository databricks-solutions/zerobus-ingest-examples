"""Column types: the base kind of each type name, and the basic-type check on values."""

import pytest

from zerobus_mock_server.schema import SchemaError, fits, make_column


@pytest.mark.parametrize("text, kind", [
    ("INT", "INT"), ("long", "LONG"), ("string", "STRING"), ("ARRAY<STRING>", "ARRAY"),
    ("DECIMAL(10,2)", "DECIMAL"), ("STRUCT<lat: DOUBLE NOT NULL, lon: DOUBLE>", "STRUCT"),
    ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
])
def test_type_text_maps_to_its_base_kind(text, kind):
    assert make_column("c", text).kind == kind


def test_unknown_types_and_bad_column_names_are_refused():
    with pytest.raises(SchemaError):
        make_column("c", "WIDGET")
    with pytest.raises(SchemaError):
        make_column("not a name", "INT")


def test_the_check_is_the_basic_type_of_each_value():
    assert fits("INT", 1) and not fits("INT", True) and not fits("INT", "1")
    assert fits("DOUBLE", 1) and fits("DOUBLE", 1.5) and not fits("DOUBLE", "1.5")
    assert fits("STRING", "x") and not fits("STRING", 3)
    assert fits("ARRAY", [1]) and not fits("ARRAY", {"a": 1})
    assert fits("STRUCT", {"lat": 1.0}) and not fits("STRUCT", [1])
    assert fits("LONG", None) and fits("VARIANT", {"any": "thing"})
