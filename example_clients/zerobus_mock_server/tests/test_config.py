"""Configuration: defaults, and the checks that refuse a table definition."""

import pytest

from zerobus_mock_server.config import ConfigError, from_dict

GOOD_TABLE = {"name": "main.default.t", "columns": [{"name": "a", "type": "STRING"}]}


def test_defaults_match_the_documented_record_limit():
    cfg = from_dict({"tables": [GOOD_TABLE]})
    assert cfg.max_record_bytes == 10_485_760
    assert cfg.max_payload_bytes == 10_485_760 - 19
    assert cfg.host == "127.0.0.1" and cfg.public_hosts == ["localhost"]


@pytest.mark.parametrize("raw", [
    {},
    {"tables": [dict(GOOD_TABLE, name="not_three_parts")]},
    {"tables": [dict(GOOD_TABLE, columns=[{"name": "a", "type": "WIDGET"}])]},
    {"tables": [dict(GOOD_TABLE, columns=[{"name": "a", "type": "INT"}, {"name": "a", "type": "INT"}])]},
    {"tables": [GOOD_TABLE, GOOD_TABLE]},
    {"tables": [GOOD_TABLE], "limits": {"max_record_bytes": 10}},
])
def test_bad_tables_and_limits_are_refused(raw):
    with pytest.raises(ConfigError):
        from_dict(raw)
