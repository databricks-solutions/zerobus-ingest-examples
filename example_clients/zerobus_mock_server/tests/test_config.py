"""Configuration validation and the documented limits."""

import pytest

from zerobus_mock_server.config import ConfigError, from_dict

GOOD_TABLE = {"name": "main.default.t", "columns": [{"name": "a", "type": "STRING", "nullable": False}]}


def cfg(**extra):
    base = {"tables": [GOOD_TABLE]}
    base.update(extra)
    return from_dict(base)


def test_defaults_match_documented_limits():
    c = cfg()
    assert c.limits.max_record_bytes == 10_485_760
    assert c.limits.message_header_bytes == 19
    assert c.limits.max_payload_bytes == 10_485_741
    assert c.auth.token_ttl_seconds == 3600


def test_table_name_must_be_three_part():
    with pytest.raises(ConfigError):
        from_dict({"tables": [{"name": "t", "columns": [{"name": "a", "type": "INT"}]}]})


def test_rescue_column_must_be_nullable_variant():
    bad = {"name": "main.default.t", "rescue_column": "r", "columns": [
        {"name": "a", "type": "STRING"}, {"name": "r", "type": "VARIANT", "nullable": False}]}
    with pytest.raises(ConfigError):
        from_dict({"tables": [bad]})
    wrong_type = {"name": "main.default.t", "rescue_column": "r", "columns": [
        {"name": "a", "type": "STRING"}, {"name": "r", "type": "INT"}]}
    with pytest.raises(ConfigError):
        from_dict({"tables": [wrong_type]})


def test_names_that_collide_after_underscore_folding_are_rejected():
    clash = {"name": "main.default.t", "columns": [
        {"name": "order_id", "type": "LONG"}, {"name": "orderId", "type": "LONG"}]}
    with pytest.raises(ConfigError) as e:
        from_dict({"tables": [clash]})
    assert "4044" in str(e.value)


def test_known_clients_required_when_enabled():
    with pytest.raises(ConfigError):
        from_dict({"tables": [GOOD_TABLE], "auth": {"require_known_clients": True}})


def test_limit_must_exceed_header():
    with pytest.raises(ConfigError):
        from_dict({"tables": [GOOD_TABLE], "limits": {"max_record_bytes": 10, "message_header_bytes": 19}})
