"""Token request rules: authorization_details, privileges and the token scope (unit tests)."""

import base64
import json

import pytest

from zerobus_mock_server.auth import (
    TokenProblem,
    TokenStore,
    parse_authorization_details,
    parse_basic,
)

TABLES = ["main.default.air_quality"]


def details(*entries):
    return json.dumps(list(entries))


def table_entry(path="main.default.air_quality", privileges=("SELECT", "MODIFY")):
    return {"type": "unity_catalog_privileges", "privileges": list(privileges),
            "object_type": "TABLE", "object_full_path": path, "operations": ["zerobuswrite"]}


def test_one_table_with_select_and_modify_is_accepted():
    assert parse_authorization_details(details(table_entry()), TABLES) == "main.default.air_quality"


def test_catalog_and_schema_entries_are_allowed_alongside_the_table():
    raw = details(
        {"type": "unity_catalog_privileges", "privileges": ["USE CATALOG"], "object_type": "CATALOG", "object_full_path": "main"},
        table_entry(),
    )
    assert parse_authorization_details(raw, TABLES) == "main.default.air_quality"


def test_other_table_privileges_are_refused():
    with pytest.raises(TokenProblem) as e:
        parse_authorization_details(details(table_entry(privileges=("ALL PRIVILEGES",))), TABLES)
    assert e.value.status == 401 and e.value.error == "invalid_authorization_details"


def test_unknown_table_is_refused():
    with pytest.raises(TokenProblem) as e:
        parse_authorization_details(details(table_entry("main.default.nope")), TABLES)
    assert "not known" in e.value.description


def test_two_tables_are_refused():
    with pytest.raises(TokenProblem):
        parse_authorization_details(details(table_entry(), table_entry()), TABLES)


@pytest.mark.parametrize("raw", ["", "not json", "{}", "[]", "[1]"])
def test_malformed_details_are_refused(raw):
    with pytest.raises(TokenProblem):
        parse_authorization_details(raw, TABLES)


def test_basic_auth_header():
    header = "Basic " + base64.b64encode(b"cid:sec").decode()
    assert parse_basic(header) == ("cid", "sec")
    assert parse_basic("Bearer x") is None


def test_token_is_bound_to_one_table_and_expires():
    clock = [1000.0]
    store = TokenStore(ttl_seconds=60, now=lambda: clock[0])
    token, ttl = store.issue("cid", "main.default.air_quality", "ws")
    assert ttl == 60
    assert store.check(token, "main.default.air_quality") is None
    assert store.check(token, "main.default.other") == "PERMISSION_DENIED"
    assert store.check(None, "main.default.air_quality") == "UNAUTHENTICATED"
    assert store.check("nope", "main.default.air_quality") == "UNAUTHENTICATED"
    clock[0] += 61
    assert store.check(token, "main.default.air_quality") == "UNAUTHENTICATED"
