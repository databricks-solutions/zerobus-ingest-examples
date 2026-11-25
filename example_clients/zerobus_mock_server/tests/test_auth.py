"""Token rules without a server: the authorization_details shape, table binding, and expiry."""

import json

import pytest

from zerobus_mock_server import auth
from zerobus_mock_server.auth import TokenStore, table_from_details

T1, T2 = "main.default.one", "main.default.two"
ENTRY = {"type": "unity_catalog_privileges", "object_type": "TABLE", "object_full_path": T1,
         "privileges": ["SELECT", "MODIFY"]}


@pytest.mark.parametrize("raw", [
    "not json",
    "[]",
    json.dumps([{"type": "unity_catalog_privileges", "object_type": "CATALOG", "object_full_path": "main",
                 "privileges": ["USE CATALOG"]}]),  # no TABLE entry
    json.dumps([ENTRY, dict(ENTRY, object_full_path=T2)]),  # two tables
    json.dumps([dict(ENTRY, privileges=["SELECT", "CREATE"])]),  # a privilege other than SELECT/MODIFY
    json.dumps([dict(ENTRY, object_full_path="main.default.unknown")]),  # not a configured table
])
def test_authorization_details_must_name_exactly_one_known_table(raw):
    with pytest.raises(ValueError):
        table_from_details(raw, [T1, T2])


def test_token_is_bound_to_its_table_expires_on_request_and_by_the_clock(monkeypatch):
    store = TokenStore()
    tok = store.issue(T1)
    assert store.check(tok, T1) is None
    assert store.check(tok, T2) == "PERMISSION_DENIED"
    store.expire_next(1)
    assert store.check(tok, T1) == "UNAUTHENTICATED"  # the deterministic expiry, once
    assert store.check(tok, T1) is None
    assert store.check(None, T1) == "UNAUTHENTICATED"
    monkeypatch.setattr(auth, "TTL_SECONDS", 0)  # a zero-second lifetime has already run out
    assert store.check(store.issue(T1), T1) == "UNAUTHENTICATED"
