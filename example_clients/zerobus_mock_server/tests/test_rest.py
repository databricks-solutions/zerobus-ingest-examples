"""REST: the token endpoint and the insert endpoint, against the live listener."""

import json

from helpers import AQ, DRIFT, get_token, token_details


def _insert(rest, tok, body, table=AQ, ctype="application/json"):
    return rest.post(f"/zerobus/v1/tables/{table}/insert", content=body,
                     headers={"authorization": f"Bearer {tok}", "content-type": ctype})


def test_token_is_issued_for_the_table_in_authorization_details(rest, mock):
    tok = get_token(rest, AQ)
    assert tok.startswith("mock_")
    bad = rest.post("/oidc/v1/token", data={"grant_type": "client_credentials",
                                            "authorization_details": token_details(AQ, ("SELECT", "USE SCHEMA"))},
                    auth=("mock-client-id", "mock-client-secret"))
    assert bad.status_code == 401 and bad.json()["error"] == "invalid_authorization_details"
    unknown = rest.post("/oidc/v1/token", data={"grant_type": "client_credentials",
                                                "authorization_details": token_details("main.default.nope")},
                        auth=("mock-client-id", "mock-client-secret"))
    assert unknown.status_code == 401 and "not known" in unknown.text


def test_json_array_and_single_object_bodies_are_stored(rest, fresh):
    tok = get_token(rest, AQ)
    assert _insert(rest, tok, json.dumps([{"device_name": "a", "temp": 1}, {"device_name": "b"}])).status_code == 200
    assert _insert(rest, tok, json.dumps({"device_name": "c", "humidity": 3})).status_code == 200
    assert [r["device_name"] for r in fresh.tables.rows(AQ)] == ["a", "b", "c"]
    assert {r.protocol for r in fresh.tables.read(AQ)} == {"rest"}


def test_non_json_content_type_is_415_and_unknown_table_is_404(rest, fresh):
    tok = get_token(rest, AQ)
    assert _insert(rest, tok, "device_name,temp", ctype="text/csv").status_code == 415
    resp = rest.post("/zerobus/v1/tables/main.default.nope/insert", content="[]",
                     headers={"authorization": f"Bearer {tok}", "content-type": "application/json"})
    assert resp.status_code == 404 and "cannot be found" in resp.json()["message"]


def test_missing_token_is_401_and_another_tables_token_is_403(rest, fresh):
    body = json.dumps([{"device_name": "a"}])
    assert rest.post(f"/zerobus/v1/tables/{AQ}/insert", content=body,
                     headers={"content-type": "application/json"}).status_code == 401
    other = get_token(rest, DRIFT)
    resp = _insert(rest, other, body)
    assert resp.status_code == 403 and resp.json()["error_code"] == "PERMISSION_DENIED"


def test_one_bad_record_rejects_the_whole_request(rest, fresh):
    tok = get_token(rest, AQ)
    resp = _insert(rest, tok, json.dumps([{"device_name": "ok"}, {"device_name": "bad", "nope": 1}]))
    assert resp.status_code == 400 and "is not a column" in resp.json()["message"]
    assert fresh.tables.rows(AQ) == []


def test_one_hundred_records_over_rest_are_stored(rest, fresh):
    tok = get_token(rest, AQ)
    for i in range(100):
        assert _insert(rest, tok, json.dumps([{"device_name": f"rest-{i}", "temp": i, "humidity": 1}])).status_code == 200
    assert len(fresh.tables.rows(AQ)) == 100
