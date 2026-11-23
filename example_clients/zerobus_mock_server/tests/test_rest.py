"""REST listener against a live mock: token endpoint, insert endpoint, faults, control API."""

import json
import time

import pytest

from helpers import TABLES, TOKEN_RESOURCE, get_token, token_details

AQ = TABLES["air_quality"]
SE = TABLES["sensor_events"]
JSON_CT = {"content-type": "application/json"}


def insert(rest, table, body, token=None, ct="application/json"):
    headers = {"content-type": ct}
    if token:
        headers["authorization"] = f"Bearer {token}"
    payload = body if isinstance(body, (bytes, str)) else json.dumps(body)
    return rest.post(f"/zerobus/v1/tables/{table}/insert", content=payload, headers=headers)


# ---------------------------------------------------------------- token endpoint

def test_token_happy_path_returns_one_hour_bearer_token(rest, fresh):
    resp = rest.post("/oidc/v1/token", data={
        "grant_type": "client_credentials", "scope": "all-apis", "resource": TOKEN_RESOURCE,
        "authorization_details": token_details(AQ)}, auth=("mock-client-id", "mock-client-secret"))
    assert resp.status_code == 200
    body = resp.json()
    assert body["token_type"] == "Bearer" and body["expires_in"] == 3600
    assert body["access_token"].startswith("mock_")


@pytest.mark.parametrize("overrides, expected_error", [
    ({"grant_type": "password"}, "unsupported_grant_type"),
    ({"scope": "openid"}, "invalid_scope"),
    ({"resource": "api://databricks/zerobusDirectWriteApi"}, "invalid_target"),
])
def test_token_request_shape_is_checked(rest, fresh, overrides, expected_error):
    form = {"grant_type": "client_credentials", "scope": "all-apis", "resource": TOKEN_RESOURCE,
            "authorization_details": token_details(AQ)}
    form.update(overrides)
    resp = rest.post("/oidc/v1/token", data=form, auth=("cid", "sec"))
    assert resp.status_code == 400
    assert resp.json()["error"] == expected_error


def test_token_without_client_credentials_is_invalid_client(rest, fresh):
    resp = rest.post("/oidc/v1/token", data={
        "grant_type": "client_credentials", "scope": "all-apis", "resource": TOKEN_RESOURCE,
        "authorization_details": token_details(AQ)})
    assert resp.status_code == 401 and resp.json()["error"] == "invalid_client"


def test_token_with_privilege_other_than_select_modify_is_refused(rest, fresh):
    resp = rest.post("/oidc/v1/token", data={
        "grant_type": "client_credentials", "scope": "all-apis", "resource": TOKEN_RESOURCE,
        "authorization_details": token_details(AQ, privileges=("ALL PRIVILEGES",))}, auth=("cid", "sec"))
    assert resp.status_code == 401
    assert resp.json()["error"] == "invalid_authorization_details"


def test_token_for_unknown_table_is_refused(rest, fresh):
    resp = rest.post("/oidc/v1/token", data={
        "grant_type": "client_credentials", "scope": "all-apis", "resource": TOKEN_RESOURCE,
        "authorization_details": token_details("main.default.missing")}, auth=("cid", "sec"))
    assert resp.status_code == 401 and resp.json()["error"] == "invalid_authorization_details"


# ---------------------------------------------------------------- insert endpoint

def test_success_returns_empty_json_200_and_stores_rows(rest, fresh):
    tok = get_token(rest, AQ)
    resp = insert(rest, AQ, [{"device_name": "device_num_1", "temp": 28, "humidity": 60},
                             {"device_name": "device_num_1", "temp": 28, "humidity": 60}], tok)
    assert resp.status_code == 200 and resp.json() == {}
    assert fresh.tables.stats(AQ)["rows"] == 2


def test_jsonl_body_is_accepted(rest, fresh):
    tok = get_token(rest, AQ)
    body = "\n".join(json.dumps({"device_name": f"d{i}", "temp": i}) for i in range(3))
    resp = insert(rest, AQ, body, tok, ct="application/jsonl")
    assert resp.status_code == 200
    assert fresh.tables.stats(AQ)["rows"] == 3


def test_unsupported_content_type_is_415(rest, fresh):
    tok = get_token(rest, AQ)
    resp = insert(rest, AQ, "device_name=x", tok, ct="text/plain")
    assert resp.status_code == 415 and resp.json()["error_code"] == "INVALID_PARAMETER_VALUE"


def test_unknown_table_is_404_with_docs_message(rest, fresh):
    resp = insert(rest, "main.default.missing", [{"a": 1}], "x")
    assert resp.status_code == 404
    assert resp.json() == {"error_code": "NOT_FOUND", "message": 'Table "main.default.missing" cannot be found.'}


def test_missing_or_unknown_token_is_401(rest, fresh):
    assert insert(rest, AQ, [{"device_name": "a"}]).status_code == 401
    assert insert(rest, AQ, [{"device_name": "a"}], "mock_nope").status_code == 401


def test_token_issued_for_another_table_is_403(rest, fresh):
    tok = get_token(rest, SE)
    resp = insert(rest, AQ, [{"device_name": "a"}], tok)
    assert resp.status_code == 403 and resp.json()["error_code"] == "PERMISSION_DENIED"


def test_one_bad_record_rejects_the_whole_request(rest, fresh):
    tok = get_token(rest, AQ)
    resp = insert(rest, AQ, [{"device_name": "ok"}, {"device_name": "bad", "temp": "72F"}], tok)
    assert resp.status_code == 400
    body = resp.json()
    assert body["error_code"] == "INVALID_PARAMETER_VALUE"
    assert "record 1" in body["message"] and "TYPE_INCOMPATIBLE" in body["message"]
    assert fresh.tables.stats(AQ)["rows"] == 0  # atomic: nothing stored
    assert fresh.tables.stats(AQ)["natural_rejections"] == {"TYPE_INCOMPATIBLE": 1}


def test_unknown_field_is_rejected_on_a_plain_table(rest, fresh):
    tok = get_token(rest, AQ)
    resp = insert(rest, AQ, [{"device_name": "a", "region": "us"}], tok)
    assert resp.status_code == 400 and "FIELD_NOT_IN_TABLE" in resp.json()["message"]


def test_rescue_table_accepts_unknown_fields(rest, fresh):
    tok = get_token(rest, SE)
    resp = insert(rest, SE, [{"device_name": "sensor-1", "temp": "72F", "extra_field": "some value"}], tok)
    assert resp.status_code == 200
    row = fresh.tables.values(SE)[0]
    assert row["temp"] is None
    assert json.loads(row["rescue"]) == {"temp": "72F", "extra_field": "some value"}


def test_body_that_is_not_records_is_400(rest, fresh):
    tok = get_token(rest, AQ)
    assert insert(rest, AQ, "[1, 2]", tok).status_code == 400
    assert insert(rest, AQ, "not json at all", tok).status_code == 400


def test_record_over_the_configured_limit_is_out_of_range(rest, fresh):
    tok = get_token(rest, AQ)
    big = {"device_name": "x" * 70_000}  # the shared mock's limit is 64 KiB
    resp = insert(rest, AQ, [big], tok)
    assert resp.status_code == 400 and resp.json()["error_code"] == "OUT_OF_RANGE"
    assert fresh.tables.stats(AQ)["rows"] == 0


# ---------------------------------------------------------------- injected faults (REST)

def add(mock, **spec):
    return mock.add_fault(spec)


@pytest.mark.parametrize("status, code, retry_after", [
    (429, "RESOURCE_EXHAUSTED", "2"), (500, "INTERNAL_ERROR", None), (503, "UNAVAILABLE", None),
])
def test_http_error_fault_answers_with_status_and_docs_error_code(rest, fresh, status, code, retry_after):
    tok = get_token(rest, AQ)
    params = {"status": status}
    if retry_after:
        params["retry_after_seconds"] = int(retry_after)
    add(fresh, type="http_error", protocol="rest", table=AQ, params=params)
    resp = insert(rest, AQ, [{"device_name": "a"}], tok)
    assert resp.status_code == status and resp.json()["error_code"] == code
    if retry_after:
        assert resp.headers.get("retry-after") == retry_after
    assert fresh.tables.stats(AQ)["rows"] == 0
    events = fresh.faults.events()
    assert len(events) == 1 and events[0]["fault_type"] == "http_error"


def test_http_error_once_then_success(rest, fresh):
    tok = get_token(rest, AQ)
    add(fresh, type="http_error", protocol="rest", table=AQ, params={"status": 503}, trigger={"mode": "once"})
    assert insert(rest, AQ, [{"device_name": "a"}], tok).status_code == 503
    assert insert(rest, AQ, [{"device_name": "a"}], tok).status_code == 200


def test_token_fault_invalid_client(rest, fresh):
    add(fresh, type="token_invalid_client", table=AQ)
    resp = rest.post("/oidc/v1/token", data={"grant_type": "client_credentials", "scope": "all-apis",
                     "resource": TOKEN_RESOURCE, "authorization_details": token_details(AQ)}, auth=("c", "s"))
    assert resp.status_code == 401 and resp.json()["error"] == "invalid_client"


def test_token_fault_invalid_authorization_details(rest, fresh):
    add(fresh, type="token_invalid_authorization_details", table=AQ)
    resp = rest.post("/oidc/v1/token", data={"grant_type": "client_credentials", "scope": "all-apis",
                     "resource": TOKEN_RESOURCE, "authorization_details": token_details(AQ)}, auth=("c", "s"))
    assert resp.status_code == 401 and resp.json()["error"] == "invalid_authorization_details"


@pytest.mark.parametrize("ftype, status, code", [
    ("unauthorized", 401, "UNAUTHENTICATED"),
    ("permission_denied", 403, "PERMISSION_DENIED"),
    ("table_not_found", 404, "NOT_FOUND"),
])
def test_auth_and_table_faults_on_insert(rest, fresh, ftype, status, code):
    tok = get_token(rest, AQ)
    add(fresh, type=ftype, protocol="rest", table=AQ)
    resp = insert(rest, AQ, [{"device_name": "a"}], tok)
    assert resp.status_code == status and resp.json()["error_code"] == code


def test_schema_mismatch_fault_is_injected_on_the_record(rest, fresh):
    tok = get_token(rest, AQ)
    add(fresh, type="schema_mismatch", protocol="rest", table=AQ, trigger={"mode": "every_nth", "n": 2})
    assert insert(rest, AQ, [{"device_name": "a"}], tok).status_code == 200
    resp = insert(rest, AQ, [{"device_name": "b"}], tok)
    assert resp.status_code == 400
    assert "FIELD_NOT_IN_TABLE" in resp.json()["message"] and "4044" in resp.json()["message"]
    assert fresh.tables.stats(AQ)["rows"] == 1


def test_record_too_large_fault(rest, fresh):
    tok = get_token(rest, AQ)
    add(fresh, type="record_too_large", protocol="rest", table=AQ, trigger={"mode": "once"})
    resp = insert(rest, AQ, [{"device_name": "a"}], tok)
    assert resp.status_code == 400 and resp.json()["error_code"] == "OUT_OF_RANGE"


def test_slow_response_fault_delays_the_answer(rest, fresh):
    tok = get_token(rest, AQ)
    add(fresh, type="slow_response", protocol="rest", table=AQ, params={"delay_ms": 300})
    started = time.monotonic()
    assert insert(rest, AQ, [{"device_name": "a"}], tok).status_code == 200
    assert time.monotonic() - started >= 0.3


def test_schema_change_fault_adds_a_column_for_the_next_request(rest, fresh):
    tok = get_token(rest, AQ)
    add(fresh, type="schema_change", protocol="rest", table=AQ, trigger={"mode": "once"},
        params={"add_columns": [{"name": "note", "type": "STRING", "nullable": True}]})
    assert insert(rest, AQ, [{"device_name": "a", "note": "hi"}], tok).status_code == 200
    assert fresh.tables.describe(AQ)["schema_version"] == 2


# ---------------------------------------------------------------- control API

def test_control_api_lists_adds_and_removes_faults(rest, fresh):
    created = rest.post("/_mock/faults", json={"type": "unauthorized", "table": AQ, "trigger": {"mode": "once"}})
    assert created.status_code == 201
    fid = created.json()["id"]
    listed = rest.get("/_mock/faults").json()
    assert any(f["id"] == fid for f in listed["faults"])
    assert rest.delete(f"/_mock/faults/{fid}").status_code == 200
    assert rest.delete(f"/_mock/faults/{fid}").status_code == 404


def test_control_api_rejects_a_bad_fault_spec(rest, fresh):
    resp = rest.post("/_mock/faults", json={"type": "not-a-fault"})
    assert resp.status_code == 400 and resp.json()["error_code"] == "INVALID_PARAMETER_VALUE"


def test_control_api_reads_rows_and_stats(rest, fresh):
    tok = get_token(rest, AQ)
    insert(rest, AQ, [{"device_name": "a", "temp": 1}, {"device_name": "a", "temp": 2}, {"device_name": "b"}], tok)
    rows = rest.get(f"/_mock/tables/{AQ}/rows").json()
    assert rows["count"] == 3 and rows["rows"][0]["values"]["device_name"] == "a"
    assert rows["rows"][0]["protocol"] == "rest"
    stats = rest.get(f"/_mock/tables/{AQ}/stats", params={"key": "device_name"}).json()
    assert stats["rows"] == 3 and stats["duplicate_rows"] == 1 and stats["duplicate_keys"] == ["a"]
    assert rest.get("/_mock/tables/main.default.nope/rows").status_code == 404


def test_control_api_events_show_each_injected_fault(rest, fresh):
    tok = get_token(rest, AQ)
    add(fresh, type="unauthorized", protocol="rest", table=AQ, trigger={"mode": "once"})
    insert(rest, AQ, [{"device_name": "a"}], tok)
    insert(rest, AQ, [{"device_name": "a"}], tok)
    events = rest.get("/_mock/events").json()["events"]
    assert len(events) == 1
    assert events[0]["fault_type"] == "unauthorized" and events[0]["action"] == "401 UNAUTHENTICATED"
    stats = rest.get(f"/_mock/tables/{AQ}/stats").json()
    assert sum(stats["injected_faults"].values()) == 1


def test_reset_clears_rows_faults_and_tokens(rest, fresh):
    tok = get_token(rest, AQ)
    insert(rest, AQ, [{"device_name": "a"}], tok)
    add(fresh, type="unauthorized", table=AQ)
    rest.post("/_mock/reset", json={"tables": [AQ]})
    assert fresh.tables.stats(AQ)["rows"] == 0
    assert rest.get("/_mock/faults").json()["faults"] == []
    assert insert(rest, AQ, [{"device_name": "a"}], tok).status_code == 401  # token was cleared


def test_health_and_ca_endpoints(rest, fresh):
    assert rest.get("/_mock/health").json()["status"] == "ok"
    assert rest.get("/_mock/ca.pem").text.startswith("-----BEGIN CERTIFICATE-----")
