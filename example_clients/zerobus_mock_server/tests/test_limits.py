"""The record size limit, over REST. The test config uses a 64 KB limit (conftest.py)."""

import json

from helpers import AQ, get_token


def test_a_record_over_the_limit_is_out_of_range_and_nothing_is_stored(rest, fresh):
    tok = get_token(rest, AQ)
    big = {"device_name": "big", "temp": 1, "humidity": 1, "pad": "x" * 70_000}
    resp = rest.post(f"/zerobus/v1/tables/{AQ}/insert", content=json.dumps([big]),
                     headers={"authorization": f"Bearer {tok}", "content-type": "application/json"})
    assert resp.status_code == 400 and resp.json()["error_code"] == "OUT_OF_RANGE"
    assert "maximum message size" in resp.json()["message"]
    assert fresh.tables.rows(AQ) == []
