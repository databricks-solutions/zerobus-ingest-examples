"""The documented record limit (10 MB, 19 bytes of headers), on a mock with default limits."""

import json

import httpx
import pytest

from helpers import TABLES, ca_context, get_token
from conftest import free_port
from zerobus_mock_server import MockZerobus, from_dict

AQ = TABLES["air_quality"]


@pytest.fixture(scope="module")
def default_limits_mock(tmp_path_factory):
    base = tmp_path_factory.mktemp("limits")
    cfg = {
        "server": {"host": "127.0.0.1", "public_hosts": ["localhost"],
                   "rest_port": free_port(), "grpc_port": free_port()},
        "tls": {"cert_dir": str(base / "tls")},
        "tables": [{"name": AQ, "columns": [{"name": "device_name", "type": "STRING", "nullable": False}]}],
    }
    m = MockZerobus(from_dict(cfg, base_dir=base)).start()
    yield m
    m.stop()


def _record_of_json_size(size: int) -> dict:
    # {"device_name":"..."} is 18 bytes of JSON around the value
    return {"device_name": "x" * (size - len(json.dumps({"device_name": ""}, separators=(",", ":"))))}


def test_default_payload_limit_is_ten_megabytes_minus_headers(default_limits_mock):
    assert default_limits_mock.cfg.limits.max_payload_bytes == 10_485_760 - 19


def test_record_at_the_limit_is_accepted_and_one_byte_over_is_out_of_range(default_limits_mock):
    m = default_limits_mock
    limit = m.cfg.limits.max_payload_bytes
    with httpx.Client(base_url=m.rest_base_url, verify=ca_context(m.ca_path), timeout=60) as client:
        tok = get_token(client, AQ)
        headers = {"authorization": f"Bearer {tok}", "content-type": "application/json"}
        ok = client.post(f"/zerobus/v1/tables/{AQ}/insert", content=json.dumps([_record_of_json_size(limit)]), headers=headers)
        assert ok.status_code == 200, ok.text[:200]
        too_big = client.post(f"/zerobus/v1/tables/{AQ}/insert", content=json.dumps([_record_of_json_size(limit + 1)]), headers=headers)
        assert too_big.status_code == 400 and too_big.json()["error_code"] == "OUT_OF_RANGE"
    assert m.tables.stats(AQ)["rows"] == 1
