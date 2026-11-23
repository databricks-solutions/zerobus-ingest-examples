"""Shared fixtures.

The suite runs the mock in-process on free local ports. ``mock`` is session
scoped; ``fresh`` resets its tables, faults and events before each test that
uses it. The real Python SDK trusts the mock's CA through ``SSL_CERT_FILE``,
a bundle of the system roots plus the mock CA, which the fixture writes.
"""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path

import certifi
import httpx
import pytest

from zerobus_mock_server import MockZerobus, from_dict
from helpers import TABLES, ca_context

CONFIG = {
    "server": {"host": "127.0.0.1", "public_hosts": ["localhost"]},
    # Small limit for the shared mock so the oversize tests stay quick. A second
    # mock with the documented 10 MB limit is used in test_limits.py.
    "limits": {"max_record_bytes": 65536, "message_header_bytes": 19},
    "auth": {"token_ttl_seconds": 3600},
    "fault_seed": 7,
    "tables": [
        {"name": TABLES["air_quality"], "columns": [
            {"name": "device_name", "type": "STRING", "nullable": False},
            {"name": "temp", "type": "INT"},
            {"name": "humidity", "type": "LONG"},
        ]},
        {"name": TABLES["sensor_events"], "rescue_column": "rescue", "columns": [
            {"name": "device_name", "type": "STRING", "nullable": False},
            {"name": "temp", "type": "INT"},
            {"name": "rescue", "type": "VARIANT"},
        ]},
        {"name": TABLES["proto_readings"], "columns": [
            {"name": "device_name", "type": "STRING", "nullable": False},
            {"name": "temp", "type": "INT"},
            {"name": "humidity", "type": "LONG"},
            {"name": "tags", "type": "ARRAY<STRING>"},
            {"name": "location", "type": "STRUCT<lat: DOUBLE NOT NULL, lon: DOUBLE>"},
        ]},
        {"name": TABLES["evolving"], "columns": [
            {"name": "device_name", "type": "STRING", "nullable": False},
            {"name": "temp", "type": "INT"},
        ]},
        {"name": TABLES["drift"], "columns": [
            {"name": "device_name", "type": "STRING", "nullable": False},
            {"name": "temp", "type": "INT"},
        ]},
        {"name": TABLES["strict"], "columns": [
            {"name": "id", "type": "LONG", "nullable": False},
            {"name": "amount", "type": "DECIMAL(10,2)"},
            {"name": "created", "type": "DATE"},
            {"name": "updated", "type": "TIMESTAMP"},
            {"name": "tags", "type": "MAP<STRING,INT>"},
            {"name": "blob", "type": "BINARY"},
            {"name": "flag", "type": "BOOLEAN"},
        ]},
    ],
    "faults": [],
}


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _ports(config: dict) -> dict:
    cfg = json.loads(json.dumps(config))
    cfg["server"]["rest_port"] = free_port()
    cfg["server"]["grpc_port"] = free_port()
    return cfg


def _write_trust_bundle(path: Path, ca_path: Path) -> None:
    path.write_text(Path(certifi.where()).read_text() + "\n" + ca_path.read_text())


@pytest.fixture(scope="session")
def mock(tmp_path_factory):
    base = tmp_path_factory.mktemp("mock")
    cfg = _ports(CONFIG)
    cfg["event_log"] = str(base / "events.jsonl")
    cfg["tls"] = {"cert_dir": str(base / "tls")}
    m = MockZerobus(from_dict(cfg, base_dir=base)).start()
    bundle = base / "trust.pem"
    _write_trust_bundle(bundle, m.ca_path)
    os.environ["SSL_CERT_FILE"] = str(bundle)
    yield m
    m.stop()


@pytest.fixture
def fresh(mock):
    mock.reset(schema=True)
    yield mock
    mock.reset(schema=True)


@pytest.fixture
def rest(mock):
    """An httpx client that trusts the mock's CA, against the REST listener."""
    with httpx.Client(base_url=mock.rest_base_url, verify=ca_context(mock.ca_path), timeout=30) as client:
        yield client
