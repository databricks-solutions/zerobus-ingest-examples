"""Shared fixtures. One in-process mock per session (the plugin's server, on free ports).

``zerobus_mock`` (the plugin's fixture) resets the mock before each test. ``mock`` is the server
object for in-process checks, ``rest`` an httpx client that trusts the mock's CA. The Python SDK
reads the same CA through SSL_CERT_FILE, which the plugin writes.
"""

from __future__ import annotations

import socket

import httpx
import pytest

from helpers import AQ, DRIFT, PR, ca_context
from zerobus_mock_server.control import MockControl  # noqa: F401  (documented fixture type)
from zerobus_mock_server.pytest_plugin import zerobus_mock, zerobus_mock_server  # noqa: F401

CONFIG_TABLES = [
    {"name": AQ, "columns": [
        {"name": "device_name", "type": "STRING", "nullable": False},
        {"name": "temp", "type": "INT"},
        {"name": "humidity", "type": "LONG"},
    ]},
    {"name": PR, "columns": [
        {"name": "device_name", "type": "STRING", "nullable": False},
        {"name": "temp", "type": "INT"},
        {"name": "humidity", "type": "LONG"},
        {"name": "tags", "type": "ARRAY<STRING>"},
        {"name": "location", "type": "STRUCT<lat: DOUBLE NOT NULL, lon: DOUBLE>"},
    ]},
    {"name": DRIFT, "columns": [
        {"name": "device_name", "type": "STRING", "nullable": False},
        {"name": "temp", "type": "INT"},
    ]},
]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def zerobus_mock_config() -> dict:
    # A 64 KB record limit keeps the oversize tests quick. test_limits.py covers the same rule.
    return {
        "server": {"host": "127.0.0.1", "public_hosts": ["localhost"],
                   "rest_port": _free_port(), "grpc_port": _free_port()},
        "limits": {"max_record_bytes": 65536},
        "tables": CONFIG_TABLES,
    }


@pytest.fixture
def mock(zerobus_mock_server, zerobus_mock):
    """The server object. Requesting ``zerobus_mock`` resets it for this test."""
    return zerobus_mock_server


@pytest.fixture
def fresh(mock):
    return mock


@pytest.fixture
def rest(mock):
    with httpx.Client(base_url=mock.rest_base_url, verify=ca_context(mock.ca_path), timeout=30) as client:
        yield client


@pytest.fixture
def channel(mock):
    import grpc
    creds = grpc.ssl_channel_credentials(root_certificates=mock.ca_path.read_bytes())
    ch = grpc.secure_channel(f"localhost:{mock.cfg.grpc_port}", creds)
    yield ch
    ch.close()
