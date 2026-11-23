"""pytest fixtures: an in-process mock per session (zerobus_mock_server), reset per test (zerobus_mock)."""

from __future__ import annotations

import os
import socket
import tempfile
from pathlib import Path

import pytest

from .config import from_dict
from .control import MockControl
from .runner import MockZerobus

def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def zerobus_mock_config() -> dict:
    return {
        "server": {"host": "127.0.0.1", "public_hosts": ["localhost"],
                   "rest_port": _free_port(), "grpc_port": _free_port()},
        "tables": [{"name": "main.default.events", "columns": [
            {"name": "device_name", "type": "STRING", "nullable": False},
            {"name": "temp", "type": "INT"},
        ]}],
    }


@pytest.fixture(scope="session")
def zerobus_mock_server(zerobus_mock_config):
    base = Path(tempfile.mkdtemp(prefix="zb-mock-"))
    server = MockZerobus(from_dict(dict(zerobus_mock_config, tls={"cert_dir": str(base / "tls")}), base_dir=base)).start()
    import certifi  # the SDKs read SSL_CERT_FILE: the system roots plus the mock's CA
    bundle = base / "trust.pem"
    bundle.write_text(Path(certifi.where()).read_text() + "\n" + server.ca_path.read_text())
    os.environ["SSL_CERT_FILE"] = str(bundle)
    yield server
    server.stop()


@pytest.fixture
def zerobus_mock(zerobus_mock_server):
    """A control client. The server is reset (rows, scenarios, tokens) before and after each test."""
    control = MockControl(zerobus_mock_server.rest_base_url, ca_path=str(zerobus_mock_server.ca_path))
    control.reset(schema=True)
    yield control
    control.reset(schema=True)
    control.close()
