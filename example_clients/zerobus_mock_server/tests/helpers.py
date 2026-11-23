"""Helpers shared by the test modules (names, token requests)."""

import json
import ssl
from pathlib import Path

import httpx

TABLES = {
    "air_quality": "main.default.air_quality",
    "sensor_events": "main.default.sensor_events",
    "proto_readings": "main.default.proto_readings",
    "evolving": "main.default.evolving",
    "strict": "main.default.strict",
    "drift": "main.default.drift",
}

TOKEN_RESOURCE = "api://databricks/workspaces/localhost/zerobusDirectWriteApi"


def token_details(table: str, privileges=("SELECT", "MODIFY")) -> str:
    return json.dumps([
        {"type": "unity_catalog_privileges", "privileges": ["USE CATALOG"],
         "object_type": "CATALOG", "object_full_path": table.split(".")[0]},
        {"type": "unity_catalog_privileges", "privileges": ["USE SCHEMA"],
         "object_type": "SCHEMA", "object_full_path": ".".join(table.split(".")[:2])},
        {"type": "unity_catalog_privileges", "privileges": list(privileges),
         "object_type": "TABLE", "object_full_path": table, "operations": ["zerobuswrite"]},
    ])


def get_token(client: httpx.Client, table: str, **overrides) -> str:
    form = {
        "grant_type": "client_credentials",
        "scope": "all-apis",
        "resource": TOKEN_RESOURCE,
        "authorization_details": token_details(table),
    }
    form.update(overrides)
    resp = client.post("/oidc/v1/token", data=form, auth=("mock-client-id", "mock-client-secret"))
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


def ca_context(ca_path) -> ssl.SSLContext:
    """TLS context that trusts only the mock's CA (for httpx)."""
    return ssl.create_default_context(cafile=str(Path(ca_path)))


def ca_bytes(ca_path) -> bytes:
    """The mock's CA certificate as bytes (for gRPC and Flight TLS credentials)."""
    return Path(ca_path).read_bytes()
