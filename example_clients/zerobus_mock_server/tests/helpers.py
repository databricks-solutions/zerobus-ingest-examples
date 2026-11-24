"""Table names, token requests and TLS helpers shared by the test modules."""

import json
import ssl

import httpx

AQ = "main.default.air_quality"
PR = "main.default.proto_readings"
DRIFT = "main.default.drift"
CLIENT_ID, CLIENT_SECRET = "mock-client-id", "mock-client-secret"


def token_details(table: str, privileges=("SELECT", "MODIFY")) -> str:
    return json.dumps([
        {"type": "unity_catalog_privileges", "privileges": ["USE CATALOG"],
         "object_type": "CATALOG", "object_full_path": table.split(".")[0]},
        {"type": "unity_catalog_privileges", "privileges": list(privileges),
         "object_type": "TABLE", "object_full_path": table, "operations": ["zerobuswrite"]},
    ])


def get_token(client: httpx.Client, table: str, **overrides) -> str:
    form = {"grant_type": "client_credentials", "scope": "all-apis",
            "resource": "api://databricks/workspaces/localhost/zerobusDirectWriteApi",
            "authorization_details": token_details(table)}
    form.update(overrides)
    resp = client.post("/oidc/v1/token", data=form, auth=(CLIENT_ID, CLIENT_SECRET))
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


def ca_context(ca_path) -> ssl.SSLContext:
    """A TLS context that trusts only the mock's CA (for httpx)."""
    return ssl.create_default_context(cafile=str(ca_path))
