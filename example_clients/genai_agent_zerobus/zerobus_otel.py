"""
zerobus_otel — drop-in, in-process OpenTelemetry → Databricks ZeroBus exporter.

Import this into any Python agent/service to ship its OpenTelemetry spans/logs/metrics
straight into Unity Catalog Delta tables over OTLP/gRPC — no collector, no sidecar.

What it does (the producer → ZeroBus auth contract):
  1. Mints a ZeroBus-scoped OAuth token via client-credentials, with:
       - resource audience = api://databricks/workspaces/<id>/zerobusDirectWriteApi
       - authorization_details = per-TABLE Unity Catalog privileges (RFC 9396)
     Token minting + refresh is delegated to the Databricks SDK's ClientCredentials
     (databricks.sdk.oauth), so the ~1h access token is cached and auto-refreshed for you.
  2. Attaches `authorization: Bearer <token>` per gRPC call + the
     `x-databricks-zerobus-table-name` header, per signal (each signal → its own table).

Prerequisites (see README):
  - The destination OTel tables must exist (create them with MLflow, or with the DDL in the README).
  - The service principal must hold USE CATALOG / USE SCHEMA (traversal) + SELECT / MODIFY per table.

Usage:
    from zerobus_otel import ZerobusConfig, build_providers
    tp, lp, mp = build_providers(ZerobusConfig.from_env(), resource)
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass

import grpc
from databricks.sdk.oauth import ClientCredentials
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter


@dataclass
class ZerobusConfig:
    workspace_url: str        # host only, e.g. dbc-xxxx.cloud.databricks.com
    workspace_id: str
    region: str
    client_id: str
    client_secret: str
    catalog: str
    schema: str
    table_prefix: str

    @classmethod
    def from_env(cls) -> "ZerobusConfig":
        env = os.environ
        return cls(
            workspace_url=env["WORKSPACE_URL"],
            workspace_id=env["WORKSPACE_ID"],
            region=env["REGION"],
            client_id=env["DATABRICKS_CLIENT_ID"],
            client_secret=env["DATABRICKS_CLIENT_SECRET"],
            catalog=env["CATALOG"],
            schema=env["SCHEMA"],
            table_prefix=env["TABLE_PREFIX"],
        )

    @property
    def endpoint(self) -> str:
        return f"{self.workspace_id}.zerobus.{self.region}.cloud.databricks.com:443"

    @property
    def token_url(self) -> str:
        return f"https://{self.workspace_url}/oidc/v1/token"

    @property
    def resource_audience(self) -> str:
        return f"api://databricks/workspaces/{self.workspace_id}/zerobusDirectWriteApi"

    def table(self, signal: str) -> str:
        return f"{self.catalog}.{self.schema}.{self.table_prefix}_otel_{signal}"


def _table_authorization_details(cfg: ZerobusConfig, signal: str) -> str:
    """Per-table UC-privileges grant (RFC 9396). ZeroBus requires table-level, not schema-level."""
    return json.dumps([
        {"type": "unity_catalog_privileges", "privileges": ["USE CATALOG"],
         "object_type": "CATALOG", "object_full_path": cfg.catalog},
        {"type": "unity_catalog_privileges", "privileges": ["USE SCHEMA"],
         "object_type": "SCHEMA", "object_full_path": f"{cfg.catalog}.{cfg.schema}"},
        {"type": "unity_catalog_privileges", "privileges": ["SELECT", "MODIFY"],
         "object_type": "TABLE", "object_full_path": cfg.table(signal)},
    ])


class _TokenSource:
    """ZeroBus-scoped OAuth token for one signal/table — minted + auto-refreshed by the Databricks SDK.

    Wraps the SDK's client-credentials TokenSource (databricks.sdk.oauth.ClientCredentials). The SDK sends
    exactly the request the ZeroBus contract requires — resource audience (via endpoint_params) + per-table
    authorization_details (RFC 9396), with HTTP Basic client creds — and handles token caching + refresh
    (including async refresh ahead of the ~1h expiry) for us.
    """
    def __init__(self, cfg: ZerobusConfig, signal: str):
        self._cc = ClientCredentials(
            client_id=cfg.client_id,
            client_secret=cfg.client_secret,
            token_url=cfg.token_url,
            scopes="all-apis",
            endpoint_params={"resource": cfg.resource_audience},               # ZeroBus audience
            authorization_details=_table_authorization_details(cfg, signal),   # per-table UC privileges
            use_header=True,                                                   # HTTP Basic client_id:secret
        )

    def token(self) -> str:
        return self._cc.token().access_token


class _BearerAuthPlugin(grpc.AuthMetadataPlugin):
    """Injects `authorization: Bearer <token>` on every gRPC call (token auto-refreshed)."""
    def __init__(self, token_source: _TokenSource):
        self._ts = token_source

    def __call__(self, context, callback):
        callback((("authorization", f"Bearer {self._ts.token()}"),), None)


def _channel_credentials(token_source: _TokenSource) -> grpc.ChannelCredentials:
    # TLS to ZeroBus + per-call bearer token. Call credentials require a secure channel.
    return grpc.composite_channel_credentials(
        grpc.ssl_channel_credentials(),
        grpc.metadata_call_credentials(_BearerAuthPlugin(token_source)),
    )


def _signal_exporter(exporter_cls, cfg: ZerobusConfig, signal: str):
    creds = _channel_credentials(_TokenSource(cfg, signal))
    return exporter_cls(
        endpoint=cfg.endpoint,
        credentials=creds,
        headers=(("x-databricks-zerobus-table-name", cfg.table(signal)),),
    )


def build_providers(cfg: ZerobusConfig, resource):
    """Return (TracerProvider, LoggerProvider, MeterProvider) wired DIRECTLY to ZeroBus."""
    tp = TracerProvider(resource=resource)
    tp.add_span_processor(BatchSpanProcessor(_signal_exporter(OTLPSpanExporter, cfg, "spans")))

    lp = LoggerProvider(resource=resource)
    lp.add_log_record_processor(BatchLogRecordProcessor(_signal_exporter(OTLPLogExporter, cfg, "logs")))

    reader = PeriodicExportingMetricReader(
        _signal_exporter(OTLPMetricExporter, cfg, "metrics"), export_interval_millis=2000)
    mp = MeterProvider(resource=resource, metric_readers=[reader])

    return tp, lp, mp
