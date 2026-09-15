# Databricks notebook source
# MAGIC %md
# MAGIC # ZeroBus + OpenTelemetry — agent observability in the lakehouse
# MAGIC
# MAGIC A runnable, end-to-end companion to the blog. It follows the same flow:
# MAGIC
# MAGIC 1. **MLflow creates the tables + experiment** — MLflow owns the Unity Catalog OTel tables and registers the trace "profile."
# MAGIC 2. **Configure & run the agent** — a LangChain chain, OpenTelemetry-instrumented, whose spans export to **ZeroBus** in-process and land in those tables.
# MAGIC 3. **Verify in SQL** — the spans are queryable Delta seconds later.
# MAGIC 4. **See the trace in MLflow** — the same rows render as an agentic trace tree.
# MAGIC
# MAGIC **Architecture:** `LangChain agent → OpenTelemetry → ZeroBus → Unity Catalog Delta → MLflow`
# MAGIC
# MAGIC > **Fill in every `<...>` placeholder** in the Configuration cell with your own workspace values and naming.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - **A SQL warehouse** in the workspace (any; a Serverless one is ideal — this notebook auto-picks one).
# MAGIC - **An existing Unity Catalog catalog you can create schemas in.** The notebook creates the schema + tables;
# MAGIC   creating a brand-new *catalog* needs a metastore admin, so point at a catalog you already have access to.
# MAGIC - **Permission to create a service principal and grant on that schema** (workspace admin, or equivalent).
# MAGIC   You'll create the SP in **step 3** — nothing to set up in advance.
# MAGIC - **MLflow 3** (installed in the next cell).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install dependencies
# MAGIC LangChain + the Traceloop OpenTelemetry instrumentor (emits `gen_ai.*` spans) + the OTLP/gRPC exporter, plus MLflow 3 and the Databricks LangChain bridge.

# COMMAND ----------

# MAGIC %pip install -q \
# MAGIC   "mlflow>=3.13" \
# MAGIC   databricks-langchain \
# MAGIC   langchain langchain-core \
# MAGIC   opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc \
# MAGIC   "opentelemetry-instrumentation-langchain==0.62.3"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC Fill in each `<...>`. Only the service-principal **secret** is sensitive — it's read from a secret scope; everything else is plain config.

# COMMAND ----------

# --- destination (Unity Catalog) — names are snake_case (letters/digits/underscores, no hyphens) ---
CATALOG       = "<catalog-name>"               # must ALREADY exist (you need CREATE SCHEMA on it)
SCHEMA        = "otel"                          # created for you in step 2
TABLE_PREFIX  = "<your-table-prefix>"          # tables become <prefix>_otel_spans / _logs / _metrics

# --- workspace / ZeroBus endpoint ---
WORKSPACE_URL = "<your-workspace-host>"         # host only, e.g. dbc-xxxx.cloud.databricks.com
WORKSPACE_ID  = "<your-workspace-id>"
REGION        = "<your-region>"                 # e.g. us-west-2

# --- agent + experiment ---
MODEL_ENDPOINT  = "databricks-claude-sonnet-5"  # any Model Serving chat endpoint in your workspace
SERVICE_NAME    = "<your-service-name>"          # the service.name you'll filter traces on
EXPERIMENT_NAME = "/Users/<your-email@example.com>/<experiment-name>"

FQ_PREFIX = f"{CATALOG}.{SCHEMA}.{TABLE_PREFIX}"
print("destination:", f"{FQ_PREFIX}_otel_spans / _logs / _metrics")
print("experiment :", EXPERIMENT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the schema, then the tables + experiment
# MAGIC The **catalog** must already exist (see prerequisites). The notebook creates the **schema** below, then binds an
# MAGIC experiment to a `trace_location` — which makes MLflow **create the four OTel tables**, register the telemetry
# MAGIC profile, and add its two reconstruction views. MLflow needs a SQL warehouse to run that SQL — we auto-pick one.

# COMMAND ----------

# Create the destination schema (the catalog must already exist — creating a catalog needs a metastore admin).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

import os, mlflow
from mlflow.entities import UnityCatalog
from databricks.sdk import WorkspaceClient

# MLflow runs its create-table / trace-read SQL on this warehouse (ZeroBus ingest itself needs none)
os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = next(wh.id for wh in WorkspaceClient().warehouses.list())

mlflow.set_tracking_uri("databricks")
exp = mlflow.set_experiment(
    experiment_name=EXPERIMENT_NAME,
    trace_location=UnityCatalog(catalog_name=CATALOG, schema_name=SCHEMA, table_prefix=TABLE_PREFIX),
)
print("experiment_id:", exp.experiment_id)
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE '{TABLE_PREFIX}*'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create the service principal for this table, and grant it
# MAGIC ZeroBus writes as a **service principal**. The next cell uses the Databricks SDK to create one (or reuse it),
# MAGIC mint a fresh OAuth secret, store it in a secret scope, and set `CLIENT_ID` / `SECRET_SCOPE` for the cells below —
# MAGIC no CLI, no manual copy/paste. The notebook user needs permission to create service principals and secret scopes.
# MAGIC More on the OAuth M2M flow: https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m
# MAGIC
# MAGIC The grant cell then gives that SP what its **table-scoped** token needs: `SELECT` + `MODIFY` **per table**;
# MAGIC `USE CATALOG` / `USE SCHEMA` are traversal privileges at catalog / schema scope. Needs GRANT rights on the
# MAGIC schema; idempotent — safe to re-run.

# COMMAND ----------

# Create (or reuse) the service principal, mint its OAuth secret, and store it in a secret scope.
# Sets CLIENT_ID + SECRET_SCOPE for the cells below. Needs rights to create SPs and secret scopes.
from databricks.sdk import WorkspaceClient

SP_DISPLAY_NAME = "zerobus-agent-sp"   # the SP to create (or reuse if it already exists)
SECRET_SCOPE    = "zerobus"            # secret scope that will hold its client_secret

w = WorkspaceClient()

# find-or-create the service principal (idempotent across re-runs)
sp = next((s for s in w.service_principals.list() if s.display_name == SP_DISPLAY_NAME), None)
if sp is None:
    sp = w.service_principals.create(display_name=SP_DISPLAY_NAME)
CLIENT_ID = sp.application_id
print("service principal:", SP_DISPLAY_NAME, "| applicationId:", CLIENT_ID)

# mint a fresh OAuth secret (1 year); returned only once, so store it immediately
secret = w.service_principal_secrets_proxy.create(service_principal_id=sp.id, lifetime="31536000s")
try:
    w.secrets.create_scope(scope=SECRET_SCOPE)
except Exception:
    pass  # scope already exists
w.secrets.put_secret(scope=SECRET_SCOPE, key="client_secret", string_value=secret.secret)
print(f"secret stored in scope '{SECRET_SCOPE}' key 'client_secret' (value not printed)")

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{CLIENT_ID}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{CLIENT_ID}`")
for signal in ["spans", "logs", "metrics"]:
    spark.sql(f"GRANT SELECT, MODIFY ON TABLE {FQ_PREFIX}_otel_{signal} TO `{CLIENT_ID}`")
print("granted USE CATALOG / USE SCHEMA + per-table SELECT, MODIFY to", CLIENT_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The ZeroBus exporter (embeddable helper)
# MAGIC This is the in-process OpenTelemetry → ZeroBus exporter. For each signal it uses the **Databricks SDK's
# MAGIC `ClientCredentials`** to mint a ZeroBus-scoped OAuth **access token** (resource audience + per-table
# MAGIC `authorization_details`) and attaches it — plus the `x-databricks-zerobus-table-name` header — on every
# MAGIC OTLP/gRPC call. Two different lifetimes to keep straight: the **access token** the SDK sends lives **~1 hour**
# MAGIC and the SDK **caches and auto-refreshes** it for you; it refreshes from the SP's **`client_secret`** (the
# MAGIC long-lived credential you stored in step 3). The agent never sees a token.
# MAGIC In a real project this would be an imported module (`producer/zerobus_otel.py`); it's inlined here so the
# MAGIC notebook is self-contained.

# COMMAND ----------

import json
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
    workspace_url: str; workspace_id: str; region: str
    client_id: str; client_secret: str
    catalog: str; schema: str; table_prefix: str

    @property
    def endpoint(self):
        return f"{self.workspace_id}.zerobus.{self.region}.cloud.databricks.com:443"

    @property
    def token_url(self):
        return f"https://{self.workspace_url}/oidc/v1/token"

    @property
    def resource_audience(self):
        return f"api://databricks/workspaces/{self.workspace_id}/zerobusDirectWriteApi"

    def table(self, signal):
        return f"{self.catalog}.{self.schema}.{self.table_prefix}_otel_{signal}"


def _auth_details(cfg, signal):
    return json.dumps([
        {"type": "unity_catalog_privileges", "privileges": ["USE CATALOG"], "object_type": "CATALOG", "object_full_path": cfg.catalog},
        {"type": "unity_catalog_privileges", "privileges": ["USE SCHEMA"], "object_type": "SCHEMA", "object_full_path": f"{cfg.catalog}.{cfg.schema}"},
        {"type": "unity_catalog_privileges", "privileges": ["SELECT", "MODIFY"], "object_type": "TABLE", "object_full_path": cfg.table(signal)},
    ])


class _TokenSource:
    """ZeroBus-scoped OAuth token for one signal/table, minted + auto-refreshed by the Databricks SDK's
    ClientCredentials. It sends exactly what the ZeroBus contract needs — the resource audience (via
    endpoint_params) + per-table authorization_details (RFC 9396), with HTTP Basic client creds — and
    caches + refreshes the ~1h access token for us (from the long-lived client_secret)."""
    def __init__(self, cfg, signal):
        self._cc = ClientCredentials(
            client_id=cfg.client_id, client_secret=cfg.client_secret, token_url=cfg.token_url,
            scopes="all-apis",
            endpoint_params={"resource": cfg.resource_audience},        # ZeroBus audience
            authorization_details=_auth_details(cfg, signal),          # per-table UC privileges
            use_header=True,                                           # HTTP Basic client_id:secret
        )
    def token(self):
        return self._cc.token().access_token


class _BearerAuthPlugin(grpc.AuthMetadataPlugin):
    def __init__(self, ts): self._ts = ts
    def __call__(self, context, callback): callback((("authorization", f"Bearer {self._ts.token()}"),), None)


def _creds(ts):
    return grpc.composite_channel_credentials(
        grpc.ssl_channel_credentials(), grpc.metadata_call_credentials(_BearerAuthPlugin(ts)))


def _exporter(cls, cfg, signal):
    return cls(endpoint=cfg.endpoint, credentials=_creds(_TokenSource(cfg, signal)),
               headers=(("x-databricks-zerobus-table-name", cfg.table(signal)),))


def build_providers(cfg, resource):
    tp = TracerProvider(resource=resource)
    tp.add_span_processor(BatchSpanProcessor(_exporter(OTLPSpanExporter, cfg, "spans")))
    lp = LoggerProvider(resource=resource)
    lp.add_log_record_processor(BatchLogRecordProcessor(_exporter(OTLPLogExporter, cfg, "logs")))
    reader = PeriodicExportingMetricReader(_exporter(OTLPMetricExporter, cfg, "metrics"), export_interval_millis=2000)
    mp = MeterProvider(resource=resource, metric_readers=[reader])
    return tp, lp, mp


print("ZeroBus exporter helper defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Configure and run the agent
# MAGIC An ordinary LangChain chain. The only ZeroBus-aware lines are `build_providers(...)` and attaching the
# MAGIC instrumentor to that provider — the chain itself has no ZeroBus code. We use `ChatDatabricks` so it runs
# MAGIC on Databricks Model Serving with no external API key (swap for `init_chat_model("<provider>:<model>")`
# MAGIC to point at your own provider — the telemetry path is identical).

# COMMAND ----------

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.langchain import LangchainInstrumentor
from databricks_langchain import ChatDatabricks
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# read the SP secret from the scope you populated in step 3 (the only sensitive value)
CLIENT_SECRET = dbutils.secrets.get(SECRET_SCOPE, "client_secret")

cfg = ZerobusConfig(
    workspace_url=WORKSPACE_URL, workspace_id=WORKSPACE_ID, region=REGION,
    client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
    catalog=CATALOG, schema=SCHEMA, table_prefix=TABLE_PREFIX,
)

# name this producer — service.name is the value you filter on in the Verification step
resource = Resource.create({"service.name": SERVICE_NAME})
tracer_provider, logger_provider, meter_provider = build_providers(cfg, resource)
trace.set_tracer_provider(tracer_provider)

# hand LangChain's spans to that provider — this is the whole integration
LangchainInstrumentor().instrument(tracer_provider=tracer_provider)

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful weather assistant. Answer in one sentence."),
    ("human", "{question}"),
])
model = ChatDatabricks(endpoint=MODEL_ENDPOINT)
chain = prompt | model | StrOutputParser()

answer = chain.invoke({"question": "What's the weather in Paris, and should I bring an umbrella?"})
print("answer:", answer)

# flush the batch exporters so the spans reach ZeroBus before we query
tracer_provider.force_flush(); logger_provider.force_flush(); meter_provider.force_flush()
print("spans flushed to ZeroBus.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify the spans landed (SQL)
# MAGIC ZeroBus ingestion is near-real-time — the rows are queryable within seconds. Filter on the `service.name`
# MAGIC you set above. (You can also browse the table in **Catalog Explorer** under your catalog → `otel` schema.)

# COMMAND ----------

import time as _t
_t.sleep(8)  # allow async ingestion
display(spark.sql(f"""
  SELECT name, kind, attributes:['gen_ai.operation.name']::string AS op, service_name
  FROM {FQ_PREFIX}_otel_spans
  WHERE service_name = '{SERVICE_NAME}'
  ORDER BY start_time_unix_nano
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. See the agentic trace in MLflow
# MAGIC MLflow reconstructs the tree from the raw OTel spans and maps the `gen_ai.*` attributes onto MLflow span
# MAGIC types (`AGENT`, `CHAT_MODEL`, `TASK`) — even though a generic Traceloop-instrumented app wrote them.

# COMMAND ----------

traces = mlflow.search_traces(
    experiment_ids=[exp.experiment_id],
    filter_string="trace.timestamp_ms > 1735689600000",   # epoch-ms, not a date string
    return_type="list",
)
print(f"reconstructed {len(traces)} trace(s):")
for t in traces:
    print(f"  trace_id={t.info.trace_id}  #spans={len(t.data.spans)}")
    for s in t.data.spans:
        print(f"     - {s.name} [{s.span_type}]")

# COMMAND ----------

# MAGIC %md
# MAGIC To see the interactive tree: left sidebar → **Experiments** → open your experiment → **Traces** tab.
# MAGIC That view is reconstructed from the very rows ZeroBus ingested in step 5 — the full loop:
# MAGIC **agent → OpenTelemetry → ZeroBus → Unity Catalog → MLflow.**
# MAGIC
# MAGIC ### Next steps
# MAGIC - **Build evaluations** on these traces with MLflow's evaluation tools.
# MAGIC - **Correlate** the `_otel_*` tables against your cost / business tables.
# MAGIC - **Host the agent as a Databricks App** so it emits the same telemetry from a hosted service.

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Clean up
# MAGIC Uncomment to remove the tables/views this notebook created. The MLflow experiment is deleted separately from the Experiments UI.

# COMMAND ----------

# for suffix in ["otel_spans", "otel_logs", "otel_metrics", "otel_annotations", "trace_metadata", "trace_unified"]:
#     spark.sql(f"DROP TABLE IF EXISTS {FQ_PREFIX}_{suffix}")
# print("dropped", TABLE_PREFIX, "tables/views")
