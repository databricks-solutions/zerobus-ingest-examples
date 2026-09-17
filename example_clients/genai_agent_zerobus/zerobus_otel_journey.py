# Databricks notebook source
# MAGIC %md
# MAGIC # GenAI Agent → Zerobus (OpenTelemetry) — end to end in Databricks
# MAGIC
# MAGIC The Databricks-native companion to this example's `README.md` Quick start. Same flow, run from a notebook:
# MAGIC create the destination + experiment, run the LangChain agent, verify the spans, view the agentic trace.
# MAGIC
# MAGIC ```
# MAGIC Your Python agent ──(OpenTelemetry)──► Zerobus Ingest ──► Unity Catalog Delta ──► MLflow Traces UI
# MAGIC      gen_ai.* spans        OTLP/gRPC :443                   <prefix>_otel_*         agentic trace tree
# MAGIC ```
# MAGIC
# MAGIC This notebook **reuses the example's code** — it imports `zerobus_otel.py` (the drop-in exporter) and
# MAGIC `langchain_agent.py` (the agent) from this folder rather than re-defining them. That is why you run it
# MAGIC from inside a clone of the repo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run this in Databricks
# MAGIC
# MAGIC 1. **Clone this repo into your workspace** as a Git folder: sidebar → **Workspace** → your folder →
# MAGIC    **Create → Git folder** → paste the repo URL.
# MAGIC 2. Open **this notebook** from inside the clone
# MAGIC    (`example_clients/genai_agent_zerobus/zerobus_otel_journey.py`). Because it sits next to
# MAGIC    `zerobus_otel.py` and `langchain_agent.py`, `import zerobus_otel` / `from langchain_agent import …` just work.
# MAGIC 3. Attach **serverless** (or any) compute, fill in the **Configuration** cell, and **Run all**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - A Databricks workspace with **Zerobus Ingest** and **Unity Catalog** enabled, and a **SQL warehouse**
# MAGIC   (any; a Serverless one is ideal — this notebook auto-picks one).
# MAGIC - **An existing Unity Catalog catalog you can create schemas in** — the notebook creates the schema + tables,
# MAGIC   but creating a brand-new *catalog* needs a metastore admin, so point at a catalog you already have access to.
# MAGIC - **Permission to create a service principal and grant on that schema** (workspace admin, or equivalent).
# MAGIC   You create the SP in **step 3** — nothing to set up in advance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install dependencies
# MAGIC The example's `requirements.txt` (LangChain + the Traceloop instrumentor + the OTLP/gRPC exporter +
# MAGIC `databricks-langchain`), plus MLflow 3 for the Traces UI. Run from the example folder (this notebook's dir).

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt "mlflow>=3.13"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC Fill in each `<...>`. These match the variables in the README's `.env`. Only the service-principal
# MAGIC **secret** is sensitive — it's created and stored in a secret scope in step 3, never hard-coded here.

# COMMAND ----------

import os

# --- destination (Unity Catalog) — snake_case names (letters/digits/underscores, no hyphens) ---
CATALOG       = "<catalog-name>"                # must ALREADY exist (you need CREATE SCHEMA on it)
SCHEMA        = "otel"                           # created for you in step 2
TABLE_PREFIX  = "<your-table-prefix>"           # tables become <prefix>_otel_spans / _logs / _metrics

# --- workspace / ZeroBus endpoint ---
WORKSPACE_URL = "<your-workspace-host>"          # host only, e.g. dbc-xxxx.cloud.databricks.com
WORKSPACE_ID  = "<your-workspace-id>"            # numeric id (the o=... in the workspace URL)
REGION        = "<your-region>"                  # e.g. us-west-2

# --- agent + experiment ---
MODEL_ENDPOINT  = "databricks-claude-sonnet-5"   # any Databricks AI Gateway / Model Serving chat endpoint NAME
SERVICE_NAME    = "genai-agent"                   # the service.name you filter traces on
EXPERIMENT_NAME = "/Users/<your-email@example.com>/<experiment-name>"

# Export the non-secret config so the example's code (ZerobusConfig.from_env / langchain_agent /
# ChatDatabricks) picks it up. The SP creds (DATABRICKS_CLIENT_ID/SECRET) are added in step 3.
os.environ.update({
    "CATALOG": CATALOG, "SCHEMA": SCHEMA, "TABLE_PREFIX": TABLE_PREFIX,
    "WORKSPACE_URL": WORKSPACE_URL, "WORKSPACE_ID": WORKSPACE_ID, "REGION": REGION,
    "MODEL_ENDPOINT": MODEL_ENDPOINT, "OTEL_SERVICE_NAME": SERVICE_NAME,
})

FQ_PREFIX = f"{CATALOG}.{SCHEMA}.{TABLE_PREFIX}"
print("destination:", f"{FQ_PREFIX}_otel_spans / _logs / _metrics")
print("experiment :", EXPERIMENT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the schema, then the tables + experiment
# MAGIC The **catalog** must already exist. The notebook creates the **schema**, then binds an MLflow experiment to a
# MAGIC `trace_location` — which makes MLflow **create the four OTel tables**, register the telemetry profile, and add
# MAGIC its two reconstruction views. MLflow runs that SQL on a warehouse; we auto-pick one.

# COMMAND ----------

import mlflow
from mlflow.entities import UnityCatalog
from databricks.sdk import WorkspaceClient

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"schema ready: {CATALOG}.{SCHEMA}")

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
# MAGIC ## 3. Create the service principal, store its secret, and grant it
# MAGIC The agent writes to Zerobus as a **service principal**. This cell creates one (or reuses it), mints a
# MAGIC **1-year** OAuth secret, stores it in a secret scope, and grants it what its **table-scoped** Zerobus token
# MAGIC needs — `SELECT` + `MODIFY` **per table**, plus `USE CATALOG` / `USE SCHEMA` traversal. Then it exports the
# MAGIC SP creds so **both** the ZeroBus exporter and `ChatDatabricks` authenticate as this SP. Idempotent; needs
# MAGIC rights to create SPs, secret scopes, and grant on the schema.

# COMMAND ----------

SP_DISPLAY_NAME = "zerobus-agent-sp"   # the SP to create (or reuse if it already exists)
SECRET_SCOPE    = "zerobus"            # secret scope that will hold its client_secret

w = WorkspaceClient()

# find-or-create the service principal (idempotent across re-runs)
sp = next((s for s in w.service_principals.list() if s.display_name == SP_DISPLAY_NAME), None)
if sp is None:
    sp = w.service_principals.create(display_name=SP_DISPLAY_NAME)
CLIENT_ID = sp.application_id
print("service principal:", SP_DISPLAY_NAME, "| applicationId:", CLIENT_ID)

# mint a fresh 1-year OAuth secret; returned only once, so store it immediately
secret = w.service_principal_secrets_proxy.create(service_principal_id=sp.id, lifetime="31536000s")
try:
    w.secrets.create_scope(scope=SECRET_SCOPE)
except Exception:
    pass  # scope already exists
w.secrets.put_secret(scope=SECRET_SCOPE, key="client_secret", string_value=secret.secret)
print(f"secret stored in scope '{SECRET_SCOPE}' key 'client_secret' (value not printed)")

# grant the SP what its table-scoped Zerobus token needs (after the tables exist)
spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{CLIENT_ID}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{CLIENT_ID}`")
for signal in ["spans", "logs", "metrics"]:
    spark.sql(f"GRANT SELECT, MODIFY ON TABLE {FQ_PREFIX}_otel_{signal} TO `{CLIENT_ID}`")
print("granted USE CATALOG / USE SCHEMA + per-table SELECT, MODIFY to", CLIENT_ID)

# export SP creds so BOTH zerobus_otel (ZerobusConfig.from_env) and ChatDatabricks (SDK M2M) run as this SP
os.environ["DATABRICKS_HOST"] = f"https://{WORKSPACE_URL}"
os.environ["DATABRICKS_CLIENT_ID"] = CLIENT_ID
os.environ["DATABRICKS_CLIENT_SECRET"] = dbutils.secrets.get(SECRET_SCOPE, "client_secret")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run the agent
# MAGIC Import the example's code — `zerobus_otel` (the drop-in exporter) and `build_agent` (the reasoning +
# MAGIC tool-calling weather agent) — then do the only ZeroBus-aware wiring: build providers pointed at Zerobus and
# MAGIC hand the tracer provider to the LangChain instrumentor. This is exactly what `langchain_agent.py` does in
# MAGIC `main()`; the agent itself has no ZeroBus code.

# COMMAND ----------

import zerobus_otel
from langchain_agent import build_agent, QUESTION

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.langchain import LangchainInstrumentor

cfg = zerobus_otel.ZerobusConfig.from_env()
resource = Resource.create({"service.name": SERVICE_NAME, "service.namespace": "genai-agents"})

tracer_provider, logger_provider, meter_provider = zerobus_otel.build_providers(cfg, resource)
trace.set_tracer_provider(tracer_provider)
LangchainInstrumentor().instrument(tracer_provider=tracer_provider)

result = build_agent().invoke({"messages": [("user", QUESTION)]})
print("answer:", result["messages"][-1].content)

# flush the batch exporters so the spans reach ZeroBus before we query
tracer_provider.force_flush(); logger_provider.force_flush(); meter_provider.force_flush()
print("spans flushed to ZeroBus.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify the spans landed (SQL)
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
# MAGIC ## 6. See the agentic trace in MLflow
# MAGIC MLflow reconstructs the tree from the raw OTel spans and maps the `gen_ai.*` attributes onto MLflow span
# MAGIC types (`AGENT`, `CHAT_MODEL`, `TOOL`, `TASK`) — even though a generic instrumented app wrote them.

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
# MAGIC That view is reconstructed from the very rows ZeroBus ingested in step 4 — the full loop:
# MAGIC **agent → OpenTelemetry → ZeroBus → Unity Catalog → MLflow.**
# MAGIC
# MAGIC ### Next steps
# MAGIC - **Build evaluations** on these traces with MLflow's evaluation tools.
# MAGIC - **Correlate** the `_otel_*` tables against your cost / business tables.
# MAGIC - **Host the agent as a Databricks App** so it emits the same telemetry from a hosted service.
