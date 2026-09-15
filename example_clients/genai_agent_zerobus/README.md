# GenAI Agent → Zerobus (OpenTelemetry)

Send a Python GenAI agent's **OpenTelemetry** telemetry — traces, logs, metrics — straight into
**Unity Catalog** Delta tables via **Zerobus Ingest**, then view it as an agentic trace tree in
**MLflow**. No collector, no sidecar: the agent authenticates as a service principal and exports
OTLP/gRPC directly to Zerobus.

```
Your Python agent ──(OpenTelemetry)──► Zerobus Ingest ──► Unity Catalog Delta ──► MLflow Traces UI
     gen_ai.* spans        OTLP/gRPC :443                   <prefix>_otel_*         agentic trace tree
```

Because the popular agent frameworks already emit the OpenTelemetry GenAI (`gen_ai.*`) semantic
conventions, wiring one to Zerobus is a **configuration change, not a rewrite**. This example uses
LangChain, but the reusable piece — [`zerobus_otel.py`](zerobus_otel.py) — works with any OTel-based
Python agent.

## What's here

| File | What it is |
|---|---|
| [`zerobus_otel.py`](zerobus_otel.py) | **The drop-in.** In-process OTLP→Zerobus exporter + the OAuth auth flow. Import it into any agent. |
| [`langchain_agent.py`](langchain_agent.py) | Worked example: instrument a LangChain agent with two lines and ship its spans to Zerobus. |
| [`notebook/zerobus_otel_journey.py`](notebook/zerobus_otel_journey.py) | End-to-end explainer notebook (Databricks): creates the tables + experiment, runs the agent, verifies, and shows the trace. |
| `.env.example` | Configuration template. |
| `requirements.txt` | Dependencies. |

## How the auth works

Zerobus accepts OTLP with two things beyond standard OpenTelemetry, on every request:

1. **A bearer token** — minted for a **service principal** via OAuth client-credentials, scoped to
   Zerobus with a `resource` audience (`api://databricks/workspaces/<id>/zerobusDirectWriteApi`) and
   **per-table** `authorization_details` (RFC 9396).
2. **A table-name header** — `x-databricks-zerobus-table-name` — because each signal routes to its own table.

`zerobus_otel.py` handles all of it using the **Databricks SDK's `ClientCredentials`** token source,
which mints and **auto-refreshes** the token for you. Two lifetimes to keep straight:

- The **access token** the SDK sends is short-lived (**~1 hour**) and refreshed automatically.
- It refreshes from the service principal's **`client_secret`** — the long-lived credential you create
  once and store securely.

## Prerequisites

- A Databricks workspace with **Zerobus Ingest** and **Unity Catalog** enabled, and a **SQL warehouse**.
- A **service principal** with an OAuth secret. See
  [OAuth M2M authentication](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m).
- The destination **OTel tables must exist** before first ingest (Zerobus does not auto-create them).
  Easiest path: let **MLflow** create them (see the notebook). Or create them with SQL — the four tables
  are `<prefix>_otel_spans`, `_otel_logs`, `_otel_metrics` (MLflow also adds `_otel_annotations` and two
  `_trace_*` views).
- The service principal must be granted, **after the tables exist**:
  ```sql
  GRANT USE CATALOG ON CATALOG <catalog>              TO `<sp-application-id>`;
  GRANT USE SCHEMA  ON SCHEMA  <catalog>.<schema>     TO `<sp-application-id>`;
  GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.<prefix>_otel_spans   TO `<sp-application-id>`;
  GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.<prefix>_otel_logs    TO `<sp-application-id>`;
  GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.<prefix>_otel_metrics TO `<sp-application-id>`;
  ```
  `SELECT`/`MODIFY` are **table-level** (Zerobus's token is table-scoped); `USE CATALOG`/`USE SCHEMA`
  are catalog/schema traversal grants.

## Configuration

Copy `.env.example` to `.env` and fill it in (only `DATABRICKS_CLIENT_SECRET` is sensitive):

| Variable | Description |
|---|---|
| `CATALOG` / `SCHEMA` / `TABLE_PREFIX` | Destination. Tables are `<CATALOG>.<SCHEMA>.<TABLE_PREFIX>_otel_{spans,logs,metrics}`. |
| `WORKSPACE_URL` | Workspace host only, e.g. `dbc-xxxx.cloud.databricks.com`. |
| `WORKSPACE_ID` | Numeric workspace id (the `o=...` in the workspace URL). |
| `REGION` | e.g. `us-west-2`. |
| `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` | The service principal's OAuth credentials. |
| `OTEL_SERVICE_NAME` | How this producer is named in the traces (what you filter on). |

## Quick start

```bash
pip install -r requirements.txt
cp .env.example .env            # then fill it in
set -a; . ./.env; set +a        # load config into the environment
python langchain_agent.py
```

By default `langchain_agent.py` uses a fake chat model, so it runs with **no model API key** and still
emits `gen_ai.*` spans. To use a real model, swap one line in `build_chain()` (see the comments) — e.g.
`ChatDatabricks(endpoint=...)` on Databricks, or `init_chat_model("<provider>:<model>")` for any provider.

Verify the spans landed (SQL Editor, or Catalog Explorer under your `<schema>`):

```sql
SELECT name, kind, attributes:['gen_ai.operation.name']::string AS op, service_name
FROM <catalog>.<schema>.<prefix>_otel_spans
WHERE service_name = '<your OTEL_SERVICE_NAME>'
ORDER BY start_time_unix_nano;
```

## Drop it into your own agent

You don't need LangChain — `zerobus_otel.py` works with any OpenTelemetry-instrumented Python code:

```python
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from zerobus_otel import ZerobusConfig, build_providers

tp, lp, mp = build_providers(ZerobusConfig.from_env(),
                             Resource.create({"service.name": "my-agent"}))
trace.set_tracer_provider(tp)
# ...then instrument your framework (or use your own spans); flush tp/lp/mp before exit.
```

For LangChain specifically, the whole integration is two lines — build the providers, then
`LangchainInstrumentor().instrument(tracer_provider=tp)`. See `langchain_agent.py`.

## See it in MLflow

MLflow reconstructs the raw OTel spans as an agentic trace tree — mapping `gen_ai.*` attributes onto
MLflow span types (`AGENT`, `CHAT_MODEL`, `TASK`) — even though a generic OTel-instrumented app wrote
them. Have MLflow **own the tables** (bind an experiment to the same catalog/schema/prefix via a
`trace_location`), let Zerobus **write into them**, then open the experiment's **Traces** tab. The
notebook walks this end to end.

## Ingested data / schema

Standard Databricks OpenTelemetry v2 tables:

| Table | Contents |
|---|---|
| `<prefix>_otel_spans` | Trace spans — the agent/LLM/tool call tree, with `gen_ai.*` attributes |
| `<prefix>_otel_logs` | Log events emitted during runs |
| `<prefix>_otel_metrics` | Metrics (e.g. token usage, durations) |

## Extend

- **Any framework:** OpenAI, LlamaIndex, DSPy, etc. — swap the LangChain instrumentor for that
  framework's OTel instrumentor; the export path is identical.
- **Logs & metrics:** the same `build_providers()` wires logger and meter providers alongside traces.
- **Serverless / short-lived processes:** always `force_flush()` before the process exits (the batch
  exporters won't flush on their own).
