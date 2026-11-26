# LangChain and LangGraph with OpenInference: agent traces into Delta with Zerobus Ingest

This example sends the OpenTelemetry traces of a LangChain or LangGraph agent into a Unity Catalog Delta table through [Zerobus Ingest](https://docs.databricks.com/ingestion/zerobus-overview) and the OpenTelemetry Collector. The agent is instrumented with Arize's [OpenInference](https://github.com/Arize-ai/openinference) instrumentor for LangChain, which records each run as spans with the OpenInference semantic conventions (`openinference.span.kind` is `AGENT`, `CHAIN`, `LLM` or `TOOL`).

The application sends plain OTLP to a Collector on the same host. The Collector's `oauth2client` extension gets and refreshes the OAuth token for the service principal and forwards the spans to Zerobus Ingest. The application has no token code.

There is no message bus between the agent and the lakehouse.

```
┌────────────────────────────┐
│ LangChain / LangGraph agent│  create_agent + one tool
│ OpenInference instrumentor │  spans: AGENT, CHAIN, LLM, TOOL
└─────────────┬──────────────┘
              │  OTLP/HTTP  127.0.0.1:4318
              v
┌────────────────────────────┐
│ OpenTelemetry Collector    │  oauth2client gets and refreshes the OAuth token
│ (contrib)                  │  header: x-databricks-zerobus-table-name
└─────────────┬──────────────┘
              │  OTLP/gRPC :443
              v
┌────────────────────────────┐
│ Zerobus Ingest             │  writes to <prefix>_otel_spans
│ Delta table in Unity Catalog│
└────────────────────────────┘
```

The example uses the OpenInference conventions, not the `gen_ai.*` conventions of the [GenAI agent example](../genai_agent_zerobus/). It has no in-process Zerobus exporter, so the Collector is the only component that holds the service principal's secret.

The agent in this example uses a scripted chat model that makes no network calls, so you don't need an LLM key to run it. Swap in a real model as described in [How to extend](#how-to-extend).

## Overview

- `src/agent.py`: a LangChain agent (`create_agent`) with a `get_weather` tool and an offline scripted chat model.
- `src/telemetry.py`: sets up the OpenTelemetry tracer provider, the OTLP/HTTP exporter and the OpenInference LangChain instrumentor.
- `src/run_agent.py`: runs the agent in a loop, one run per interval.
- `config.example.yaml`: the Collector configuration. It has one `oauth2client` extension and one OTLP gRPC exporter for the spans table.
- `create_table.sql`: the spans table. It's the same definition as the OpenTelemetry Collector example.
- `tests/`: offline tests for the spans the agent emits. They don't need a Collector, Zerobus or a network connection.

## Prerequisites

- Python 3.12, with `pip`.
- The [OpenTelemetry Collector Contrib](https://github.com/open-telemetry/opentelemetry-collector-releases/releases) distribution, version 0.165.0 or later. The `oauth2client` extension is in Contrib, not in the core distribution.
- A Unity Catalog catalog and schema where you can create tables, and a Databricks SQL warehouse to run the table statement.
- A service principal with an OAuth secret. It needs `USE CATALOG` on the catalog, `USE SCHEMA` on the schema, and `MODIFY` and `SELECT` on the spans table. `ALL PRIVILEGES` isn't enough. See [Configure OpenTelemetry (OTLP) clients](https://docs.databricks.com/aws/en/ingestion/opentelemetry/configure) for the service principal setup.
- DBR 17.3 or above to create and query the table, because the table definition turns on variant shredding.

## Configuration reference

Copy `config.example.env` to `config.env` and fill it in. Load it into the shell that starts the Collector, and don't commit it.

| Variable | Used by | Value |
| --- | --- | --- |
| `DATABRICKS_CLIENT_ID` | Collector | The service principal's application ID. |
| `DATABRICKS_CLIENT_SECRET` | Collector | The service principal's OAuth secret. |
| `WORKSPACE_URL` | Collector | Your workspace URL without `https://`. |
| `WORKSPACE_ID` | Collector | Your workspace ID. |
| `ZEROBUS_ENDPOINT` | Collector | Your Zerobus Ingest endpoint host, without `https://` or the port. |
| `CATALOG`, `SCHEMA` | Collector | The Unity Catalog catalog and schema that hold the table. |
| `TABLE_PREFIX` | Collector | The table prefix. The table is `<prefix>_otel_spans`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Agent | The Collector's HTTP base URL, `http://127.0.0.1:4318`. The exporter adds `/v1/traces`. |
| `OTEL_SERVICE_NAME` | Agent | The `service.name` on each span. Defaults to `langchain-agent`. |
| `OPENINFERENCE_HIDE_INPUTS`, `OPENINFERENCE_HIDE_OUTPUTS` | Agent | Set to `true` to leave prompts, tool inputs and outputs out of the spans. See [How to extend](#how-to-extend). |

The Collector listens on `127.0.0.1` only, on ports 4317 (gRPC) and 4318 (HTTP). Other hosts can't reach it.

To find your workspace URL and Zerobus Ingest endpoint, see [Get your workspace URL and Zerobus Ingest endpoint](https://docs.databricks.com/ingestion/zerobus-ingest#get-your-workspace-url-and-zerobus-ingest-endpoint).

## Quick start

1. **Create the table and grant access.** Replace the placeholders in `create_table.sql` with your catalog, schema and prefix, then run it in a SQL warehouse. Grant the service principal `USE CATALOG` on the catalog, `USE SCHEMA` on the schema, and `MODIFY` and `SELECT` on `<prefix>_otel_spans`.

2. **Install the Collector Contrib.** Download the archive for your platform from the OpenTelemetry Collector releases page, check its SHA-256 hash, extract it, then confirm that `oauth2client` is in the build:

   ```bash
   ./otelcol-contrib --version
   ./otelcol-contrib components | grep oauth2client
   ```

3. **Start the Collector** with `config.example.yaml`, after you load `config.env`:

   ```bash
   set -a; . ./config.env; set +a
   ./otelcol-contrib --config=config.example.yaml
   ```

   Wait for `Everything is ready. Begin running and processing data.`

4. **Install the example's dependencies** in a virtual environment. The versions are pinned in `requirements.txt`.

   ```bash
   python3.12 -m venv .venv && . .venv/bin/activate
   pip install -r requirements-dev.txt
   ```

5. **Run the offline tests.** They don't need the Collector, Zerobus or a network connection.

   ```bash
   PYTHONPATH=src python -m pytest -q tests
   ```

6. **Run the agent.** Each run sends one trace (seven spans for the scripted agent) to the Collector.

   ```bash
   export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
   PYTHONPATH=src python src/run_agent.py --runs 10 --interval 1
   ```

   If the Collector stops while the agent runs, the agent's exporter retries. A batch whose retries run out is dropped, so expect a gap in the rows for the time the Collector is down.

7. **Query the table** in a SQL warehouse:

   ```sql
   SELECT name, kind, variant_get(attributes, '$["openinference.span.kind"]', 'STRING') AS openinference_kind, count(*) AS spans
   FROM <catalog>.<schema>.<prefix>_otel_spans
   GROUP BY name, kind, openinference_kind
   ORDER BY spans DESC;
   ```

   You should see rows for `weather_agent` (AGENT), `tools` and `model` (CHAIN), `ScriptedChatModel` (LLM) and `get_weather` (TOOL).

## Schema

The table `<prefix>_otel_spans` has one row per span. Its columns follow the OpenTelemetry Collector's spans table: `time`, `service_name`, `trace_id`, `span_id`, `parent_span_id`, `name`, `kind`, `start_time_unix_nano`, `end_time_unix_nano`, `status`, `events`, `links`, and the `resource` and `instrumentation_scope` structs. The `attributes` column is a `VARIANT`. The OpenInference attributes are in it:

| Attribute | Example values | Notes |
| --- | --- | --- |
| `openinference.span.kind` | `AGENT`, `CHAIN`, `LLM`, `TOOL` | Read it with `variant_get(attributes, '$["openinference.span.kind"]', 'STRING')`. |
| `input.value`, `output.value` | The tool's input and output, or the chain's state | Omitted when `OPENINFERENCE_HIDE_INPUTS` or `OPENINFERENCE_HIDE_OUTPUTS` is `true`. |
| `llm.*`, `tool.*` | Model name, messages, tool name and parameters | Set on `LLM` and `TOOL` spans. |
| `metadata` | LangGraph node, step and agent name | A JSON string. |

The `name` column holds the span name, such as `weather_agent` or `get_weather`. The `kind` column is the OpenTelemetry span kind, which is `SPAN_KIND_INTERNAL` for these spans.

For the full column list and types, see [`create_table.sql`](create_table.sql).

## How to extend

- **Use a live model.** Replace `ScriptedChatModel` in `src/agent.py` with a chat model from a LangChain partner package, such as `ChatOpenAI` from `langchain-openai` or `ChatDatabricks` from `databricks-langchain`. Pin the package version in `requirements.txt` first. Keep `build_agent()` and the instrumentation as they are: the instrumentor records the model's `LLM` spans with no other change.
- **Leave prompts and outputs out.** OpenInference records inputs and outputs by default, so prompts and tool results are written to the table. To keep them out, set `OPENINFERENCE_HIDE_INPUTS=true` and `OPENINFERENCE_HIDE_OUTPUTS=true` before you start the agent. The OpenInference configuration has finer switches, such as `OPENINFERENCE_HIDE_INPUT_MESSAGES` and `OPENINFERENCE_HIDE_OUTPUT_MESSAGES`.
- **Trace your own graph.** The instrumentor covers any LangChain or LangGraph run in the process, so a `StateGraph` you build yourself produces the same span kinds. Call `setup_tracing()` once before you build the graph.
- **Send the same spans to a second destination.** Add a second exporter under `service.pipelines.traces.exporters` in the Collector configuration, for example a local OTLP trace viewer. The Collector sends each span to every exporter in the list.
- **Add logs or metrics.** The Collector's `logs` and `metrics` pipelines write to `<prefix>_otel_logs` and `<prefix>_otel_metrics`. Create those tables from the [Configure OpenTelemetry (OTLP) clients](https://docs.databricks.com/aws/en/ingestion/opentelemetry/configure) page, add a `oauth2client` extension with its own table scope for each, and add the pipelines.
- **Other frameworks.** This example covers LangChain and LangGraph. The same Collector serves any OpenTelemetry-instrumented app that sends OTLP to `127.0.0.1:4318`.

## File reference

| File | Purpose |
| --- | --- |
| `src/agent.py` | The LangChain agent, its tool, and the scripted chat model. |
| `src/telemetry.py` | Tracer provider, OTLP/HTTP exporter and OpenInference instrumentation. |
| `src/run_agent.py` | Command-line loop that runs the agent. |
| `tests/test_agent_spans.py` | Offline tests for the spans. |
| `config.example.yaml` | Collector configuration with `${env:...}` placeholders. |
| `config.example.env` | Environment variables for the Collector and the agent, with placeholders. |
| `create_table.sql` | The spans table definition. |
| `requirements.txt`, `requirements-dev.txt` | Pinned dependencies. |
