# Google ADK to Zerobus Ingest (OpenTelemetry)

## Overview

This example runs a [Google Agent Development Kit (ADK)](https://google.github.io/adk-docs/) agent and sends its traces to a Delta table in Databricks through Zerobus Ingest OTLP. ADK creates the spans itself: `invoke_workflow`, `invoke_agent`, `generate_content` and `execute_tool`, with the `gen_ai.*` attributes from the OpenTelemetry GenAI semantic conventions. Nothing in the agent code creates a span by hand.

The agent app exports the spans over OTLP/HTTP to a local OpenTelemetry Collector (contrib distribution). The Collector's `oauth2client` extension gets and refreshes the service principal's OAuth token, and forwards the spans to Zerobus Ingest.

The agent uses a fake model, so you don't need a Gemini or other model API key. The model asks for one `roll_dice` tool call and then writes an answer from the tool result. To use a real model, see [How to extend](#how-to-extend).

```
ADK agent (google-adk)  --OTLP/HTTP :4318-->  OpenTelemetry Collector (oauth2client)  --OTLP/gRPC-->  Zerobus Ingest  -->  Delta table
```

## Prerequisites

- Python 3.12.
- OpenTelemetry Collector contrib v0.165.0 or later, from the [releases page](https://github.com/open-telemetry/opentelemetry-collector-releases/releases) or the `otel/opentelemetry-collector-contrib` image. The `oauth2client` extension must be in the build.
- A Databricks workspace with Zerobus Ingest enabled, and a service principal with an OAuth secret.
- Privileges for the service principal on the target table: `USE CATALOG` on the catalog, `USE SCHEMA` on the schema, and `MODIFY` and `SELECT` on the table. The table's creator already has these.

## Configuration reference

Copy `config.example.env` to `.env` and fill it in. The Collector reads the `DATABRICKS_*`, `WORKSPACE_ID`, `REGION`, `ZEROBUS_ENDPOINT`, `CATALOG`, `SCHEMA` and `TABLE_PREFIX` values. The agent app reads the `OTEL_*` values.

| Variable | Used by | Description |
|----------|---------|-------------|
| `DATABRICKS_WORKSPACE_URL` | Collector | Workspace URL, for example `https://<workspace-url>`. The token is requested from `<workspace-url>/oidc/v1/token`. |
| `DATABRICKS_CLIENT_ID` | Collector | Service principal client ID. |
| `DATABRICKS_CLIENT_SECRET` | Collector | Service principal OAuth secret. Keep it out of source control. |
| `WORKSPACE_ID` | Collector | Workspace ID, used in the `resource` for the token request. |
| `ZEROBUS_ENDPOINT` | Collector | Zerobus Ingest host, `<workspace-id>.zerobus.<region>.cloud.databricks.com`. |
| `CATALOG`, `SCHEMA`, `TABLE_PREFIX` | Collector | The table name is `<catalog>.<schema>.<prefix>_otel_spans`. |
| `OTEL_SERVICE_NAME` | Agent app | Service name on every span, stored in `service_name`. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Agent app | Collector OTLP/HTTP traces URL, `http://127.0.0.1:4318/v1/traces`. |
| `ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS` | Agent app | ADK records prompt and response content in spans by default (`true`). Set it to `false` if the content is sensitive. |

## Quick start

1. Create the spans table. Replace the placeholders in `sql/create_otel_spans_table.sql` with your catalog, schema and prefix, then run it in a SQL editor or with the Databricks CLI.

2. Grant the service principal access to the table:

   ```sql
   GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-name>`;
   GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-name>`;
   GRANT MODIFY, SELECT ON TABLE <catalog>.<schema>.<prefix>_otel_spans TO `<service-principal-name>`;
   ```

3. Install the agent's pinned dependencies in a virtual environment:

   ```bash
   python3.12 -m venv .venv
   . .venv/bin/activate
   pip install -r requirements.txt
   ```

4. Start the Collector with your `.env` values loaded:

   ```bash
   set -a; . ./.env; set +a
   otelcol-contrib --config collector/collector.yaml
   ```

   Wait for `Everything is ready. Begin running and processing data.` in the output.

5. In a second terminal, run the agent. Each run is one agent invocation with one tool call:

   ```bash
   set -a; . ./.env; set +a
   python src/dice_agent.py --runs 5 --interval 1
   ```

   Each line of output looks like `run 1/5: Rolled a d6 and got 4.`

6. Query the table after about 10 seconds:

   ```sql
   SELECT name, count(*) AS n
   FROM <catalog>.<schema>.<prefix>_otel_spans
   GROUP BY name
   ORDER BY name;
   ```

   Five runs produce 25 spans: 5 `invoke_workflow`, 5 `invoke_agent`, 5 `execute_tool` and 10 `generate_content`.

7. Run the offline tests, which don't need Databricks or the Collector:

   ```bash
   pip install -r requirements-dev.txt
   python -m pytest -q tests
   ```

## Schema

The table has the fixed OpenTelemetry Zerobus schema (`otel.schemaVersion` = `v2`). It is defined in [`sql/create_otel_spans_table.sql`](sql/create_otel_spans_table.sql). The columns that matter for agent traces:

| Column | Example value |
|--------|---------------|
| `name` | `invoke_agent dice_agent`, `generate_content fake-dice-model`, `execute_tool roll_dice` |
| `trace_id`, `span_id`, `parent_span_id` | Link each step to its agent run |
| `service_name` | `adk-dice-agent` (from `OTEL_SERVICE_NAME`) |
| `attributes` | `VARIANT`: `gen_ai.operation.name`, `gen_ai.agent.name`, `gen_ai.tool.name`, `gen_ai.usage.input_tokens` and similar |
| `start_time_unix_nano`, `end_time_unix_nano` | Span timing |

The span tree for one agent run is `invoke_workflow` at the root, with `invoke_agent` below it. Under `invoke_agent` are the `generate_content` spans (one per model call) and the `execute_tool` span (one per tool call). Use `parent_span_id` to rebuild it.

To see the attributes of a span, query `to_json(attributes)`.

## How to extend

- **Use a real model.** Replace `FakeDiceModel` in `src/fake_model.py` with an ADK model, such as a Gemini or LiteLLM model, and set its credentials in the environment. The spans don't change. `generate_content` gets the model's name and token usage.
- **Add tools.** Add Python functions to `tools=[...]` on the `LlmAgent` in `src/dice_agent.py`. ADK creates an `execute_tool` span for each call.
- **Send logs and metrics.** This example exports traces only. ADK also exports logs and metrics when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, but this example doesn't test them. To add them, create the matching `_otel_logs` and `_otel_metrics` tables, and add a logs and a metrics pipeline in `collector/collector.yaml` with their own `oauth2client` extension and exporter.
- **Keep content out of spans.** Set `ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS=false` to drop prompts and responses from the spans.
