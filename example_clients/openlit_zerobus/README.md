# OpenLIT to Zerobus Ingest: traces and metrics through the OpenTelemetry Collector

This example sends traces and metrics from an LLM application instrumented with [OpenLIT](https://github.com/openlit/openlit) to two Unity Catalog Delta tables through Zerobus Ingest's OTLP endpoint. OpenLIT's `openlit.init()` call instruments the LLM framework, and the application sends OTLP to the OpenTelemetry Collector on the same host. The Collector's `oauth2client` extension gets an OAuth token from a service principal, refreshes it before it expires, and forwards each signal to its table. The chat model in the app is a fake model, so the example runs offline and needs no model API key.

## Overview

OpenLIT is an Apache-2.0 SDK. A single `openlit.init()` call instruments the supported LLM providers and frameworks in the process, and exports OpenTelemetry traces and metrics over OTLP. This example uses LangChain: each chain call creates a `chat` span with the `gen_ai.*` attributes, and the SDK records the `gen_ai.client.*` and `gen_ai.server.*` histograms.

The SDK can't get OAuth tokens for Zerobus Ingest. The Collector's `oauth2client` extension does that, so the application needs no credentials. Its configuration has one `oauth2client` extension and one exporter per table, and a pipeline for each signal.

Use this example when your LLM application runs longer than one hour and you don't want each application process to handle OAuth tokens. The Collector runs next to the application, so the application sends to `localhost` and doesn't need a separate message bus in between.

Files:

- `src/app.py`: the dice-game app. It calls `openlit.init()` and runs a LangChain chain on a fake chat model in a loop.
- `config.example.yaml`: the Collector configuration, with environment variables and no secrets.
- `scripts/create_tables.sql`: the traces and metrics tables.
- `scripts/send_bad_span.py`: sends one OTLP request with a bad span, for testing how the pipeline handles bad records.
- `tests/test_app.py`: offline unit tests.
- `requirements.txt` and `requirements-dev.txt`: pinned Python dependencies.

## Prerequisites

- A Databricks workspace in a region that supports Zerobus Ingest, with Unity Catalog. Databricks Runtime 17.3 or above is needed to create and query these tables, because the table definitions turn on variant shredding.
- A service principal with an OAuth secret and the Workspace access entitlement. Grant it `USE CATALOG` on the catalog, `USE SCHEMA` on the schema, and `MODIFY` and `SELECT` on each table. `ALL PRIVILEGES` isn't enough. See [Zerobus Ingest OpenTelemetry configuration](https://docs.databricks.com/aws/en/ingestion/opentelemetry/configure).
- The OpenTelemetry Collector Contrib distribution, release v0.165.0 or later. The `oauth2client` extension is not in the core distribution. This example was tested with v0.165.0.
- Python 3.12. The pinned packages are `openlit` 1.45.0 and `langchain-core` 1.8.2, with the rest of the dependency tree pinned in `requirements.txt`.

## Configuration reference

Set these environment variables in the shell where you start the Collector. Set the application variables in the shell where you run the app.

| Variable | Where | What it is |
|---|---|---|
| `DATABRICKS_CLIENT_ID` | Collector | The service principal's application ID. |
| `DATABRICKS_CLIENT_SECRET` | Collector | The service principal's OAuth secret. The Collector reads it from the environment, so the secret stays out of the file. |
| `DATABRICKS_WORKSPACE_URL` | Collector | Your workspace URL, `https://<workspace-url>`. The token URL is this value with `/oidc/v1/token` added. |
| `WORKSPACE_ID` | Collector | Your workspace ID, `<workspace-id>`. It appears in the token's `resource` value. |
| `ZEROBUS_ENDPOINT` | Collector | The Zerobus Ingest endpoint host, `<workspace-id>.zerobus.<region>.cloud.databricks.com`, without the port. The Collector adds `:443`. |
| `CATALOG`, `SCHEMA` | Collector | The Unity Catalog catalog and schema that hold the tables. |
| `TABLE_PREFIX` | Collector | The prefix of the two tables: `<prefix>_otel_spans` and `<prefix>_otel_metrics`. |
| `OPENLIT_OTLP_ENDPOINT` | App | Where OpenLIT sends OTLP over HTTP. Defaults to `http://127.0.0.1:4318`, the Collector's HTTP receiver. |
| `OPENLIT_ENVIRONMENT` | App | The `deployment.environment` value on the telemetry. Defaults to `demo`. |

The Collector configuration in `config.example.yaml` has these parts:

- Two `oauth2client/…` extensions, one for each table. Each token's `authorization_details` allows `USE CATALOG`, `USE SCHEMA`, and `SELECT` and `MODIFY` on its own table.
- Two `otlp_grpc/…` exporters. Each sends to the same endpoint with its own extension and its own `x-databricks-zerobus-table-name` header.
- An OTLP receiver on `127.0.0.1:4317` (gRPC) and `127.0.0.1:4318` (HTTP), a `batch` processor, and two pipelines (traces and metrics).

The app calls `openlit.init()` with `otlp_endpoint`, `application_name`, `environment`, and `disable_events=True`. See [Logs](#logs) below for why events are off.

## Quick start

1. Create the tables. Replace `<catalog>.<schema>.<prefix>` in `scripts/create_tables.sql` with your values, and run the file in Databricks SQL. Then grant the service principal access:

   ```sql
   GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-uuid>`;
   GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-uuid>`;
   GRANT MODIFY, SELECT ON TABLE <catalog>.<schema>.<prefix>_otel_spans TO `<service-principal-uuid>`;
   GRANT MODIFY, SELECT ON TABLE <catalog>.<schema>.<prefix>_otel_metrics TO `<service-principal-uuid>`;
   ```

2. Install the Collector Contrib. Download the archive for your platform from the [OpenTelemetry Collector releases page](https://github.com/open-telemetry/opentelemetry-collector-releases/releases), then check the binary:

   ```bash
   tar -xzf otelcol-contrib_<version>_linux_amd64.tar.gz
   ./otelcol-contrib --version
   ./otelcol-contrib components | grep oauth2client
   ```

3. Start the Collector with the example configuration. Copy the example, set the environment variables from the reference above, and run it:

   ```bash
   cp config.example.yaml collector.yaml
   export DATABRICKS_CLIENT_ID="<service-principal-uuid>"
   export DATABRICKS_CLIENT_SECRET="<client-secret>"
   export DATABRICKS_WORKSPACE_URL="https://<workspace-url>"
   export WORKSPACE_ID="<workspace-id>"
   export ZEROBUS_ENDPOINT="<workspace-id>.zerobus.<region>.cloud.databricks.com"
   export CATALOG="<catalog>"
   export SCHEMA="<schema>"
   export TABLE_PREFIX="<prefix>"
   ./otelcol-contrib --config collector.yaml
   ```

   Wait for `Everything is ready. Begin running and processing data.` The Collector also runs as the upstream container image, `otel/opentelemetry-collector-contrib:0.165.0`. The container runs as a non-root user, so the configuration file must be readable by it (`chmod 644`).

4. Install the app's dependencies and run it in another terminal. The app sends telemetry to the Collector on `127.0.0.1:4318`:

   ```bash
   pip install -r requirements.txt
   OPENLIT_OTLP_ENDPOINT=http://127.0.0.1:4318 python src/app.py --calls 10 --interval 1
   ```

   The app prints one reply for each call. The chat model returns canned replies in turn, so the output doesn't depend on the dice.

5. Query the tables. Spans appear after the Collector's batch timeout (5 seconds with this configuration). Metrics appear after the SDK's export interval, which is 60 seconds by default:

   ```sql
   SELECT time, service_name, name, kind
   FROM <catalog>.<schema>.<prefix>_otel_spans
   ORDER BY time DESC
   LIMIT 10;
   ```

   Each call produces one `chat FakeListChatModel` span with kind `SPAN_KIND_CLIENT`.

   ```sql
   SELECT name, histogram.count AS calls, histogram.sum AS total
   FROM <catalog>.<schema>.<prefix>_otel_metrics
   WHERE name = 'gen_ai.client.operation.duration';
   ```

To run the tests, install `requirements-dev.txt` and run `pytest tests`. The tests don't need a Collector or a workspace.

## Schema

The two tables come from `scripts/create_tables.sql`, which is the traces and metrics DDL from the configure page. Zerobus Ingest doesn't create tables, so create them first. You don't configure a mapping: the OTLP schema fixes the columns. The columns this example fills:

| Table | Column | Value from this example |
|---|---|---|
| Spans | `name`, `kind` | `chat FakeListChatModel`, `SPAN_KIND_CLIENT` |
| Spans | `attributes` | The `gen_ai.*` attributes from OpenLIT, such as `gen_ai.operation.name` (`chat`), `gen_ai.provider.name` (`langchain`), and `gen_ai.request.model` |
| Metrics | `name`, `histogram` | `gen_ai.client.operation.duration`, `gen_ai.client.token.usage` (one series for input and one for output tokens), `gen_ai.server.request.duration`, `gen_ai.server.time_to_first_token`, and `gen_ai.usage.cost` |
| All | `service_name` | The `application_name` passed to `openlit.init()` (`openlit-dice-app`) |
| All | `record_id`, `time` | A unique ID per row, and the telemetry time |

The fake model returns no token counts, so the token values are what OpenLIT records for the calls. Use a real model to see token counts from the provider.

## Logs

OpenLIT also exports events as OpenTelemetry logs. If `disable_events` is left at its default, the app sends logs to `/v1/logs` on the same endpoint. This configuration has no logs pipeline, so the Collector answers `404 Not Found`, and the app prints `Failed to export logs batch code: 404`. The example sets `disable_events=True` for that reason. To keep logs, add a `logs` pipeline with its own `oauth2client` extension, exporter and `<prefix>_otel_logs` table, following the OpenTelemetry Collector configuration.

## How to extend

- **A real model:** replace `FakeListChatModel` in `build_chain()` with a provider's chat model, such as `ChatOpenAI`. OpenLIT instruments the call the same way. Set that provider's API key in the app's environment. The Collector doesn't need it.
- **Other frameworks:** OpenLIT's `openlit.init()` instruments the frameworks it finds installed. Install the framework you use and keep the same init call. See the [OpenLIT integrations list](https://docs.openlit.io/latest/sdk/integrations).
- **Prompts and responses:** `openlit.init()` records message content on spans by default (`capture_message_content=True`). Set it to `False` to keep prompt and completion text out of the table.
- **Logs:** see Logs above.
- **Another table:** add an `oauth2client/…` extension with a token scoped to the new table, an `otlp_grpc/…` exporter with its `x-databricks-zerobus-table-name` header, and a pipeline that uses them. Create the table with the same DDL and grant the service principal access.
- **Your own application:** any OpenTelemetry SDK can send OTLP to the Collector's HTTP or gRPC receiver. Only the Collector holds credentials.
