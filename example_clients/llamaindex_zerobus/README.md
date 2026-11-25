# LlamaIndex to Zerobus Ingest: query-engine spans through the OpenTelemetry Collector

This example sends the OpenTelemetry spans from a LlamaIndex retrieval-augmented generation (RAG) app to a Delta table in Unity Catalog, through Zerobus Ingest. The app sends plain OTLP to a local OpenTelemetry Collector Contrib. The Collector's `oauth2client` extension gets an OAuth token for a service principal, refreshes it, and forwards the spans to Zerobus Ingest. The app needs no token code.

## Overview

LlamaIndex has its own OpenTelemetry package, `llama-index-observability-otel`. It turns each LlamaIndex operation into a span: a query call, the retriever, the embedding calls, the LLM call, and the synthesis step. Each span has a parent, so a query shows up as one tree.

Use this example when your RAG or query-engine app runs longer than one hour, so a static token would expire, or when you want one place to hold the service principal's secret. Spans go to one table, `<prefix>_otel_spans`. The LlamaIndex package emits spans only, so this example has no logs or metrics table.

The example app uses LlamaIndex's `MockLLM` and `MockEmbedding`, so it runs with no model API key. The span tree is the same kind a real LLM app produces.

**What lands in the table:** every span row has the LlamaIndex span name, the trace and span IDs, the parent span ID, and the service name. The payload is in the `events` column, not in `attributes`. Events include the embedding inputs and outputs, the retrieved text, and the LLM prompt. If your prompts or documents contain sensitive text, they go to the table too.

## Prerequisites

- A workspace in a region that supports Zerobus Ingest. See [Zerobus Ingest](https://docs.databricks.com/ingestion/zerobus-ingest) for the endpoint and the workspace URL.
- A Unity Catalog catalog and schema, with a managed Delta table for spans that meets the [Zerobus Ingest table requirements](https://docs.databricks.com/ingestion/zerobus-concepts#table-requirements). Create the table with `scripts/create_table.sql`.
- A service principal with an OAuth secret and the Workspace access entitlement. It needs `USE CATALOG`, `USE SCHEMA`, and `MODIFY` and `SELECT` on the spans table.
- [OpenTelemetry Collector Contrib](https://github.com/open-telemetry/opentelemetry-collector-releases/releases) with the `oauth2client` extension. Check that `otelcol-contrib components` lists it.
- Python 3.11 or later with `pip` (tested with 3.12).
- Docker is optional; the Quick start doesn't use it.

## Configuration reference

Set these variables where the Collector runs. The Collector config reads them with `${env:...}`.

| Variable | Value |
| :---- | :---- |
| `DATABRICKS_CLIENT_ID` | The service principal's application ID |
| `DATABRICKS_CLIENT_SECRET` | The service principal's OAuth secret |
| `DATABRICKS_WORKSPACE_URL` | Your workspace URL, `https://<workspace-url>` |
| `WORKSPACE_ID` | Your workspace ID, `<workspace-id>` |
| `ZEROBUS_ENDPOINT` | Your Zerobus Ingest endpoint host, without `https://` or the port |
| `CATALOG` | The Unity Catalog catalog that holds the table |
| `SCHEMA` | The schema that holds the table |
| `TABLE_PREFIX` | The prefix you used in `create_table.sql`. The table is `<prefix>_otel_spans` |

The Collector config is `config.example.yaml`. It has one OTLP receiver (gRPC on `127.0.0.1:4317`, HTTP on `127.0.0.1:4318`), one batch processor, one `oauth2client/spans` extension, and one traces pipeline to `otlp/spans`.

The app reads one variable:

| Variable | Default | Purpose |
| :---- | :---- | :---- |
| `OTLP_TRACES_ENDPOINT` | `http://127.0.0.1:4318/v1/traces` | Where the app sends spans |
| `OTEL_SERVICE_NAME` | `llamaindex-rag-demo` | The `service.name` on every span |

## Quick start

1. Create the table. In Databricks SQL, replace `<catalog>.<schema>.<prefix>` and `<service-principal-uuid>` in `scripts/create_table.sql`, then run it.

2. Install the Collector Contrib. Download the archive for your platform from the [OpenTelemetry Collector releases page](https://github.com/open-telemetry/opentelemetry-collector-releases/releases), compare its SHA-256 hash with the `.sha256` file next to it, and extract it. Then check the extension:

   ```bash
   ./otelcol-contrib --version
   ./otelcol-contrib components | grep oauth2client
   ```

3. Set the variables from the Configuration reference in the shell where you start the Collector, then start it:

   ```bash
   ./otelcol-contrib --config=config.example.yaml
   ```

   The log ends with `Everything is ready. Begin running and processing data.` The `"otlp" alias is deprecated` warning is expected; the config keeps the `otlp` exporter name.

4. Install the app's dependencies in a virtual environment:

   ```bash
   python3 -m venv .venv
   . .venv/bin/activate
   pip install -r requirements.txt
   ```

5. Run the app. It runs three query rounds and exits after it flushes the spans:

   ```bash
   OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces python3 src/app.py --queries 3
   ```

   Each round prints `round=N question=… answer_chars=…`, and the last line is `done rounds=3`.

6. Query the table. Rows can take a minute or two to appear:

   ```sql
   SELECT name, count(*) AS spans, count(DISTINCT trace_id) AS traces
   FROM <catalog>.<schema>.<prefix>_otel_spans
   WHERE service_name = 'llamaindex-rag-demo'
   GROUP BY name
   ORDER BY spans DESC;
   ```

   The root span is `RetrieverQueryEngine.query`. Its children include `VectorIndexRetriever.retrieve`, `MockEmbedding.get_query_embedding` and `MockLLM.complete`.

7. Run the offline tests. They don't need the Collector, a workspace, or a model key:

   ```bash
   pip install -r requirements-dev.txt
   python3 -m pytest -q tests
   ```

To use a real model, replace `MockLLM` and `MockEmbedding` in `build_query_engine()` with your model classes and install the matching LlamaIndex integration package.

## Schema

The spans table has the OpenTelemetry v2 span schema used by Zerobus Ingest. Step 1 creates it. `scripts/create_table.sql` has the full definition. The columns this app fills most often:

| Column | Type | What the app puts there |
| :---- | :---- | :---- |
| `record_id` | STRING | A unique ID for the record |
| `time` | TIMESTAMP | The span's start time |
| `date` | DATE | The date of `time` |
| `service_name` | STRING | `llamaindex-rag-demo` (or `OTEL_SERVICE_NAME`) |
| `trace_id` | STRING | The trace ID. A query and its child spans share it |
| `span_id` | STRING | The span ID |
| `parent_span_id` | STRING | The parent span's ID. Empty for a root span |
| `name` | STRING | The LlamaIndex span name, such as `RetrieverQueryEngine.query` |
| `kind` | STRING | `SPAN_KIND_INTERNAL` for every span here |
| `start_time_unix_nano`, `end_time_unix_nano` | LONG | Start and end times |
| `attributes` | VARIANT | Empty in this example. LlamaIndex puts its data in `events` |
| `events` | ARRAY of STRUCT | The LlamaIndex events. Each has a `name` (such as `LLMCompletionStartEvent`) and `attributes`, which include the prompt, the retrieved text and the embedding values |
| `status` | STRUCT | `STATUS_CODE_OK` for a span that completed |
| `resource` | STRUCT | `service.name` and other resource attributes |

## How to extend

- **Another LlamaIndex app:** call `configure_tracing()` once, before you build the index or the query engine, then run your code as usual. Every LlamaIndex call in the process is traced.
- **Another service name:** set `OTEL_SERVICE_NAME`, or pass `service_name=` to `configure_tracing()`.
- **Another exporter or Collector:** change `OTLP_TRACES_ENDPOINT`. The app's `OTLPSpanExporter` takes the same URL for any OTLP/HTTP receiver.
- **Logs or metrics:** LlamaIndex's OpenTelemetry package emits spans only. Logs and metrics need their own pipeline and their own tables (`<prefix>_otel_logs`, `<prefix>_otel_metrics`), with the table definitions and grants from the OpenTelemetry Collector page.
- **Bad records:** `scripts/send_bad_span.py` sends one valid span and one bad span to the Collector, and prints the response. Use it to test how your pipeline handles a bad record.

## Troubleshooting

- **The Collector logs `Exporting failed … DeadlineExceeded`:** the Collector retries the batch, and in this example the batch landed after the retry. If the error repeats, check that the Collector can reach the Zerobus Ingest endpoint on port 443.
- **The app logs `Failed to export spans batch due to timeout, max retries or shutdown` after you restart the Collector:** the app's span exporter retries for a limited time. Spans from the batch that failed are dropped. Keep the Collector running during the app's run, or restart the Collector between runs.
- **No rows, and the Collector logs a permission error:** check the grants in `scripts/create_table.sql`. The service principal needs `USE CATALOG`, `USE SCHEMA`, and `MODIFY` and `SELECT` on the spans table.
- **A request returns HTTP 400 with `length mismatch`:** one span in the request has an invalid trace or span ID. The whole request is rejected, and the valid spans in it aren't stored. `scripts/send_bad_span.py` reproduces this.
