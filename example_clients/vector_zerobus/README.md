# Vector Zerobus: an app's logs, metrics and traces into Delta with the native sink

This example sends an app's OpenTelemetry logs, metrics and traces through
[Vector](https://vector.dev/) straight into Unity Catalog Delta tables, using Vector's native
[`databricks_zerobus`](https://vector.dev/docs/reference/configuration/sinks/databricks_zerobus/)
sink and Zerobus Ingest.

There is no message bus, no collector tier, and no staging storage in between. The app is a
small FastAPI dice roller instrumented with the OpenTelemetry SDK. Vector receives its OTLP
data, shapes each signal to match a table, and the sink sends batches as Apache Arrow over
Arrow Flight to Zerobus, which writes them into the tables. If you already run Vector as an
agent or aggregator, adding the lakehouse as a destination is one sink block per table.

The whole thing runs with `docker compose up`.

```
┌──────────────────────────┐
│   dice-app (FastAPI)     │  GET /rolldice → 1 log + 1 span + 1 counter
│   OpenTelemetry SDK      │  traffic: a curl loop rolls once a second
└────────────┬─────────────┘
             │  OTLP/gRPC :4317
             v
┌──────────────────────────┐
│          Vector          │  opentelemetry source
│  logs ── remap ──────────┼─┐
│  metrics ─ metric_to_log │ │  the native sink takes log events, so metrics
│  traces ── local loop ───┼─┤  and traces are turned into log events first
└──────────────────────────┘ │
             ┌───────────────┘  3 databricks_zerobus sinks, Arrow Flight, OAuth
             v
┌──────────────────────────┐
│      Zerobus Ingest      │  serverless, no broker
└────────────┬─────────────┘
             v
┌──────────────────────────┐
│  Delta in Unity Catalog  │  dice_logs · dice_metrics · dice_traces
└──────────────────────────┘
```

---

## Overview

- **What it shows:** a working Vector pipeline (`vector/vector.yaml`) that receives OTLP from
  an app (`app/app.py`) and writes each signal to its own Delta table:
  - **Logs** (`dice_logs`): each "ada rolled a 4" record with its severity, player, result,
    and the trace and span IDs it was logged under.
  - **Metrics** (`dice_metrics`): the `dice.rolls` counter, one row per result per export.
    Vector's `metric_to_log` transform turns metric events into log events for the sink.
  - **Traces** (`dice_traces`): one row per `roll` span with its timing. The native sink
    accepts log events only, and Vector rejects connecting trace events to it, so they take
    a local loop: an `http` sink sends them as JSON to an `http_server` source in the same
    Vector, which receives them as log events.
- **Why the secrets backend:** since Vector 0.57.0, `${VAR}` interpolation in config files
  is off by default, so a value like `"${DATABRICKS_CLIENT_SECRET}"` reaches the sink as
  literal text. This example passes the endpoints and credentials through Vector's `exec`
  [secrets backend](https://vector.dev/docs/reference/configuration/secrets/)
  (`vector/env-secrets.sh`), which reads them from the container's environment.
- **Durability:** each sink uses a disk buffer, so events Vector has accepted survive a crash
  or a forced stop. The sink waits for Zerobus to acknowledge each batch before it reports
  the batch delivered. Each event can be delivered one or more times, so dedupe on `event_id` (logs, metrics)
  or `span_id` (traces).
- **Bad records:** `tests/vector-dead-letter.yaml` shows how to route events that would
  break a batch to a dead-letter sink, so the good events still land.

Tested with Vector 0.61.0 (`timberio/vector:0.61.0-debian`) and the OpenTelemetry Python
SDK 1.33.1.

## Prerequisites

- Docker with Compose v2.
- A Databricks workspace in a region that supports
  [Zerobus Ingest](https://docs.databricks.com/ingestion/zerobus-overview), with Unity Catalog.
- A service principal with an OAuth secret
  ([how to create one](https://docs.databricks.com/ingestion/zerobus-ingest#service-principal)).
  Note its application ID and secret.
- Your Zerobus Ingest endpoint and workspace URL
  ([where to find them](https://docs.databricks.com/ingestion/zerobus-ingest#get-your-workspace-url-and-zerobus-ingest-endpoint)).
- Network access from the Vector host to both on port 443.

## Quick start

### 1. Create the tables and grant access

Open `scripts/create_table.sql`, replace `<catalog>.<schema>` and
`<service-principal-uuid>`, and run it in Databricks SQL. Zerobus doesn't create tables.

### 2. Configure

```bash
cp config.example.env .env
```

Fill in `.env` (see [Configuration reference](#configuration-reference)). Then set
`table_name` on the three sinks in `vector/vector.yaml` to the tables you created:

```yaml
    table_name: main.default.dice_logs      # your catalog.schema.table
    table_name: main.default.dice_metrics   # your catalog.schema.table
    table_name: main.default.dice_traces    # your catalog.schema.table
```

### 3. Start the pipeline

```bash
docker compose up -d --build
docker compose logs -f vector
```

Typically within a few seconds, Vector logs one line per sink:

```output
INFO databricks_zerobus_ingest_sdk::stream::arrow: Arrow Flight stream created successfully table_name=<catalog>.<schema>.dice_logs
INFO vector::topology::builder: Healthcheck passed.
```

A warning, `HeadersProvider attempted to set reserved header 'x-databricks-zerobus-table-name', ignoring`, also appears at startup. It's harmless.

### 4. Watch the data arrive

```sql
-- Each roll, with the span it was logged under
SELECT l.timestamp, l.player, l.result, l.message, t.duration_ms
FROM <catalog>.<schema>.dice_logs l
JOIN <catalog>.<schema>.dice_traces t USING (trace_id, span_id)
ORDER BY l.timestamp DESC
LIMIT 10;

-- How often each result came up (cumulative counter, latest export)
SELECT result, max(value) AS rolls
FROM <catalog>.<schema>.dice_metrics
GROUP BY result
ORDER BY result;
```

A roll happens about every second, and the counter exports every 10 seconds. In our test
run, logs, metrics and traces all landed within a minute, and the log rows joined to
their spans on `trace_id` and `span_id`.

To roll by hand: `curl "http://localhost:8000/rolldice?player=you"`.

### 5. Stop

```bash
docker compose down        # keeps the disk buffer volume
docker compose down -v     # also deletes it
```

A normal stop sends the events Vector is holding before it exits.

## Configuration reference

`.env` (copied from `config.example.env`; gitignored):

| Variable | Required | Description |
|---|---|---|
| `ZEROBUS_SERVER_ENDPOINT` | Yes | Zerobus Ingest endpoint, with `https://`, e.g. `https://<workspace-id>.zerobus.<region>.cloud.databricks.com` |
| `DATABRICKS_WORKSPACE_URL` | Yes | Workspace URL, with `https://`, e.g. `https://<workspace-url>`. The sink uses it for OAuth tokens and to read each table's schema from Unity Catalog |
| `DATABRICKS_CLIENT_ID` | Yes | Service principal application ID |
| `DATABRICKS_CLIENT_SECRET` | Yes | Service principal OAuth secret |

`docker-compose.yml` sets the app's `OTEL_SERVICE_NAME` (`dice-roller`) and
`OTEL_EXPORTER_OTLP_ENDPOINT` (`http://vector:4317`).

`vector/vector.yaml`, the sink options this example sets on each of the three sinks:

| Option | Value here | Description |
|---|---|---|
| `ingestion_endpoint` | `SECRET[env.zerobus_endpoint]` | From `ZEROBUS_SERVER_ENDPOINT` via the secrets backend |
| `unity_catalog_endpoint` | `SECRET[env.workspace_url]` | From `DATABRICKS_WORKSPACE_URL` |
| `table_name` | `main.default.dice_logs`, `…dice_metrics`, `…dice_traces` | Your `catalog.schema.table` |
| `auth.strategy` | `oauth` | OAuth client credentials; the sink gets and refreshes tokens |
| `auth.client_id`, `auth.client_secret` | `SECRET[env.client_id]`, `SECRET[env.client_secret]` | From `.env` |
| `buffer.type`, `buffer.max_size` | `disk`, `268435488` | Disk buffer (minimum ~256 MB) under `data_dir: /var/lib/vector`, a named volume in Compose |

Other options worth knowing (see the
[sink reference](https://vector.dev/docs/reference/configuration/sinks/databricks_zerobus/)):
`stream_options.compression` (`none`, `lz4_frame`, `zstd`), `batch.max_bytes` and
`batch.timeout_secs` (defaults 10 MB and 1 s), and `user_agent`.

## Schema

`scripts/create_table.sql` creates three tables. Each column is set by the matching
`remap` transform in `vector/vector.yaml`.

`dice_logs` (`shape_logs`, from `otel.logs`):

| Column | Type | Set from |
|---|---|---|
| `event_id` | `STRING` | `uuid_v4()`, a unique ID per event, used for dedupe |
| `timestamp` | `TIMESTAMP` | The log record's timestamp |
| `service` | `STRING` | Resource attribute `service.name` |
| `severity` | `STRING` | `severity_text`, such as `INFO` or `WARN` (a roll of six logs a warning) |
| `message` | `STRING` | The log message, such as `ada rolled a 4` |
| `player`, `result` | `STRING`, `INT` | Log attributes `dice.player` and `dice.result` |
| `trace_id`, `span_id` | `STRING` | The span the record was logged under |

`dice_metrics` (`shape_metrics`, from `otel.metrics` through `metric_to_log`):

| Column | Type | Set from |
|---|---|---|
| `event_id` | `STRING` | `uuid_v4()` |
| `timestamp` | `TIMESTAMP` | The data point's timestamp |
| `service` | `STRING` | Tag `resource.service.name` |
| `metric` | `STRING` | Metric name, `dice.rolls` |
| `kind` | `STRING` | `absolute` (a cumulative counter) or `incremental` |
| `value` | `DOUBLE` | Counter value (or gauge value for gauges) |
| `result` | `STRING` | Tag `dice.result` |

`dice_traces` (`shape_traces`, from `otel.traces` through the local loop):

| Column | Type | Set from |
|---|---|---|
| `trace_id`, `span_id`, `parent_span_id` | `STRING` | Span IDs; `parent_span_id` is `NULL` for a root span |
| `name` | `STRING` | Span name, `roll` |
| `start_time`, `end_time` | `TIMESTAMP` | Span start and end |
| `duration_ms` | `DOUBLE` | `end_time` minus `start_time` |
| `service` | `STRING` | Resource attribute `service.name` |
| `player`, `result` | `STRING`, `INT` | Span attributes `dice.player` and `dice.result` |
| `status_code` | `INT` | Span status code (0 = unset, 1 = OK, 2 = error) |

How events map to a table:

- Top-level event fields map to columns by name.
- A field with no matching column isn't sent, and a column with no matching field is written as `NULL`.
- The sink reads the schema from Unity Catalog at startup, so there's no schema in the Vector config.
- Keep columns nullable. If a column is `NOT NULL` and one event in a batch is missing it, the sink drops the whole batch and doesn't retry it.
- The sink doesn't support `VARIANT` columns.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `endpoint '${…}' is not a valid URI` | `${VAR}` interpolation is off since Vector 0.57.0. Use the secrets backend, as `vector/vector.yaml` does |
| `Health check … Schema resolution failed: Unity Catalog API returned error …` | `DATABRICKS_WORKSPACE_URL` must be your workspace URL with `https://`; check the table names and grants |
| `Data type mismatch between otel.traces (["Trace"]) and <sink> (["Log"])` | The native sink takes log events only. Send traces through the local loop (`traces_to_logs` → `traces_as_logs`) as this config does |
| `Schema constraint violation … Null value for non-nullable field '<column>'` and `Events dropped … count=<N>` | A `NOT NULL` column is missing on at least one event, so the whole batch was dropped. Make it nullable, set it in `remap`, or use the dead-letter route below |
| The app logs `Transient error StatusCode.UNAVAILABLE … exporting … to …:4317, retrying in <N>s` | Vector isn't reachable. The OpenTelemetry exporter retries with backoff (up to 32 s here) and resumes on its own after a short outage; events in flight when Vector stops can be lost |
| Health check fails but Vector keeps running | That's Vector's default. Start Vector with `--require-healthy` to exit instead |
| Duplicate rows | Each event can be delivered one or more times. Dedupe on `event_id` or `span_id`, for example with `ROW_NUMBER()` in a view |

## How to extend

- **Use your own app:** Point any OpenTelemetry SDK or Collector at Vector's
  `opentelemetry` source (`:4317` gRPC, `:4318` HTTP), then change the `shape_*` transforms
  so their fields match your tables' columns. Keep the attributes you query by as columns.
- **Add columns:** Add the column to the table first, then set the field in `remap`. Fields
  with no column are skipped, so the order doesn't break the pipeline.
- **Route bad events to a dead letter:** `tests/vector-dead-letter.yaml` checks a required
  field with `abort`, sets `drop_on_abort: true` and `reroute_dropped: true`, and sends
  `parse_json.dropped` to a `console` sink. Swap `console` for `file` to keep them.
  `./tests/run_tests.sh --live` runs it against `dice_logs`: two rows land and one event
  prints as dead-lettered.
- **Send to more tables:** Add another `databricks_zerobus` sink with its own `table_name`,
  fed by a `route` or `filter` transform, for example one table per service.
- **Tune throughput:** Raise `batch.max_bytes` (keep it at or below 10 MB) or set
  `stream_options.compression: zstd` to cut network egress.

## File reference

| File | Purpose |
|---|---|
| `docker-compose.yml` | Runs the dice app, a traffic loop, and Vector 0.61.0 with a disk-buffer volume |
| `app/app.py`, `app/requirements.txt`, `app/Dockerfile` | The FastAPI dice roller, instrumented with the OpenTelemetry SDK (logs, metrics, traces over OTLP/gRPC) |
| `config.example.env` | Template for `.env` (endpoints and service principal credentials) |
| `vector/vector.yaml` | The pipeline: `opentelemetry` source, `remap` and `metric_to_log` transforms, the trace loop, three Zerobus sinks |
| `vector/env-secrets.sh` | `exec` secrets backend that serves `.env` values to the config |
| `scripts/create_table.sql` | Target tables and grants |
| `tests/run_tests.sh` | `vector validate` for both configs; `--live` runs the dead-letter test |
| `tests/vector-dead-letter.yaml`, `tests/bad-records.jsonl` | Dead-letter example and its input |

## Reference

- [Vector `databricks_zerobus` sink](https://vector.dev/docs/reference/configuration/sinks/databricks_zerobus/)
- [Vector `opentelemetry` source](https://vector.dev/docs/reference/configuration/sources/opentelemetry/)
- [Vector `metric_to_log` transform](https://vector.dev/docs/reference/configuration/transforms/metric_to_log/)
- [Vector secrets](https://vector.dev/docs/reference/configuration/secrets/)
- [Vector Remap Language (VRL)](https://vector.dev/docs/reference/vrl/)
- [OpenTelemetry Python](https://opentelemetry.io/docs/languages/python/)
- [Zerobus Ingest overview](https://docs.databricks.com/ingestion/zerobus-overview)
- [Zerobus Ingest limits](https://docs.databricks.com/ingestion/zerobus-limits)

## License

&copy; 2026 Databricks, Inc. All rights reserved. The source in this example is provided
subject to the [Databricks License](https://databricks.com/db-license-source).
