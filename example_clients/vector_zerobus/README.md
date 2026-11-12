# Vector Zerobus: logs into Delta with the native sink

This example sends logs from [Vector](https://vector.dev/) straight into a Unity Catalog
Delta table using Vector's native
[`databricks_zerobus`](https://vector.dev/docs/reference/configuration/sinks/databricks_zerobus/)
sink and Zerobus Ingest.

There is no message bus, no collector tier, and no staging storage in between. Vector
parses and reshapes each log line, and the sink sends batches as Apache Arrow over Arrow
Flight to Zerobus, which writes them into the table. If you already run Vector as an agent
or aggregator, adding the lakehouse as a destination is one sink block.

The whole thing runs with `docker compose up`. The sample source generates syslog lines;
swap it for the source you already use.

```
┌──────────────────────────┐
│          Vector          │  demo_logs source (syslog lines), 1 event/s
│  remap: parse + redact   │  parse_syslog, add event_id, redact SSNs
└────────────┬─────────────┘
             │  Arrow Flight, OAuth as a service principal
             v
┌──────────────────────────┐
│      Zerobus Ingest      │  serverless, no broker
└────────────┬─────────────┘
             │
             v
┌──────────────────────────┐
│  Delta in Unity Catalog  │  <catalog>.<schema>.vector_logs
└──────────────────────────┘
```

---

## Overview

- **What it shows:** a working Vector pipeline (`vector/vector.yaml`) that parses syslog,
  adds an `event_id`, redacts US Social Security numbers, and writes to a Delta table.
- **Why the secrets backend:** since Vector 0.57.0, `${VAR}` interpolation in config files
  is off by default, so a value like `"${DATABRICKS_CLIENT_SECRET}"` reaches the sink as
  literal text. This example passes the endpoints and credentials through Vector's `exec`
  [secrets backend](https://vector.dev/docs/reference/configuration/secrets/)
  (`vector/env-secrets.sh`), which reads them from the container's environment.
- **Durability:** the sink uses a disk buffer, so events Vector has accepted survive a crash
  or a hard kill. The sink waits for Zerobus to acknowledge each batch before it reports
  the batch delivered. Delivery is at least once, so dedupe on `event_id`.
- **Bad records:** `tests/vector-dead-letter.yaml` shows how to route events that would
  break a batch to a dead-letter sink, so the good events still land.

Tested with Vector 0.61.0 (`timberio/vector:0.61.0-debian`).

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

### 1. Create the table and grant access

Open `scripts/create_table.sql`, replace `<catalog>.<schema>` and
`<service-principal-uuid>`, and run it in Databricks SQL. Zerobus doesn't create tables.

### 2. Configure

```bash
cp config.example.env .env
```

Fill in `.env` (see [Configuration reference](#configuration-reference)). Then set
`table_name` in `vector/vector.yaml` to the table you created:

```yaml
    table_name: main.default.vector_logs   # your catalog.schema.table
```

### 3. Start the pipeline

```bash
docker compose up -d
docker compose logs -f vector
```

Within a few seconds Vector logs:

```
INFO vector::sinks::databricks_zerobus::unity_catalog_schema: Inferred Arrow schema from Unity Catalog table <catalog>.<schema>.vector_logs
INFO databricks_zerobus_ingest_sdk::stream::arrow: Arrow Flight stream created successfully
INFO vector::topology::builder: Healthcheck passed.
```

A warning, `HeadersProvider attempted to set reserved header 'x-databricks-zerobus-table-name', ignoring`, also appears at startup. It's harmless.

### 4. Watch the data arrive

```sql
SELECT timestamp, host, service, severity, message
FROM <catalog>.<schema>.vector_logs
ORDER BY timestamp DESC
LIMIT 10;
```

A new row arrives about every second. In our test run, 46 rows landed in 45 seconds,
each with a distinct `event_id`.

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
| `DATABRICKS_WORKSPACE_URL` | Yes | Workspace URL, with `https://`. The sink uses it for OAuth tokens and to read the table's schema from Unity Catalog |
| `DATABRICKS_CLIENT_ID` | Yes | Service principal application ID |
| `DATABRICKS_CLIENT_SECRET` | Yes | Service principal OAuth secret |

`vector/vector.yaml`, the sink options this example sets:

| Option | Value here | Description |
|---|---|---|
| `ingestion_endpoint` | `SECRET[env.zerobus_endpoint]` | From `ZEROBUS_SERVER_ENDPOINT` via the secrets backend |
| `unity_catalog_endpoint` | `SECRET[env.workspace_url]` | From `DATABRICKS_WORKSPACE_URL` |
| `table_name` | `main.default.vector_logs` | Your `catalog.schema.table` |
| `auth.strategy` | `oauth` | OAuth client credentials; the sink gets and refreshes tokens |
| `auth.client_id`, `auth.client_secret` | `SECRET[env.client_id]`, `SECRET[env.client_secret]` | From `.env` |
| `buffer.type`, `buffer.max_size` | `disk`, `268435488` | Disk buffer (minimum ~256 MB) under `data_dir: /var/lib/vector`, a named volume in Compose |

Other options worth knowing (see the
[sink reference](https://vector.dev/docs/reference/configuration/sinks/databricks_zerobus/)):
`stream_options.compression` (`none`, `lz4_frame`, `zstd`), `batch.max_bytes` and
`batch.timeout_secs` (defaults 10 MB and 1 s), and `user_agent`.

## Schema

`scripts/create_table.sql` creates:

| Column | Type | Set by (`shape_for_table` transform) |
|---|---|---|
| `event_id` | `STRING` | `uuid_v4()`, a unique ID per event, used for dedupe |
| `timestamp` | `TIMESTAMP` | The syslog line's timestamp |
| `host` | `STRING` | Syslog hostname |
| `service` | `STRING` | Syslog app name |
| `severity` | `STRING` | Syslog severity, such as `info` or `err` |
| `message` | `STRING` | The message text, with US Social Security numbers redacted |

How events map to the table:

- Top-level event fields map to columns by name.
- A field with no matching column isn't sent, and a column with no matching field is written as `NULL`.
- The sink reads the schema from Unity Catalog at startup, so there's no schema in the Vector config.
- Keep columns nullable. If a column is `NOT NULL` and one event in a batch is missing it, the sink drops the **whole batch** and doesn't retry it.
- The sink doesn't support `VARIANT` columns.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `endpoint '${…}' is not a valid URI` | `${VAR}` interpolation is off since Vector 0.57.0. Use the secrets backend, as `vector/vector.yaml` does |
| `Health check … Schema resolution failed: Unity Catalog API returned error …` | `DATABRICKS_WORKSPACE_URL` must be your workspace URL with `https://`; check the table name and grants |
| `Schema constraint violation … Null value for non-nullable field '<column>'` and `Events dropped … count=<N>` | A `NOT NULL` column is missing on at least one event, so the whole batch was dropped. Make it nullable, set it in `remap`, or use the dead-letter route below |
| Health check fails but Vector keeps running | That's Vector's default. Start Vector with `--require-healthy` to exit instead |
| Duplicate rows | Delivery is at least once. Dedupe on `event_id`, for example with `ROW_NUMBER()` in a view |

## How to extend

- **Use your own logs.** Replace the `demo_logs` source with `file`, `kubernetes_logs`,
  `syslog` or any other Vector source, then change `shape_for_table` so its fields match your
  table's columns.
- **Add columns.** Add the column to the table first, then set the field in `remap`. Fields
  with no column are skipped, so the order doesn't break the pipeline.
- **Route bad events to a dead letter.** `tests/vector-dead-letter.yaml` checks a required
  field with `abort`, sets `drop_on_abort: true` and `reroute_dropped: true`, and sends
  `parse_json.dropped` to a `console` sink. Swap `console` for `file` to keep them.
  `./tests/run_tests.sh --live` runs it against your table: two rows land and one event
  prints as dead-lettered.
- **Send to more than one table.** Add a second `databricks_zerobus` sink with its own
  `table_name`, fed by a `route` or `filter` transform.
- **Tune throughput.** Raise `batch.max_bytes` (keep it at or below 10 MB) or set
  `stream_options.compression: zstd` to cut network egress.

## File reference

| File | Purpose |
|---|---|
| `docker-compose.yml` | Runs Vector 0.61.0 with the config and a disk-buffer volume |
| `config.example.env` | Template for `.env` (endpoints and service principal credentials) |
| `vector/vector.yaml` | The pipeline: source, `remap` transform, Zerobus sink |
| `vector/env-secrets.sh` | `exec` secrets backend that serves `.env` values to the config |
| `scripts/create_table.sql` | Target table and grants |
| `tests/run_tests.sh` | `vector validate` for both configs; `--live` runs the dead-letter test |
| `tests/vector-dead-letter.yaml`, `tests/bad-records.jsonl` | Dead-letter example and its input |

## Reference

- [Vector `databricks_zerobus` sink](https://vector.dev/docs/reference/configuration/sinks/databricks_zerobus/)
- [Vector secrets](https://vector.dev/docs/reference/configuration/secrets/)
- [Vector Remap Language (VRL)](https://vector.dev/docs/reference/vrl/)
- [Zerobus Ingest overview](https://docs.databricks.com/ingestion/zerobus-overview)
- [Zerobus Ingest limits](https://docs.databricks.com/ingestion/zerobus-limits)

## License

&copy; 2026 Databricks, Inc. All rights reserved. The source in this example is provided
subject to the [Databricks License](https://databricks.com/db-license-source).
