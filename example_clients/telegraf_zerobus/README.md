# Telegraf Zerobus: host and app metrics into Delta with Telegraf's OpenTelemetry output

This example sends Telegraf metrics to a Unity Catalog Delta table through Zerobus Ingest. Telegraf's `opentelemetry` output posts them to the Zerobus Ingest OTLP metrics endpoint, which writes each one as a row. A small FastAPI dice-roll app adds application metrics: every roll is one point that Telegraf receives over HTTP and forwards with the host's cpu and memory metrics.

## Overview

Telegraf collects metrics from hosts, services, and applications. Its OpenTelemetry output sends them over OTLP/HTTP with Protobuf encoding. Zerobus Ingest's OTLP endpoint writes them into a metrics table with the OpenTelemetry metrics schema. There's no collector tier or message bus between Telegraf and the lakehouse, so you no longer need to run one. Telegraf also refreshes the service principal's OAuth token itself, through its `oauth2` secret store, so the config holds no static token.

Use this example when you already run Telegraf and want its metrics in Delta tables. The dice-roll app shows how an application can report through Telegraf without an SDK: it writes line protocol to Telegraf's `http_listener_v2` input.

Files:

- `config.example.toml`: the Telegraf configuration, with placeholders.
- `src/dice_app.py`: the FastAPI dice-roll app.
- `scripts/create_table.sql`: the metrics table.
- `scripts/roll_dice.sh`: rolls the die through the app every second.
- `tests/test_dice_app.py`: unit tests for the app and the config.

## Prerequisites

- A Databricks workspace in a region that supports Zerobus Ingest, with Unity Catalog. Querying the VARIANT columns needs Databricks Runtime 15.3 or above.
- A service principal with an OAuth secret. Grant it `USE CATALOG` on the catalog, `USE SCHEMA` on the schema, and `MODIFY` and `SELECT` on the table. `ALL PRIVILEGES` isn't enough. See [Zerobus Ingest OpenTelemetry configuration](https://docs.databricks.com/aws/en/ingestion/opentelemetry/configure).
- Telegraf 1.28.0 and above for the `oauth2` secret store, and 1.20.0 and above for the OpenTelemetry output. This example was tested with Telegraf 1.40.3. See [Telegraf's install page](https://docs.influxdata.com/telegraf/v1/install/).
- Python 3.12 for the dice-roll app. This example was tested with Python 3.12.

## Configuration reference

Set these values in `config.example.toml` and in your shell. Replace every placeholder.

| Placeholder or variable | What it is |
|---|---|
| `<workspace-url>` | Your workspace URL, for example `https://<workspace-url>`. The `oauth2` store gets tokens from `https://<workspace-url>/oidc/v1/token`. |
| `<workspace-id>` | Your workspace ID. It appears in the `resource` and the Zerobus Ingest endpoint. |
| `<region>` | The region of your workspace, for example `us-west-2`. |
| `<catalog>`, `<schema>`, `<table>` | The Unity Catalog table that receives the metrics. Create it first (step 1). |
| `<service-name>` | The value of the `service.name` resource attribute. It fills the table's `service_name` column. |
| `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` | The service principal's application ID and OAuth secret. Telegraf reads them from the environment, so the secret stays out of the file. |
| `TELEGRAF_URL` | The app's line-protocol target. The default is `http://127.0.0.1:8186/telegraf`, which matches the `http_listener_v2` input. |
| `DICE_APP_HOST` | The `host` tag on each roll. The default is the machine's hostname. |

The output block:

- `service_address` is the Zerobus Ingest endpoint with `/v1/metrics`, for example `https://<workspace-id>.zerobus.<region>.cloud.databricks.com/v1/metrics`. On Azure or GCP, use the endpoint for your cloud.
- `token = "@{zbauth:zbtoken}"` sends the token from the `oauth2` store as the `Authorization` header.
- `x-databricks-zerobus-table-name` names the one table that receives every metric from this output. Each request targets a single table.

## Quick start

1. Create the table and grant access. Replace `<catalog>.<schema>.<table>` in `scripts/create_table.sql` and run it in a SQL editor or with the Databricks CLI. Then grant the service principal the privileges in Prerequisites.

   ```sql
   GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-uuid>`;
   GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-uuid>`;
   GRANT MODIFY, SELECT ON TABLE <catalog>.<schema>.<table> TO `<service-principal-uuid>`;
   ```

2. Install Telegraf. On Debian or Ubuntu, install the `.deb` package from [Telegraf's install page](https://docs.influxdata.com/telegraf/v1/install/):

   ```bash
   sudo dpkg -i telegraf_<version>_amd64.deb
   telegraf --version
   ```

3. Configure Telegraf. Copy the example and replace the placeholders:

   ```bash
   sudo cp config.example.toml /etc/telegraf/telegraf.conf
   export DATABRICKS_CLIENT_ID="<service-principal-uuid>"
   export DATABRICKS_CLIENT_SECRET="<client-secret>"
   telegraf --config /etc/telegraf/telegraf.conf --test
   ```

4. Start Telegraf.

   ```bash
   telegraf --config /etc/telegraf/telegraf.conf
   ```

5. Start the dice-roll app in another terminal:

   ```bash
   pip install -r requirements.txt
   cd src && uvicorn dice_app:app --host 127.0.0.1 --port 8000
   ```

6. Roll the die. Ten rolls for the player `demo`, one a second:

   ```bash
   scripts/roll_dice.sh 10 demo
   ```

7. Query the table. Rows appear after the next flush, which is 5 seconds with this config:

   ```sql
   SELECT time, name, gauge.value, to_json(gauge.attributes) AS attributes, service_name
   FROM <catalog>.<schema>.<table>
   WHERE name = 'dice_roll_value'
   ORDER BY time DESC
   LIMIT 5;
   ```

   Each row has the roll as `gauge.value`, and `player` and `host` as attributes.

To run the tests, run `pytest tests` in a Python environment with `requirements.txt` installed.

## Schema

The table is the OpenTelemetry metrics table from `scripts/create_table.sql`. Zerobus Ingest creates no tables, so you create it first. Its columns are fixed by the OTLP schema, and you don't configure a mapping in Telegraf. The columns this example fills:

| Column | Value from this example |
|---|---|
| `record_id` | A unique ID for each row |
| `time`, `time_unix_nano` | The timestamp of each point. Telegraf sets it when it collects the metric |
| `service_name` | The `service.name` attribute from `config.example.toml` |
| `name` | The metric name. Each Telegraf numeric field becomes one name, `<measurement>_<field>`: `dice_roll_value` for the dice rolls, and names such as `cpu_usage_user` and `mem_used_percent` for the host metrics |
| `metric_type` | `gauge` for every row in this example |
| `gauge.value` | The numeric value. Integer fields are written as DOUBLE |
| `gauge.attributes` | The Telegraf tags as VARIANT, for example `{"player":"demo","host":"..."}` |
| `resource`, `instrumentation_scope` | The resource and scope fields the output fills, as the table reference describes them |

In testing, a string field landed no row and no `400` was logged. The example sends only numeric fields.

## Troubleshooting

- Telegraf logs `Bad Request (400)` on every flush, and no new metrics appear. The endpoint rejected a whole batch because one point in it has a timestamp it doesn't accept, such as timestamp 0 (the Unix epoch). Telegraf retries the same batch, so metrics behind it wait. Find the point's source, or stop Telegraf to end the retries. The reason isn't in Telegraf's log.
- Metrics are missing after a crash or `kill -9`. Telegraf holds unsent metrics in memory by default. A SIGKILL loses up to one flush interval of them. Stop Telegraf with SIGTERM (for example `kill <pid>`), which flushes what it holds.
- `service_name` is NULL. Set `service.name` under `[outputs.opentelemetry.attributes]`, as in `config.example.toml`.
- The app returns 503 or 502. Telegraf isn't listening on the port in `TELEGRAF_URL`, or it answered with an error. Check Telegraf's log.

## How to extend

- **More host metrics:** add any Telegraf input, such as `[[inputs.disk]]`. The table's schema doesn't change, because each numeric field becomes a metric name.
- **More application metrics:** write line protocol from any app to `http_listener_v2`, or use another Telegraf input. Keep the field values numeric.
- **Another table:** change `x-databricks-zerobus-table-name`, create the table with the same DDL, and grant the service principal on it. Each output sends to one table, so add a second `[[outputs.opentelemetry]]` block with its own header to write to a second table.
- **More attributes:** add entries under `[outputs.opentelemetry.attributes]`, for example `"deployment.environment" = "dev"`. They become resource attributes on every metric.
- **Your own app instead of the dice roll:** replace `src/dice_app.py`. Keep the line-protocol point shape: a measurement, tags for the dimensions you want to filter by, and numeric fields.
