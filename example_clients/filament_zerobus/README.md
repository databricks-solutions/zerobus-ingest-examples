# Filament and Zerobus Ingest: PostgreSQL tables into Delta with Filament's Zerobus sink

This example replicates two PostgreSQL tables into Unity Catalog Delta tables through
[Zerobus Ingest](https://docs.databricks.com/ingestion/zerobus-overview), using the native
`databrickszerobus` sink in [Filament](https://github.com/galaxy-io/filament), an open-source data
replication server. `orders` is copied in full on every run. `payments` is replicated with change data
capture (CDC), so every insert, update and delete in PostgreSQL becomes a row in a change history table.

Filament runs as three services (a server, a control plane and a worker) with PostgreSQL as its datastore
and NATS JetStream as its event bus. This folder runs all of them with `docker compose up`, together with
a sample `shop` database. The sink only exists in the worker's `-libc` image, so the worker is that image.

```
┌──────────────────────────────┐
│  PostgreSQL, database shop   │  orders (full copy), payments (CDC, wal_level=logical)
└──────────────┬───────────────┘
               │ read by the worker
               v
┌──────────────────────────────┐
│  Filament                    │  server · control plane · worker (-libc image)
│  PostgreSQL datastore        │  NATS JetStream holds run events
│  NATS JetStream              │
└──────────────┬───────────────┘
               │  databrickszerobus sink, Arrow Flight, OAuth
               v
┌──────────────────────────────┐
│  Zerobus Ingest              │
└──────────────┬───────────────┘
               v
┌──────────────────────────────┐
│  Delta in Unity Catalog      │  orders · payments
└──────────────────────────────┘
```

---

## Overview

- **What it shows:** a working Filament deployment (`docker-compose.yml`) with two pipelines, created by
  `scripts/setup.sh`:
  - **`orders-to-zb`**: `full` sync and `append` write. Every run sends all 20 rows to `orders`.
  - **`payments-cdc-to-zb`**: `cdc` sync and `append` write. The first run takes a snapshot of the 10 rows
    in `payments`. Later runs send the inserts, updates and deletes since the last run. The table keeps
    each change as a row, with `_filament_operation` set to `insert`, `update` or `delete`.
- **Lineage columns:** Filament adds `_filament_run_id` and `_filament_run_started_at` to every table, and
  the CDC columns to `payments`. The Delta tables in `scripts/create_table.sql` have them after the source
  columns.
- **Delivery:** Zerobus Ingest can deliver a row more than one time, and a failed run can leave rows behind.
  A full append resends every row on every run, so deduplicate on `id`. CDC keys repeat by design. See
  [Handle duplicates](#handle-duplicates).
- **Credentials:** the sink authenticates with a service principal's OAuth client ID and secret. The
  secret is read from `.env` and isn't written into the compose file.

Tested with Filament 0.42.0 (the `server`, `control-plane` and `worker:0.42.0-libc` images, and the
`filament` CLI 0.42.0), PostgreSQL 16 and NATS 2, against a Databricks workspace on AWS in `us-west-2`.

## Prerequisites

- Docker with Compose v2, and outbound HTTPS from the host to the workspace and the Zerobus Ingest endpoint.
- The `filament` CLI 0.42.0 on your PATH, from the
  [v0.42.0 release page](https://github.com/galaxy-io/filament/releases/tag/v0.42.0). `scripts/setup.sh` and
  the `filament run` commands use it.
- A Databricks workspace in a region that supports
  [Zerobus Ingest](https://docs.databricks.com/ingestion/zerobus-overview), with Unity Catalog and a SQL
  warehouse.
- A service principal with the Workspace access entitlement and an OAuth secret with the `all-apis`
  scope. Note its application ID and secret. See
  [Zerobus Ingest service principal setup](https://docs.databricks.com/ingestion/zerobus-ingest#service-principal).
- Your Zerobus Ingest endpoint and workspace URL. See
  [where to find them](https://docs.databricks.com/ingestion/zerobus-ingest#get-your-workspace-url-and-zerobus-ingest-endpoint).
- Permission to create tables in one Unity Catalog schema, and to grant the service principal access to it.

## Configuration reference

`.env` (copied from `config.example.env`, and gitignored):

| Variable | Required | Description |
|---|---|---|
| `ENCRYPTION_KEY` | Yes | Filament's secret key: 32 random bytes in base64 (`openssl rand -base64 32`). Keep it, because changing it makes Filament's stored secrets unreadable |
| `POSTGRES_PASSWORD` | Yes | Password for the PostgreSQL user `filament`, which owns Filament's `filament` datastore and the `shop` source database |
| `SOURCE_DSN` | Yes | The source database's connection string as the worker sees it. In Compose, the host is `postgres`: `postgres://filament:<postgres-password>@postgres:5432/shop?sslmode=disable` |
| `TABLE_PREFIX` | No | Prefix for the source and Delta table names. Leave it empty to use `orders` and `payments` |
| `DATABRICKS_WORKSPACE_URL` | Yes | Workspace URL, with `https://`, e.g. `https://<workspace-url>`. The sink uses it for OAuth tokens, and Filament requires `https://` |
| `ZEROBUS_ENDPOINT` | Yes | Zerobus Ingest endpoint, without `https://`, e.g. `<workspace-id>.zerobus.<region>.cloud.databricks.com` |
| `DATABRICKS_CLIENT_ID` | Yes | Service principal application ID |
| `DATABRICKS_CLIENT_SECRET` | Yes | Service principal OAuth secret, with the `all-apis` scope |
| `SQL_WAREHOUSE_ID` | Yes | SQL warehouse ID that the sink uses to check the target tables |
| `TARGET_CATALOG` | Yes | Unity Catalog catalog for the Delta tables |
| `TARGET_SCHEMA` | Yes | Schema for the `orders` table |
| `TARGET_CDC_SCHEMA` | Yes | Schema for the `payments` change history table. Filament's docs suggest a schema of its own, and it can be the same schema as `TARGET_SCHEMA` here |

`docker-compose.yml` publishes three health and API ports on `127.0.0.1`: the server on `8080`, the worker
on `8081` and the control plane on `8082`. The control plane sets `DISPATCH_MODE=worker`, so runs go to the
long-lived worker instead of new worker Jobs. Change the published ports in the compose file if
they clash with something on your machine.

`scripts/setup.sh` reads its settings from the environment. Its `FILAMENT_SERVER_URL` defaults to
`http://127.0.0.1:8080`. Its `FILAMENT_BIN` defaults to `filament`.

## Quick start

### 1. Configure

```bash
cp config.example.env .env
```

Fill in `.env`: generate `ENCRYPTION_KEY` with `openssl rand -base64 32`, choose `POSTGRES_PASSWORD`,
and use the same password in `SOURCE_DSN`. Use the Databricks values from [Prerequisites](#prerequisites).

### 2. Start the stack

```bash
docker compose up -d
tests/check_stack.sh
```

`check_stack.sh` checks each service's `/readyz` one time and prints one `ok` line per service. If a line is
missing, wait a few seconds and run it again. The PostgreSQL container creates the `shop` database and loads
the sample tables the first time its volume is created.

### 3. Create the Delta tables

Open `scripts/create_table.sql`, replace `<catalog>`, `<schema>` and `<cdc-schema>`, and run it in Databricks
SQL. Filament doesn't need to create these tables. If you change `TABLE_PREFIX`, add the same prefix to the
table names.

Then grant the service principal `USE CATALOG` on the catalog, `USE SCHEMA` on each schema, and `SELECT` and
`MODIFY` on each table, as [Zerobus Ingest service principal setup](https://docs.databricks.com/ingestion/zerobus-ingest#service-principal)
describes. Grant before the first run, as Filament's sink documentation requires.

### 4. Create the source, the sink and the pipelines

```bash
set -a; . ./.env; set +a
./scripts/setup.sh
```

The script creates the CLI context `zerobus-demo`, the sources `shop` and `shop-cdc` (the second one with
replication on), the sink `zb`, and the pipelines `orders-to-zb` and `payments-cdc-to-zb`.

### 5. Run the pipelines

```bash
filament run orders-to-zb
filament run payments-cdc-to-zb
```

Each run reports its record count:

```output
Run <run-id> completed: 20 records, <bytes> bytes.
Run <run-id> completed: 10 records, <bytes> bytes.
```

Wait about 20 seconds after `completed` before you query. In testing, new rows took about 20 seconds to
show up in Delta. Then check the tables:

```sql
SELECT count(*) AS rows_landed, count(DISTINCT id) AS distinct_ids
FROM <catalog>.<schema>.orders;

SELECT _filament_operation, count(*) AS changes
FROM <catalog>.<cdc-schema>.payments
GROUP BY _filament_operation;
```

After the first runs, `orders` has 20 rows for 20 ids, and `payments` has 10 rows, all `insert`.

### 6. Change the source and run the CDC pipeline again

```bash
docker compose exec -T postgres psql -U filament -d shop -v payments_table=payments < scripts/change_source.sql
filament run payments-cdc-to-zb
```

The script inserts one payment, marks three as `PAID` and deletes one. The next run sends 5 records.
After about 20 seconds, `payments` holds 15 rows in total: 11 `insert`, 3 `update` and 1 `delete`. The
delete row has only the key, with `state` set to `NULL`. Use `payments` as the value of `payments_table` if
you kept the default table names, or the prefixed name if you set `TABLE_PREFIX`.

### 7. Stop

```bash
docker compose down        # keeps the PostgreSQL volume
docker compose down -v     # also deletes it, so the next start seeds the source again
```

`setup.sh` creates the objects one time. To start over, run `docker compose down -v`, which also deletes the
PostgreSQL volume, and then `filament context delete zerobus-demo` before you run the script again.

## Schema

`scripts/create_table.sql` creates two Delta tables. Source columns come first, in the same order as the
PostgreSQL tables in `scripts/seed_source.sql`, then Filament's lineage columns.

`orders` (full append):

| Column | Delta type | Source column | Notes |
|---|---|---|---|
| `id` | `BIGINT NOT NULL` | `id BIGINT PRIMARY KEY` | The key |
| `customer` | `STRING NOT NULL` | `customer TEXT NOT NULL` | |
| `amount` | `DECIMAL(10,2)` | `amount NUMERIC(10,2)` | |
| `status` | `STRING` | `status TEXT` | `NEW` or `SHIPPED` in the sample data |
| `created_at` | `TIMESTAMP NOT NULL` | `created_at TIMESTAMPTZ NOT NULL` | |
| `_filament_run_id` | `STRING` | | The run that wrote the row |
| `_filament_run_started_at` | `TIMESTAMP` | | When that run started |

`payments` (`cdc_append`): the same kind of columns for `id`, `order_id`, `amount`, `state` and
`updated_at`, all nullable, because the table holds the change history. It adds the lineage columns and
three CDC columns:

| Column | Delta type | Notes |
|---|---|---|
| `_filament_operation` | `STRING` | `insert`, `update` or `delete` |
| `_filament_source_position` | `STRING` | The source's log position as text. It doesn't sort in log order. Rows from the snapshot have `NULL` |
| `_filament_sequence` | `BIGINT` | Orders the changes within a run |

Nullability has to match the source for a full append table. Filament fails the stream with error 8001 if
it doesn't, so `created_at` must stay `NOT NULL` in Delta.

## How to extend

- **Add a table**: add it to the source database, then create the Delta table with the same columns in the
  same order, `NOT NULL` where the source is `NOT NULL`, and Filament's lineage columns last. Grant the
  service principal `USE SCHEMA` and `SELECT, MODIFY`. Then add it to a pipeline with `--resources`, or
  create a pipeline for it.
- **Add a column to a source table**: add the Delta column after the last source column, with
  `ALTER TABLE <catalog>.<schema>.orders ADD COLUMN coupon STRING;` and then
  `ALTER TABLE <catalog>.<schema>.orders ALTER COLUMN coupon AFTER created_at;`. Adding it at the end of
  the table fails with an error 8001 about column order. This sequence was tested.
- **Let Filament create the tables**: add `--sink-create-table true` to a `filament pipeline create` command
  and set `--sink-warehouse-id` on the sink. Filament then creates the schema and tables, and grants the
  service principal on what it creates. The service principal needs `USE CATALOG` and `CREATE SCHEMA`, and
  the Can use permission on the warehouse. See [Filament's Databricks Zerobus sink reference](https://github.com/galaxy-io/filament/blob/v0.42.0/docs/pages/connectors/sinks/databricks-zerobus.mdx).
- **Use your own PostgreSQL**: point `SOURCE_DSN` at it. Every table you replicate needs a primary key. CDC
  needs `wal_level=logical`. By default Filament creates and maintains the publication for each route. See
  [Filament's PostgreSQL source reference](https://github.com/galaxy-io/filament/blob/v0.42.0/docs/pages/connectors/sources/postgres.mdx).
- **Run it in production**: Filament recommends Kubernetes with its Helm chart. This demo runs the
  worker-dispatch layout instead, and it doesn't cover Kubernetes dispatch. The worker must use the
  `-libc` image. See [Filament's Kubernetes guide](https://github.com/galaxy-io/filament/blob/v0.42.0/docs/pages/guides/deployment/kubernetes.mdx)
  and its [configuration guide](https://github.com/galaxy-io/filament/blob/v0.42.0/docs/pages/guides/deployment/configuration.mdx).

## Handle duplicates

- **`orders` (full append):** every run resends every row. After a second run, the table held 40 rows for
  20 ids. Query one row per id, for example with `count(DISTINCT id)` or a view that keeps the latest run.
- **`payments` (CDC):** keys repeat by design, because each change is a row. To read the current state of a
  key, keep the latest row by `_filament_run_started_at` and then `_filament_sequence`, and drop the key when
  that row is a `delete`.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Schema comparison failed: Field 'created_at' is incompatible ... nullability differs ... Error Code: 8001` | The Delta column is nullable, but the source column is `NOT NULL`. The stream doesn't open. | Make the Delta column match the source, for example `ALTER TABLE <catalog>.<schema>.orders ALTER COLUMN created_at SET NOT NULL`. |
| `Client field 'coupon' does not exist in Delta schema. Error Code: 8001` | A source column has no Delta column. The run fails when the stream opens, before any row is written. | Add the column as in [How to extend](#how-to-extend). |
| `Client field '_filament_run_id' is out of order ... Error Code: 8001` | A new column was added after the lineage columns. | `ALTER COLUMN <column> AFTER <last-source-column>`. |
| `run overlaps an active replication route` | An earlier run is still `RUNNING` because its worker stopped mid-run. The worker-dispatch layout has no reaper, so new runs of that pipeline are refused. In testing, cancel and pause requests waited for the dead worker, and the CLI has no cancel command. | Rows already written stay in the table. Drop them by the stranded run's `_filament_run_id`. Use Kubernetes dispatch in production, where a reaper fails runs whose worker stops reporting. This path wasn't tested. |
| `workspace_url must be an absolute https URL` | `DATABRICKS_WORKSPACE_URL` is `http://`, or missing its scheme. | Use `https://<workspace-url>`. |
| `Scopes 'all-apis' are not assigned to the client` | Per Filament's sink documentation, the OAuth secret was generated without the `all-apis` scope. | Create a new secret with that scope and update `DATABRICKS_CLIENT_SECRET`. |
| Every token request fails | Per Filament's sink documentation, token requests fail without the entitlement. Zerobus Ingest requires the Workspace access entitlement, and the service principal lacks it. | Add the entitlement to the service principal. |
| `401` or `invalid_authorization_details` | The service principal lacks `USE SCHEMA` on the schema or `SELECT, MODIFY` on the table. | Grant the privileges from [step 3](#3-create-the-delta-tables), then run the pipeline again. |
| `Connector databrickszerobus not found` | The worker isn't the `0.42.0-libc` image. | Use the `-libc` worker tag in `docker-compose.yml`. |
| `Context "zerobus-demo" already exists (remote); replace it?` when you run `setup.sh` a second time | `setup.sh` creates the context and the objects one time. The script stops at the first prompt. | Start over with `docker compose down -v` and `filament context delete zerobus-demo`, as in [Stop](#7-stop). |
| The count is still the old total right after `completed` | Delta shows the new rows about 20 seconds after Filament reports the run. | Wait about 20 seconds, then query. |

For anything else, check `docker compose logs worker`, and see
[Filament's troubleshooting guide](https://github.com/galaxy-io/filament/blob/v0.42.0/docs/pages/how-to/troubleshooting.mdx).
