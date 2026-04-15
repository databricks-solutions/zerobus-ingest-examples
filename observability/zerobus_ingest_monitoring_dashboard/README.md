# Zerobus Ingest Monitoring Dashboard

A Databricks Asset Bundle (DAB) that deploys an AI/BI dashboard for monitoring Zerobus Ingest activity in your workspace. The dashboard reads directly from the `system.lakeflow` system tables — no ETL pipeline or data preparation required.

## What it shows

The dashboard provides a single-page view of Zerobus Ingest health and throughput, filterable by table name:

| Section | Panels |
|---|---|
| Weekly Overview | Active streams, streams with errors (7d), unique active tables (7d), total records (7d), total GB (7d) |
| Last Hour | Requests, streams, active tables, GB ingested, records, error streams |
| Throughput Trends | Records/hr (7d), bytes/hr (7d), throughput by table (24h), streams opened vs closed/hr (7d) |
| Top Tables | Top 10 tables by records (7d), top 10 tables by bytes (7d) |
| Errors | Error rate % over time (7d), error code breakdown, error detail log (latest 200), ingest batch errors over time (7d) |
| Distribution | Protocol distribution (7d), data format distribution (7d) |

## Prerequisites

- Databricks CLI v0.200+ with DAB support ([install guide](https://docs.databricks.com/aws/en/dev-tools/cli/install))
- Access to a Databricks workspace with Zerobus Ingest enabled
- `USE` and `SELECT` permissions on `system.lakeflow` (requires metastore admin or workspace admin to grant)
- A SQL warehouse in the workspace

## Setup

**1. Configure your workspace**

Update `databricks.yml` with your workspace host:

```yaml
workspace:
  host: https://<your-workspace>.cloud.databricks.com
```

**2. Set your SQL warehouse name**

By default, the bundle looks for a warehouse named `"Shared Unity Catalog Serverless"`. To use a different warehouse, update the variable in `databricks.yml`:

```yaml
variables:
  warehouse_id:
    lookup:
      warehouse: "Your Warehouse Name"
```

Or pass it at deploy time:

```bash
databricks bundle deploy --var="warehouse_id=<warehouse-name>"
```

**3. Deploy**

```bash
databricks bundle deploy
```

The dashboard will be created in your workspace under the bundle deployment path.

## System tables used

| Table | Description |
|---|---|
| `system.lakeflow.zerobus_stream` | Stream lifecycle events — open/close timestamps, protocol, data format, errors |
| `system.lakeflow.zerobus_ingest` | Batch-level ingest metrics — committed records, committed bytes, commit version, errors |

Both tables have 365-day retention and are regional in scope.

## Notes

- The dashboard includes a **Table** filter that lets you scope all panels to a specific target table (full 3-part name: `catalog.schema.table`). Defaults to "All".
- If you don't have `system.lakeflow` access, the dashboard will render with empty panels. Contact your metastore or workspace admin to request access.
- This bundle does not create or manage any tables. It only deploys the dashboard.
