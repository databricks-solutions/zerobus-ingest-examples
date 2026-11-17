# syslog-ng + Zerobus Ingest OTLP — Working Example

This example walks through sending application logs to a Databricks Unity Catalog Delta table using [syslog-ng](https://www.syslog-ng.com/) and [Zerobus Ingest](https://docs.databricks.com/aws/en/ingestion/opentelemetry/) over OTLP/gRPC.

The app is a simple Flask health monitor that emits structured syslog messages at different severity levels. syslog-ng reads those messages, maps them to OpenTelemetry log attributes, and forwards them to Zerobus via the OTLP/gRPC endpoint — landing the data directly in a Delta table.

```
[ Flask App ]
     |
     | syslog (Unix socket /tmp/flask-app.sock)
     v
[ syslog-ng ]
     |
     | OTLP/gRPC (TLS, OAuth2 auto-refresh)
     v
[ Zerobus Ingest ] --> [ Delta Table in Unity Catalog ]
```

---

## Prerequisites

- **syslog-ng 4.11 or higher** — [Installation instructions](https://github.com/syslog-ng/syslog-ng?tab=readme-ov-file#installation-from-binaries)
  - Verify: `syslog-ng --version | head -1` (should say `syslog-ng 4 (4.11.x)`)
  - The `opentelemetry()` destination and `cloud-auth()` block are bundled since 4.3
- **Python 3.9+** — `python3 --version`
- **A Databricks workspace** on AWS, Azure, or GCP with Unity Catalog enabled
- **A service principal** with OAuth M2M credentials — [Setup guide](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m.html)

---

## What you need to fill in

Before running anything, you need the following values. Refer to `.env.example` for a full list.

| Value | Where to find it |
|---|---|
| `WORKSPACE_ID` | Settings → General → Workspace ID (numeric) |
| `WORKSPACE_REGION` | From your workspace URL (e.g. `us-west-2`) |
| `WORKSPACE_URL` | Your workspace hostname, without `https://` |
| `DATABRICKS_CLIENT_ID` | Service principal → Configurations tab → App ID |
| `DATABRICKS_CLIENT_SECRET` | Service principal → OAuth → Client secret |
| `CATALOG` | Your Unity Catalog catalog name |
| `SCHEMA` | Your schema name |
| `TABLE` | Your table name (e.g. `health_monitor_logs`) |
| `SERVICE_PRINCIPAL_UUID` | Same as `DATABRICKS_CLIENT_ID` — used in the SQL GRANT statements |

For finding your workspace ID and region, see [Get your workspace URL and Zerobus Ingest endpoint](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest#get-your-workspace-url-and-zerobus-ingest-endpoint).

---

## Step 1 — Create the Delta table in Databricks SQL

Open `create_table.sql`. Replace all placeholder values:

```sql
-- Replace these throughout the file:
--   <catalog>                  → your catalog name
--   <schema>                   → your schema name
--   <table>                    → your table name (e.g. health_monitor_logs)
--   <service-principal-uuid>   → your service principal's App ID (UUID)
```

Then run the full file in Databricks SQL (the workspace SQL editor or the Databricks CLI).

**Important:** `GRANT ALL PRIVILEGES` is not sufficient. The SQL file uses explicit `GRANT` statements — run all three.

**Important:** Tables must exist before syslog-ng starts. Zerobus does not auto-create tables.

---

## Step 2 — Configure syslog-ng

Open `syslog-ng.conf` and fill in the `@define` variables at the top:

```
@define workspace_id       "<YOUR_WORKSPACE_ID>"
@define workspace_region   "<YOUR_REGION>"
@define workspace_url      "<YOUR_WORKSPACE_URL>"
@define client_id          "<YOUR_CLIENT_ID>"
@define client_secret      "<YOUR_CLIENT_SECRET>"
@define catalog            "<YOUR_CATALOG>"
@define schema             "<YOUR_SCHEMA>"
@define table              "<YOUR_TABLE>"
```

The `cloud-auth(oauth2(...))` block handles token fetching and automatic renewal. No manual token rotation is needed.

Validate your config syntax before starting:

```bash
syslog-ng --syntax-only --cfgfile=syslog-ng.conf
```

The output should show the version line with no errors (the Python venv warning is harmless).

---

## Step 3 — Start syslog-ng

> **Important:** Start syslog-ng BEFORE the Flask app. syslog-ng creates the Unix socket at `/tmp/flask-app.sock` — the app needs the socket to exist when it starts.

```bash
syslog-ng -Fevd --cfgfile=syslog-ng.conf
```

Flags:
- `-F` — foreground (keeps it in your terminal so you can see logs)
- `-e` — echo log messages to stderr
- `-v` — verbose output
- `-d` — debug output

You should see syslog-ng load the `otel` and `cloud_auth` modules, start its worker threads, and then print a line like:
```
syslog-ng starting up; version='4.12.0'
```
syslog-ng 4.12.0 doesn't print a line when it connects to Zerobus Ingest, so confirm the connection with the query in Step 7 after the Flask app is sending logs.

Leave this terminal open and running.

---

## Step 4 — Install Python dependencies

In a new terminal, from this directory:

```bash
pip install -r requirements.txt
```

This installs Flask 3.x. Python's built-in `logging.handlers.SysLogHandler` is used for the syslog socket — no extra packages needed for logging.

---

## Step 5 — Start the Flask app

```bash
python app.py
```

The app starts on `http://localhost:5000` and immediately begins emitting a health log via the syslog socket every 10 seconds.

---

## Step 6 — Generate log events

In another terminal:

**Check service health (INFO log):**
```bash
curl http://localhost:5000/health
```

**Poll current metrics (INFO log):**
```bash
curl http://localhost:5000/metrics
```

**Simulate a failure (ERROR + WARN logs):**
```bash
curl -X POST http://localhost:5000/simulate/error
```

Run `/simulate/error` a few times to generate a burst of error-level events. The background monitor also automatically emits occasional `latency_degradation_detected` warnings every ~10 seconds.

---

## Step 7 — Query your logs in Databricks SQL

Once data is flowing, query your table:

```sql
-- All logs from the last hour
SELECT time, severity_text, body::string AS message
FROM <catalog>.<schema>.<table>
WHERE time > current_timestamp() - INTERVAL 1 HOUR
ORDER BY time DESC;

-- Error and warning events only
SELECT time, severity_text, body::string AS message
FROM <catalog>.<schema>.<table>
WHERE severity_text IN ('ERROR', 'WARNING')
  AND time > current_timestamp() - INTERVAL 1 HOUR
ORDER BY time DESC;

-- Log volume by severity over time
SELECT
  date_trunc('minute', time) AS minute,
  severity_text,
  COUNT(*) AS log_count
FROM <catalog>.<schema>.<table>
WHERE time > current_timestamp() - INTERVAL 1 HOUR
GROUP BY 1, 2
ORDER BY minute;

-- Extract syslog attributes from VARIANT column
SELECT
  time,
  severity_text,
  service_name,
  body::string AS message,
  attributes:['host.name']::string AS host,
  attributes:['syslog.facility']::string AS facility
FROM <catalog>.<schema>.<table>
WHERE time > current_timestamp() - INTERVAL 1 HOUR
ORDER BY time DESC;
```

> **Note:** Querying `VARIANT` columns requires Databricks Runtime 15.3 or higher. For best performance, use DBR 17.2+ which enables variant shredding.

---

## File reference

| File | Description |
|---|---|
| `app.py` | Flask health monitor app — emits syslog messages via Unix socket |
| `requirements.txt` | Python dependencies (Flask 3.x) |
| `syslog-ng.conf` | syslog-ng pipeline config — fill in your variables before running |
| `create_table.sql` | Delta table schema + permission grants — run in Databricks SQL |
| `.env.example` | Template listing all values you need to provide |
| `README.md` | This file |

---

## How it works

**The Flask app** uses Python's built-in `logging.handlers.SysLogHandler` to write structured syslog messages to a Unix datagram socket at `/tmp/flask-app.sock`. Each log line includes key=value pairs.

**syslog-ng** listens on that socket and processes each message through a `rewrite` rule that maps standard syslog macros to OpenTelemetry attribute paths. `$PROGRAM` is mapped to `.otel.resource.attributes.service.name` (a resource-level attribute), which populates the top-level `service_name` column in the Delta table. Other syslog fields (`$HOST`, `$PID`, facility, priority) are mapped to `.otel.log.attributes.*`.

**The `opentelemetry()` destination** serialises those attributes as OTLP/gRPC Protobuf messages and sends them to Zerobus Ingest. OAuth2 token management is handled automatically by the `cloud-auth()` block — tokens are fetched and refreshed without any manual intervention.

**Zerobus Ingest** validates each record against the target table schema and writes it to your Delta table. From there the data is immediately queryable with Databricks SQL.

---

## Troubleshooting

**`--- Logging error ---` in Flask output:**
The app's `SysLogHandler` can't reach `/tmp/flask-app.sock`. This means syslog-ng isn't running or wasn't started before the Flask app. Stop the Flask app, start syslog-ng first (Step 3), then restart the Flask app.

**syslog-ng fails to start — token error:**
Check your `client_id`, `client_secret`, and `workspace_url` in `syslog-ng.conf`. The OAuth token request uses HTTP Basic auth with these credentials.

**syslog-ng fails to start — TLS error:**
The `auth(tls())` block requires the system to trust Databricks' TLS certificate. On macOS and most Linux distributions this works out of the box. If you see TLS errors, verify your system CA bundle is up to date.

**syslog-ng fails to start — connection refused:**
Verify your `workspace_id` and `workspace_region` form the correct Zerobus endpoint:
`<workspace_id>.zerobus.<region>.cloud.databricks.com:443`

**No data appearing in Databricks SQL:**
- Confirm the table was created with the correct schema before starting syslog-ng.
- Check that all three GRANT statements ran successfully.
- Look for errors in syslog-ng's verbose output (the terminal where you ran `syslog-ng -Fevd`).

**`Zerobus Ingest` in Beta notice:**
This feature is currently in Beta. For limits and quotas, see [Zerobus Ingest limits](https://docs.databricks.com/aws/en/ingestion/zerobus-limits).

## Schema

The table is created by `create_table.sql`. It uses the OpenTelemetry logs layout (`otel.schemaVersion = v2`). Zerobus Ingest doesn't create tables, so run the DDL first. Each row is one syslog message.

| Column | Type | What the demo writes |
|---|---|---|
| `record_id` | STRING | Not set by `syslog-ng.conf`. Each row in the test run had a distinct value. |
| `time` | TIMESTAMP | The event time from the syslog message. `time_unix_nano` is set from `$S_UNIXTIME`. |
| `date` | DATE | Not set by `syslog-ng.conf`. |
| `service_name` | STRING | `$PROGRAM`, the program name (for example `health-monitor`). |
| `event_name` | STRING | Not set by `syslog-ng.conf`. The event name is in `attributes` as `event.name`. |
| `trace_id` | STRING | An empty string. Syslog messages carry no trace context. |
| `span_id` | STRING | An empty string. Syslog messages carry no trace context. |
| `time_unix_nano` | LONG | `$S_UNIXTIME` in nanoseconds. |
| `observed_time_unix_nano` | LONG | `$R_UNIXTIME` in nanoseconds, when syslog-ng received the message. |
| `severity_number` | STRING | The OpenTelemetry severity for the syslog level, such as `SEVERITY_NUMBER_INFO`. See the severity note below. |
| `severity_text` | STRING | `FATAL`, `ERROR`, `WARNING`, `INFO` or `DEBUG`, from the syslog level. |
| `body` | VARIANT | `$MESSAGE`, the message text. |
| `attributes` | VARIANT | `host.name`, `process.pid`, `syslog.facility`, `syslog.facility.code`, `syslog.priority` and `event.name`. |
| `dropped_attributes_count` | INT | Not set by `syslog-ng.conf`. |
| `flags` | INT | Not set by `syslog-ng.conf`. |
| `resource` | STRUCT | `attributes` holds `service.name`, from `$PROGRAM`. |
| `resource_schema_url` | STRING | Not set by `syslog-ng.conf`. |
| `instrumentation_scope` | STRUCT | `name` is `syslog-ng-otel-exporter`. |
| `log_schema_url` | STRING | Not set by `syslog-ng.conf`. |

The `rewrite r_to_otlp` block sets the `.otel.*` paths that become these columns. `.otel.resource.attributes.service.name` becomes `service_name`, and `.otel.log.body` becomes `body`. Other `.otel.log.attributes.*` paths go into `attributes`. Because `attributes` is a VARIANT column, a new attribute needs no table change. A new top-level column needs an `ALTER TABLE` on the table first, because Zerobus Ingest doesn't evolve the table itself. Adding a nullable column doesn't break existing senders ([Zerobus Ingest schema management](https://docs.databricks.com/aws/en/ingestion/zerobus-schema-management)).

Syslog levels (0 to 7) are not OpenTelemetry severity numbers. Setting `severity_number` from `$LEVEL_NUM` gives the wrong names, for example `SEVERITY_NUMBER_DEBUG2` for INFO. The `if` and `elif` block in the `log` path maps each level to its number and text instead. Also, syslog-ng writes `$LEVEL` in uppercase, so an error is `ERR` there, and the `ERROR` text comes from the mapping.

`body` and `attributes` are VARIANT. Use `body::string` and `attributes:['host.name']::string`. Querying VARIANT needs Databricks Runtime 15.3 and above. For variant shredding, use Databricks Runtime 17.2 and above.

The DDL sets the variant features with the `-preview` suffix, as in this file. These are the names this example was run with. [Configure OTLP clients](https://docs.databricks.com/aws/en/ingestion/opentelemetry/configure) lists the same features without the suffix. Check which names your runtime accepts before you rely on them.

## How to extend

- **Other log sources**: Replace `source s_flask` with another source, and keep the rewrite and the destination. The [system() source](https://syslog-ng.github.io/admin-guide/060_Sources/180_System/README) automatically collects the host's system-specific log messages on a number of platforms. A [network() source](https://syslog-ng.github.io/admin-guide/060_Sources/070_Network/README) receives messages from remote hosts over TCP or UDP. The [file() source](https://syslog-ng.github.io/admin-guide/060_Sources/020_File/README) is kept for compatibility, and syslog-ng recommends [wildcard-file()](https://syslog-ng.github.io/admin-guide/060_Sources/021_Wildcard-file/000_Wildcard-file_options) for regular files. On a `network()` source, `keep-hostname(yes)` keeps the host name from the message as `$HOST`. With `keep-hostname(no)`, syslog-ng rewrites `$HOST` to the sending host ([network() source options](https://syslog-ng.github.io/admin-guide/060_Sources/070_Network/000_Network_source_options)).
- **More fields**: Add a `set("$SOMETHING" value(".otel.log.attributes.your.key"));` line in `r_to_otlp`. New messages carry the field in `attributes`.
- **More severity or text mapping**: Add a branch to the `if` and `elif` block for the level you need. Its rewrite should set both `severity_number` and `severity_text`, and it should write the number as an expression such as `"$(+ 0 17)"`.
- **More destinations**: A second `opentelemetry()` destination needs its own `x-databricks-zerobus-table-name` header, because each request targets a single table, and its own OAuth `authorization_details` for that table. If syslog-ng reports duplicate persist names at startup, give the destination an explicit `persist-name()` ([opentelemetry() destination options](https://syslog-ng.github.io/admin-guide/070_Destinations/157_OpenTelemetry/000_opentelemetry-destination-options)). Add it to the `log` path with `destination(...)`.
- **Durability**: This example has no disk buffer, so messages held in memory are lost when syslog-ng stops. Add a `disk-buffer()` to the destination to keep them. The [syslog-ng buffering documentation](https://syslog-ng.github.io/admin-guide/080_Log/020_Buffering/README) describes the options. This example hasn't tested a disk buffer.
