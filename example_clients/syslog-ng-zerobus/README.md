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

You should see syslog-ng load the `otel` and `cloud_auth` modules, then a line like:
```
Syslog connection established; ...
```
This confirms syslog-ng has connected to Zerobus and obtained an OAuth token.

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
