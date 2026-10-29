# Sparkplug B Zerobus

A connector that subscribes to an MQTT server carrying **Sparkplug B** (for example Ignition with MQTT Transmission publishing to a Chariot MQTT server, or any Sparkplug 3.0 edge) and writes **one row per
Sparkplug message** to a Delta table through Zerobus Ingest.

- **One row per message** in a `raw_messages` table: the topic split into columns, the payload as Eclipse Tahu's
  JSON (byte for byte what Java Tahu produces) in a `VARIANT` column. Splitting it into metrics is left to SQL.
- **Sparkplug primary host application** (optional): ONLINE only while Databricks accepts the rows, so the edges
  buffer (store and forward) whenever they can't get there.
- **Confirmed writes**: a batch leaves the connector only once Zerobus has acknowledged it; an optional
  write-ahead log keeps unacknowledged rows across a crash or restart.
- **A stored procedure call per NBIRTH** (optional), for example to (re)build per-model views.
- **Strict Sparkplug sequencing** (by default): messages put back in `seq` order, an edge node's messages dropped
  after a gap until its rebirth, rebirth requests (NCMD) to edge nodes it hasn't seen born, as Eclipse Tahu's host
  library decides them (ported and tested against it); or a row for every message (`sequence_handling: keep_all`).
- **Web UI** (plain HTML, no build step) to stop, start and restart the connector, add, edit, start and stop
  sources and Zerobus, change its settings and upload the MQTT TLS files; each edge node and device online or
  offline and since when, and a rebirth request for an edge node; an optional login or a read-only mode; a health
  endpoint for Docker.
- **The Eclipse Sparkplug TCK's Host Application tests** pass as primary host, except where the connector does less
  than a full host application on purpose (no metric writes, no STALE marking); details in the connector's
  [README](https://github.com/nickconnors/sparkplug-zerobus-connector/blob/0d6855f/README.md#sparkplug-tck). Not a
  certification.

This folder is exported from [nickconnors/sparkplug-zerobus-connector](https://github.com/nickconnors/sparkplug-zerobus-connector)
(commit [`0d6855f`](https://github.com/nickconnors/sparkplug-zerobus-connector/commit/0d6855f)). Its tests live
there.

## What's here

```
sparkplug_zerobus/
├── sparkplug_connector/          # the connector (a Python package)
│   ├── config/config.yaml        # the configuration: one example source, off
│   ├── sparkplug/                # Sparkplug B decoding, primary host, NBIRTH calls (README.md: the reference; NOTICE.md)
│   └── web/static/ui/            # the web UI (HTML, Alpine.js, Pico CSS; no build step)
├── requirements.txt              # runtime dependencies
├── Dockerfile                    # Python only, with a HEALTHCHECK
├── .env.example                  # the two secrets, if you keep them in a file (Run it in Docker)
└── NOTICE.md                     # third-party notices (Eclipse Tahu, EPL-2.0; Alpine.js, Pico CSS, MIT)
```

## Primary host or plain subscriber

Sparkplug publishes births and data at MQTT QoS 0, so the MQTT server can't queue them for a subscriber that is
away. Only the edge can buffer, and it does so for its **primary host**. Choose one of two ways to run the connector:

| | Primary host (`primary_host_id` set) | Plain subscriber (`primary_host_id` empty) |
|---|---|---|
| Edge setting | MQTT Transmission's Primary Host ID = the connector's `primary_host_id`, store and forward on | nothing to change |
| Connector down, or Databricks unreachable | the edges buffer and send the backlog (flagged historical) once it is ONLINE again: nothing lost | messages published meanwhile are lost |
| Start | the edges rebirth when it goes ONLINE | it asks each edge node it finds publishing for a rebirth (NCMD, ~5 s after its first message; the MQTT user needs publish rights on `spBv1.0/+/NCMD/+`), unless `block_auto_rebirth: true` (with `sequence_handling: keep_all`) |
| Other hosts | while the connector is OFFLINE, MQTT Transmission **disconnects from the MQTT server for every subscriber**, not only for the connector | no effect on anything else |

MQTT Transmission has **one** primary host per transmitter. If another application is already its primary host,
either the connector takes over that role, or it runs as a plain subscriber next to it. Run exactly one connector
per Primary Host ID: two with the same id fight over STATE, and two with different ids write every row twice.

## Before you start

- A Unity Catalog table for the rows, and a **service principal** with an OAuth secret that may write it:

```sql
-- or an existing catalog and schema (then change the names below and in the configuration)
CREATE CATALOG IF NOT EXISTS sparkplug;
CREATE SCHEMA IF NOT EXISTS sparkplug.bronze;

CREATE TABLE IF NOT EXISTS sparkplug.bronze.raw_messages (
  msg_id STRING, msg_topic STRING, namespace STRING, group_id STRING, message_type STRING,
  edge_node_id STRING, device_id STRING, msg VARIANT, inserted_at DECIMAL(38,0)
)
CLUSTER BY (message_type, group_id, edge_node_id, device_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT USE CATALOG ON CATALOG sparkplug TO `<application id>`;
GRANT USE SCHEMA ON SCHEMA sparkplug.bronze TO `<application id>`;
GRANT MODIFY, SELECT ON TABLE sparkplug.bronze.raw_messages TO `<application id>`;
```

  The names are an example (any catalog, schema and table work). Column comments and sample queries:
  [UNITY_CATALOG_SCHEMAS.md](https://github.com/nickconnors/sparkplug-zerobus-connector/blob/0d6855f/docs/UNITY_CATALOG_SCHEMAS.md);
  the grants for the NBIRTH call: [sparkplug_connector/sparkplug/README.md](sparkplug_connector/sparkplug/README.md#databricks-setup).
- The Zerobus endpoint: `<workspace-id>.zerobus.<region>.cloud.databricks.com`.
- An MQTT user that may subscribe to `spBv1.0/#`, publish rebirth requests on `spBv1.0/+/NCMD/+` (unless
  `block_auto_rebirth: true`; Chariot closes the connection on a refused publish) and, as a primary host,
  publish its own STATE topic (`spBv1.0/STATE/<primary_host_id>`).

## Configure it

Edit [sparkplug_connector/config/config.yaml](sparkplug_connector/config/config.yaml), the connector's default
configuration (or keep your own file anywhere and pass it with `--config`). It has one example source,
`sparkplug-example`, switched off. In it:

1. Under `zerobus:`, set `zerobus_endpoint` (`<workspace-id>.zerobus.<region>.cloud.databricks.com`),
   `default_target` (`workspace_host: https://<workspace>.cloud.databricks.com` and your table) and
   `auth.client_id` (the service principal's application id), and `enabled: true` (off in the example, so a first
   start tries no table; or switch it on on the Zerobus page: *Start Zerobus with the connector*, then Start).
2. On the source, set `endpoint` (`mqtt://<mqtt server>:1883`), `username`, `enabled: true`, and
   `primary_host_id` if the connector is the edges' primary host.

The two secrets are not in the file: the connector reads them from its environment (next step),
`DATABRICKS_CLIENT_SECRET` (the service principal's OAuth secret) and `SPARKPLUG_MQTT_PASSWORD` (the MQTT user's
password). Optionally any other value can come from the environment too: `'${env:NAME}'`, or
`'${env:NAME:default}'`, in the file.

For **MQTT over TLS** use `mqtts://<host>:8883`; a private CA, client certificates and the other TLS settings go
under the source's `security:` (commented in the example). Every setting:
[sparkplug_connector/sparkplug/README.md](sparkplug_connector/sparkplug/README.md#configuration).

## Run it in Docker

From this folder (`--build-arg PIP_INDEX_URL=<your index>` if pypi.org isn't reachable). The secrets are typed in
(not shown) and passed from the shell (`-e NAME` without a value): nothing is written to disk.

Linux / macOS (bash or zsh):

```
printf "Zerobus client secret: "; read -rs DATABRICKS_CLIENT_SECRET; echo; export DATABRICKS_CLIENT_SECRET
printf "MQTT password: "; read -rs SPARKPLUG_MQTT_PASSWORD; echo; export SPARKPLUG_MQTT_PASSWORD
docker build -t sparkplug-zerobus .
docker run -d --name sparkplug-zerobus --restart unless-stopped --stop-timeout 30 \
  -e DATABRICKS_CLIENT_SECRET -e SPARKPLUG_MQTT_PASSWORD \
  -p 127.0.0.1:8001:8001 \
  --mount type=bind,source="$PWD/sparkplug_connector/config/config.yaml",target=/app/sparkplug_connector/config/config.yaml \
  --mount type=volume,source=connector-wal,target=/app/wal \
  --mount type=volume,source=connector-certs,target=/home/connector/.sparkplug_connector/certs \
  sparkplug-zerobus
```

Windows (PowerShell; a backtick ends each continued line, nothing after it):

```powershell
$env:DATABRICKS_CLIENT_SECRET = [Net.NetworkCredential]::new('', (Read-Host "Zerobus client secret" -AsSecureString)).Password
$env:SPARKPLUG_MQTT_PASSWORD = [Net.NetworkCredential]::new('', (Read-Host "MQTT password" -AsSecureString)).Password
docker build -t sparkplug-zerobus .
docker run -d --name sparkplug-zerobus --restart unless-stopped --stop-timeout 30 `
  -e DATABRICKS_CLIENT_SECRET -e SPARKPLUG_MQTT_PASSWORD `
  -p 127.0.0.1:8001:8001 `
  --mount "type=bind,source=$PWD\sparkplug_connector\config\config.yaml,target=/app/sparkplug_connector/config/config.yaml" `
  --mount "type=volume,source=connector-wal,target=/app/wal" `
  --mount "type=volume,source=connector-certs,target=/home/connector/.sparkplug_connector/certs" `
  sparkplug-zerobus
```

- The secrets: Docker keeps them in the container's settings (anyone who may run `docker inspect` on the machine
  can read them), so they survive restarts and reboots (`--restart unless-stopped`); type them again only when
  the container is made again (`docker rm`, then `docker run`, e.g. for an update). To make it again unattended,
  put them in a file instead: copy [.env.example](.env.example) to `connector.env` (git-ignored here), fill it in,
  and use `--env-file connector.env` in place of the two `-e`. The web page never sets a secret's value, only the
  name of the environment variable it is in.
- The edited file is mounted over the image's copy of it; edits made on the web page are saved into it, so don't
  mount it read-only. The container runs as uid 1000: on Linux, make the file writable by it
  (`sudo chown 1000 sparkplug_connector/config/config.yaml`), or the page's saves fail.
- It's a file the repository tracks: before pulling an update, commit or stash your edits (`git stash`, then
  `git stash pop` after the pull).
- Use `--mount`, not `-v`, for the file: if the file doesn't exist, `-v` creates an empty directory in its place
  (on Windows as well), and the connector fails with `IsADirectoryError`; `--mount` stops with an error.
- The `connector-wal` volume keeps the write-ahead log (`zerobus.wal`, on in the example) across container restarts;
  `connector-certs` keeps the certificates uploaded on the web page, which would otherwise be gone when the
  container is made again.
- `--stop-timeout 30`: on `docker stop` the connector first publishes OFFLINE (a primary host's edges start
  buffering), then writes what is queued, which can take up to ~20 s. Docker's default is 10 s, then SIGKILL; with
  the write-ahead log the rows not written are sent after the next start, without it they are lost. In Compose:
  `stop_grace_period: 30s`.
- Logs go to the console only (`docker logs`; the image sets `CONNECTOR_LOG_FILES=false`): set a size limit in
  Docker's logging options (e.g. `--log-opt max-size=50m --log-opt max-file=5`).
- The web UI is on port 8001 (`web_ui.port`). Publish it on 127.0.0.1, or switch the login on
  (`web_ui.authentication`, a Databricks workspace's OAuth; the steps are in the source repository's
  [README](https://github.com/nickconnors/sparkplug-zerobus-connector/blob/0d6855f/README.md#login-off-by-default))
  before publishing it more widely. With the login off, the page answers only to the names the connector knows
  as its own (IP addresses, `localhost`, the machine's name; in Docker the container's): to open it by another
  name, e.g. the VM's, list that in `web_ui.allowed_hosts` (DNS rebinding protection; `['*']` turns it off).

**Health check.** The image's `HEALTHCHECK` calls `GET /health` on the web UI's port every 30 s
(`CONNECTOR_HEALTH_PORT`, default 8001; set it if you change `web_ui.port`). It reports whether the process and
its web server answer, so `docker ps` shows `unhealthy` when the connector hangs. It doesn't say whether rows reach
Databricks: `GET /api/sparkplug/health` does, per source (`state`, `rows_written`, `zerobus_failing_since`,
`last_error`, the NBIRTH calls and the write-ahead log).

## Or run it with Python

With the two secrets in the shell's environment as above (the two `read` lines, or the two `$env:` lines).
Linux / macOS:

```
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m sparkplug_connector
```

Windows (PowerShell):

```powershell
py -3.12 -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python -m sparkplug_connector
```

## Web UI

`http://127.0.0.1:8001/`: **Status** (stop, start and restart the connector; rows written, queued, sources, edge
nodes; each source's state: Online, Subscribed, Waiting for Zerobus, Connecting, or Error with the reason), **Sources** (add, edit, delete, start, stop; the NBIRTH procedure settings), **Edge nodes** (each edge node
and device online or offline since when; Request rebirth sends the edge node an NCMD), **Zerobus** (endpoint, table,
client id, start and stop, each table's writer, diagnostics), **Settings** (the connector-wide settings, applied at
the next restart) and **Certificates** (the MQTT TLS files; a source names one by its file name). A save goes to the
file the connector was started with (`--config`), `${env:...}` placeholders kept; a save made on a configuration
someone else changed meanwhile is refused (reload the page). Secrets come from environment variables only: the page
sets the variable's name, never a value (a source's Password variable left empty: `SPARKPLUG_MQTT_PASSWORD`). `web_ui.read_only: true` makes the page a status display. Every page and
setting: the source repository's [README](https://github.com/nickconnors/sparkplug-zerobus-connector/blob/0d6855f/README.md#web-ui).

## License

The repository's [LICENSE.md](../../LICENSE.md), except the Eclipse Tahu-derived files, which stay under the
Eclipse Public License 2.0, and the web page's vendored Alpine.js and Pico CSS (MIT): [NOTICE.md](NOTICE.md).
