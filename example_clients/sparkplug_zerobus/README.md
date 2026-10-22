# Sparkplug B Zerobus

A connector that subscribes to an MQTT server carrying **Sparkplug B** (for example Ignition with MQTT Transmission publishing to a Chariot MQTT server, or any Sparkplug 3.0 edge) and writes **one row per
Sparkplug message** to a Delta table through Zerobus Ingest.

- **One row per message** in a `sparkplug_raw` table: the topic split into columns, the payload as Eclipse Tahu's
  JSON (byte for byte what Java Tahu produces) in a `VARIANT` column. Splitting it into metrics is left to SQL.
- **Sparkplug primary host application** (optional): ONLINE only while Databricks accepts the rows, so the edges
  buffer (store and forward) whenever they can't get there.
- **Confirmed writes**: a batch leaves the connector only once Zerobus has acknowledged it; an optional
  write-ahead log keeps unacknowledged rows across a crash or restart.
- **A stored procedure call per NBIRTH** (optional), for example to (re)build per-model views.
- **Web UI** to add, edit, start and stop sources and Zerobus, with an optional login; a health endpoint for Docker.

This folder is exported from [nickconnors/sparkplug-zerobus-connector](https://github.com/nickconnors/sparkplug-zerobus-connector)
(commit [`0cd004b`](https://github.com/nickconnors/sparkplug-zerobus-connector/commit/0cd004b)), which keeps the
Sparkplug B path of [pravinva/unified-ot-zerobus-connector](https://github.com/pravinva/unified-ot-zerobus-connector)
(see [Unified OT Zerobus](../unified_ot_zerobus/) for its multi-protocol version). The tests and the web UI's source
code live there.

## What's here

```
sparkplug_zerobus/
├── sparkplug_connector/          # the connector (a Python package)
│   ├── config/config.yaml        # the configuration: one example source, off
│   ├── sparkplug/                # Sparkplug B decoding, primary host, NBIRTH calls (README.md: the reference; NOTICE.md)
│   └── web/static/               # the web UI, prebuilt
├── requirements.txt              # runtime dependencies
├── Dockerfile                    # Python only, with a HEALTHCHECK
└── NOTICE.md                     # third-party notices (Eclipse Tahu, EPL-2.0)
```

## Primary host or plain subscriber

Sparkplug publishes births and data at MQTT QoS 0, so the MQTT server can't queue them for a subscriber that is
away. Only the edge can buffer, and it does so for its **primary host**. Choose one of two ways to run the connector:

| | Primary host (`primary_host_id` set) | Plain subscriber (`primary_host_id` empty) |
|---|---|---|
| Edge setting | MQTT Transmission's Primary Host ID = the connector's `primary_host_id`, store and forward on | nothing to change |
| Connector down, or Databricks unreachable | the edges buffer and send the backlog (flagged historical) once it is ONLINE again: nothing lost | messages published meanwhile are lost |
| Start | the edges rebirth when it goes ONLINE | press **Rebirth** on the edges after starting it: the connector doesn't send rebirth requests, and the views need the births |
| Other hosts | while the connector is OFFLINE, MQTT Transmission **disconnects from the MQTT server for every subscriber**, not only for the connector | no effect on anything else |

MQTT Transmission has **one** primary host per transmitter. If another application is already its primary host,
either the connector takes over that role, or it runs as a plain subscriber next to it. Run exactly one connector
per Primary Host ID: two with the same id fight over STATE, and two with different ids write every row twice.

## Before you start

- A Unity Catalog table for the rows, and a **service principal** with an OAuth secret that may write it:

```sql
CREATE TABLE IF NOT EXISTS bridge_stage_db.stage_db.sparkplug_raw (
  msg_id STRING, msg_topic STRING, namespace STRING, group_id STRING, message_type STRING,
  edge_node_id STRING, device_id STRING, msg VARIANT, inserted_at DECIMAL(38,0)
)
CLUSTER BY (message_type, group_id, edge_node_id, device_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT USE CATALOG ON CATALOG bridge_stage_db TO `<application id>`;
GRANT USE SCHEMA ON SCHEMA bridge_stage_db.stage_db TO `<application id>`;
GRANT MODIFY, SELECT ON TABLE bridge_stage_db.stage_db.sparkplug_raw TO `<application id>`;
```

  The names are an example (any catalog, schema and table work). Column comments and sample queries:
  [UNITY_CATALOG_SCHEMAS.md](https://github.com/nickconnors/sparkplug-zerobus-connector/blob/0cd004b/docs/UNITY_CATALOG_SCHEMAS.md);
  the grants for the NBIRTH call: [sparkplug_connector/sparkplug/README.md](sparkplug_connector/sparkplug/README.md#databricks-setup).
- The Zerobus endpoint: `<workspace-id>.zerobus.<region>.cloud.databricks.com`.
- An MQTT user that may subscribe to `spBv1.0/#` and, as a primary host, publish its own STATE topic
  (`spBv1.0/STATE/<primary_host_id>`).

## Configure it

Copy [sparkplug_connector/config/config.yaml](sparkplug_connector/config/config.yaml) to `my-config.yaml`. It has one
example source, `sparkplug-example`, switched off. In your copy:

1. Set `zerobus.default_target` to your table.
2. Set `enabled: true` on the source, and `primary_host_id` if the connector is the edges' primary host.
3. Keep secrets out of the file: it reads them from the environment (`${env:NAME}` or `${env:NAME:default}`):

```
ZEROBUS_ENDPOINT=<workspace-id>.zerobus.<region>.cloud.databricks.com
DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
DATABRICKS_CLIENT_ID=<service principal application id>
DATABRICKS_CLIENT_SECRET=<its OAuth secret>
SPARKPLUG_MQTT_ENDPOINT=mqtt://<mqtt server>:1883
SPARKPLUG_MQTT_USERNAME=<mqtt user>
SPARKPLUG_MQTT_PASSWORD=<its password>
```

For **MQTT over TLS** use `mqtts://<host>:8883`; a private CA, client certificates and the other TLS settings go
under the source's `security:` (commented in the example). Every setting:
[sparkplug_connector/sparkplug/README.md](sparkplug_connector/sparkplug/README.md#configuration).

## Run it in Docker

From this folder (`--build-arg PIP_INDEX_URL=<your index>` if pypi.org isn't reachable):

```
docker build -t sparkplug-zerobus .
docker run -d --name sparkplug-zerobus --restart unless-stopped --stop-timeout 30 --env-file connector.env \
  -p 127.0.0.1:8001:8001 \
  --mount type=bind,source="$PWD/my-config.yaml",target=/app/sparkplug_connector/config/config.yaml \
  --mount type=volume,source=connector-wal,target=/app/wal \
  sparkplug-zerobus --config /app/sparkplug_connector/config/config.yaml
```

- `connector.env` holds the variables above; keep the secrets there (a client secret saved on the web page goes
  to an encrypted store in the container's home directory and is gone when the container is recreated).
- Your file is mounted over the shipped example and passed with `--config`; edits made on the web page are saved
  into it, so don't mount it read-only. The container runs as uid 1000: on Linux, make the file writable by it
  (`sudo chown 1000 my-config.yaml`), or the page's saves fail.
- Use `--mount`, not `-v`, for the file: if the file doesn't exist, `-v` creates an empty directory in its place
  (on Windows as well), and the connector fails with `IsADirectoryError`; `--mount` stops with an error.
- The `connector-wal` volume keeps the write-ahead log (`zerobus.wal`, on in the example) across container restarts.
- `--stop-timeout 30`: on `docker stop` the connector first publishes OFFLINE (a primary host's edges start
  buffering), then writes what is queued, which can take up to ~20 s. Docker's default is 10 s, then SIGKILL; with
  the write-ahead log the rows not written are sent after the next start, without it they are lost. In Compose:
  `stop_grace_period: 30s`.
- Logs go to the console only (`docker logs`; the image sets `CONNECTOR_LOG_FILES=false`): set a size limit in
  Docker's logging options (e.g. `--log-opt max-size=50m --log-opt max-file=5`).
- The web UI is on port 8001 (`web_ui.port`). Publish it on 127.0.0.1, or switch the login on
  (`web_ui.authentication`, a Databricks workspace's OAuth; the steps are in the source repository's README)
  before publishing it more widely.

**Health check.** The image's `HEALTHCHECK` calls `GET /health` on the web UI's port every 30 s
(`CONNECTOR_HEALTH_PORT`, default 8001; set it if you change `web_ui.port`). It reports whether the process and
its web server answer, so `docker ps` shows `unhealthy` when the connector hangs. It doesn't say whether rows reach
Databricks: `GET /api/sparkplug/health` does, per source (`state`, `rows_written`, `zerobus_failing_since`,
`last_error`, the NBIRTH calls and the write-ahead log).

## Or run it with Python

```
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m sparkplug_connector --config my-config.yaml
```

## Web UI

`http://127.0.0.1:8001/`: Overview, **Sources** (add, edit, delete, start, stop; the NBIRTH procedure settings),
**ZeroBus** (endpoint, table, credentials, start and stop, diagnostics), Status, Metrics, Auth. A save goes to the
file the connector was started with (`--config`), `${env:...}` placeholders kept; the API never returns passwords
or secrets.

## License

The repository's [LICENSE.md](../../LICENSE.md), except the Eclipse Tahu-derived files, which stay under the
Eclipse Public License 2.0: [NOTICE.md](NOTICE.md).
