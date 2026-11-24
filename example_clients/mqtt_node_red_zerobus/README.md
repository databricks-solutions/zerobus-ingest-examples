# Node-RED to Zerobus Ingest over MQTT 5

> The `rescue` column is required. The table must have a `rescue VARIANT` column with the `zerobus-rescue` tag. Create the table with [`scripts/create_table.sql`](scripts/create_table.sql). Without the rescue column, one record that doesn't match the table closes the whole MQTT connection. With it, a mismatched record is stored in `rescue` and the connection stays up.

## Overview

This example shows how a [Node-RED](https://nodered.org/) flow publishes JSON records to a Unity Catalog Delta table through the [Zerobus Ingest MQTT 5 endpoint](https://docs.databricks.com/aws/en/ingestion/zerobus-mqtt). It uses only Node-RED's built-in `mqtt out` node and the `http request` node, with no extra palette packages.

The flow is a small dice-roll app. Every two seconds it rolls a six-sided die and publishes the roll as one row. Two more triggers show the operational parts: a 15-minute refresh that fetches a new OAuth token and reconnects with it, and a 30-second watch that fetches a new token and reconnects when the link is down. Three manual triggers send failing records, so you can see what each one does: a type mismatch, an extra field, and a payload that isn't JSON.

Use this example when your data already flows through Node-RED and you want to land it in a Delta table without running a message bus. The same flow pattern works for any JSON record that matches your table.

## Prerequisites

- Docker, to run the Node-RED image. The flow runs in the official `nodered/node-red` image and needs no extra palette packages.
- A Databricks workspace with the Zerobus Ingest MQTT endpoint enabled.
- A Unity Catalog table with the [rescue column](#schema). Zerobus Ingest doesn't create tables. Run [`scripts/create_table.sql`](scripts/create_table.sql), which creates the table, adds the `rescue VARIANT` column, and applies the `zerobus-rescue` tag. Wait at least 5 minutes after the tag is applied before you publish.
- A service principal with an OAuth secret, granted `USE CATALOG`, `USE SCHEMA`, `SELECT`, and `MODIFY` on the table. See [Create a service principal and grant permissions](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest#service-principal).
- Outbound TLS to the Zerobus Ingest hostname on port `8883`.

## Configuration reference

The flow reads each value from an environment variable. Nothing secret is stored in `flows.json`. The template is `config.example.env`.

| Variable | Purpose |
|---|---|
| `DATABRICKS_WORKSPACE_URL` | Workspace URL, `https://<workspace-url>`. The token request goes to `/oidc/v1/token` on this host. |
| `DATABRICKS_WORKSPACE_ID` | Numeric workspace ID. Used in the token request's `resource` value. |
| `DATABRICKS_CLIENT_ID` | Service principal application ID (OAuth client ID). |
| `DATABRICKS_CLIENT_SECRET` | Service principal OAuth secret. |
| `DATABRICKS_TABLE_NAME` | Three-level table name, `<catalog>.<schema>.<table>`. Sent in the CONNECT user property `x-databricks-zerobus-table-name`. |
| `ZEROBUS_MQTT_HOST` | MQTT hostname of the Zerobus Ingest endpoint, `<workspace-id>.zerobus.<region>.<cloud-domain>`, without `https://`. The flow connects to it on port 8883 with TLS. |
| `PORT` | Port for the Node-RED editor. `run-docker.sh` publishes it on the host's `127.0.0.1` only. Pick a free high port. |

The flow's timers and manual triggers are set in the flow editor:

| Trigger | Interval | What it does |
|---|---|---|
| Start | At startup | Fetches a token and connects. |
| Refresh | 15 minutes | Fetches a token and reconnects with `force`, so the new CONNECT carries it. |
| Watch | 30 seconds | If the link is down, fetches a new token and reconnects. Does nothing while the link is up. |
| Roll a die | 2 seconds | Publishes one valid record at QoS 1. |
| Type-mismatch record | On demand | Publishes one record whose `roll` is a string, not an INT. |
| Extra-field record | On demand | Publishes one valid record with an extra `extra_field` value. |
| Non-JSON payload | On demand | Publishes a payload that is not JSON. |

## Quick start

1. Create the table. Run [`scripts/create_table.sql`](scripts/create_table.sql) with your table name, then wait at least 5 minutes for the tag to take effect.

2. Copy the configuration template and fill in your values:

   ```bash
   cp config.example.env config.env
   ```

3. Run the container. The script loads `config.env`, copies `node-red/flows.json` and `node-red/settings.js` into `data/`, and starts Node-RED:

   ```bash
   ./scripts/run-docker.sh
   ```

4. Check the container log. You should see a token fetch and a connection:

   ```bash
   docker logs zerobus-node-red | grep -E "token|Connected to broker"
   ```

   The token line shows an 8-character fingerprint, not the token.

5. Open the editor at `http://127.0.0.1:<PORT>` (the `PORT` value from `config.env`). The flow shows the timers, the publish node, and the manual triggers.

6. After a minute or two, query the table. Zerobus Ingest writes rows asynchronously:

   ```sql
   SELECT count(*) AS n, max(rolled_at) AS last FROM <catalog>.<schema>.mqtt_node_red;
   ```

7. Stop the flow:

   ```bash
   docker stop zerobus-node-red && docker rm zerobus-node-red
   ```

<a id="schema"></a>

## Schema

The example table, from [`scripts/create_table.sql`](scripts/create_table.sql):

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.mqtt_node_red (
  roll_id BIGINT,
  player STRING,
  die_sides INT,
  roll INT,
  rolled_at STRING,
  rescue VARIANT
);

ALTER TABLE <catalog>.<schema>.mqtt_node_red
  ALTER COLUMN rescue SET TAGS ('zerobus-rescue');
```

The `rescue` column is required. Zerobus Ingest writes a field that doesn't fit the table's columns into `rescue` and keeps the connection open. Without the column, a record that doesn't match closes the MQTT connection, and publishes after it on that connection are lost until the flow reconnects.

Each valid record is one JSON object with these fields:

| Field | Type | Value |
|---|---|---|
| `roll_id` | BIGINT | `Date.now()` when the roll was made. Unique per roll in normal operation. |
| `player` | STRING | `node-red-demo` |
| `die_sides` | INT | `6` |
| `roll` | INT | A random integer from 1 to 6. |
| `rolled_at` | STRING | The roll time, ISO 8601 UTC. |

## Bad records

| Record | With the rescue column | Without the rescue column |
|---|---|---|
| Matches the table | Stored in its columns. | Stored in its columns. |
| Type mismatch (for example `roll` is a string) | Acknowledged, stored in `rescue`, connection stays up. The mismatched column is NULL. | The server closes the connection with reason 0x83 (implementation-specific error). |
| Extra field (for example `extra_field`) | Acknowledged, the extra field is stored in `rescue`, connection stays up. | The server closes the connection with reason 0x83. |
| Not JSON | Rejected with PUBACK `Payload format invalid` (0x99). Nothing is stored. Connection stays up. | Rejected with PUBACK 0x99. Nothing is stored. Connection stays up. |

Node-RED's `mqtt out` node doesn't pass a per-record result to the flow. A rejected record (0x99) shows up in the Node-RED log as `mqtt out: ErrorWithReasonCode: Publish error: Payload format invalid`, written by the flow's catch node. A record that is stored in `rescue` raises no error, so check the table to confirm it.

## How it works

- **Token**: The `Build token request` function reads the settings from the environment and builds an OAuth client credentials request scoped to the table: `USE CATALOG`, `USE SCHEMA`, and `SELECT`/`MODIFY` on the table, for the `zerobusDirectWriteApi` resource.
- **Connect**: The `Connect action` function sends a connect action to the `mqtt out` node. The token goes in the `Authorization` user property as `Bearer <token>`, and the table name goes in `x-databricks-zerobus-table-name`. Both are JSON strings in `msg.broker.userProperties`.
- **Publish**: The `mqtt out` node sends each record at QoS 1 on the topic that equals the table name.
- **Keep the link up**: The `Link state` status node records whether the `mqtt out` node is connected. While the link is up, the watch trigger does nothing. While the link is down, the watch trigger fetches a new token and sends the connect action, so each watch reconnect carries a fresh token.
- **Refresh**: Every 15 minutes the flow fetches a token and sends the connect action with `force: true`, which closes the current connection and opens a new one.
- **Fresh token per CONNECT**: Start, refresh, and watch reconnects each fetch a token before they connect, so every CONNECT carries a token fetched just before it.

The token is kept in flow context in memory. The flow logs only a short fingerprint, not the token.

## Limitations

- **Dropped records**: records published while the link is down are dropped. This includes a server-side close (for example, the connection's lifetime ends on the server) and a network loss. A publish during the short reconnect of a 15-minute refresh can be dropped too. The `mqtt out` node doesn't queue them and doesn't call back, so the flow gets no error. The watch trigger limits the gap: it checks every 30 seconds and, if the link is down, fetches a new token and reconnects, so a drop costs at most about 30 seconds of records plus the token request and connect time. In the test run for this version, a forced disconnect at 17:59:40 UTC reconnected at 18:00:17 UTC (37 seconds), and no rows were written in that gap. The watch trigger doesn't prevent loss. If you can't lose records, keep your own buffer and check the row count in the table.
- **Reconnects**: Node-RED doesn't reconnect by itself after the server closes the connection. The watch trigger does that.
- **Reconnect delay**: after a server disconnect, the reconnect waits for the next watch tick, up to 30 seconds. It fetches a new token first, so the reconnect uses a fresh token.
- **Without the rescue column**: a record that doesn't match the table closes the whole connection, with reason 0x83. The flow doesn't get a per-record error.

## How to extend

- **Other records**: Replace the `Demo record` function with your own function. Keep `msg.topic` set to `DATABRICKS_TABLE_NAME` and `msg.qos` set to 1, and make the payload match the table.
- **Other inputs**: Put your input nodes (for example `mqtt in`, `http in`, or a device node) in front of the publish path. Node-RED's `mqtt in` node subscribes to your own broker, not to Zerobus Ingest.
- **Your own retry policy**: The `mqtt out` node doesn't report per-record results to the flow, and drops made while the link is down are silent. Keep your own buffer of records, and verify delivery by counting rows in the table.
- **Editor security**: The editor listens on all interfaces inside the container, and `run-docker.sh` publishes it on the host's `127.0.0.1` only. Don't publish it on other addresses, and don't run the container with `--network host` (use `UI_HOST=127.0.0.1` there). Anyone who can use the editor can read the environment variables, including the client secret. For more than a local test, add `adminAuth` to `settings.js`.

## Files

- `scripts/create_table.sql`: creates the table with the rescue column and applies the `zerobus-rescue` tag.
- `node-red/flows.json`: the flow (no secrets, no hosts; each value comes from the environment).
- `node-red/settings.js`: the editor listens on all container interfaces; the port comes from `PORT` (`UI_HOST` overrides the bind address).
- `config.example.env`: the variables, with placeholders only.
- `scripts/run-docker.sh`: copies the flow and settings into `data/`, then starts the container with the variables and publishes the editor on `127.0.0.1:${PORT}`.
- `tests/test_flows.py`: static checks on the flow, the settings, and the table script. Run with `python3 -I tests/test_flows.py`.
