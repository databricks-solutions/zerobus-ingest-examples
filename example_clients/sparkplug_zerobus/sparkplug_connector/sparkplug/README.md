# Sparkplug B source: the reference

A `sparkplug` source connects the connector to an MQTT server that carries **Sparkplug B** (for example
Ignition with MQTT Transmission publishing to a Chariot MQTT server, or any other
Sparkplug 3.0 edge) and writes **one row per Sparkplug message** to a Delta table through Zerobus.

- **One row per message**: the `sparkplug_raw` table below, `msg` holding the payload as Eclipse Tahu's
  JSON, byte for byte what Java Tahu produces; splitting it into metrics is left to SQL.
- **Sparkplug primary host application**: with `primary_host_id` set, the edges buffer their data
  (store and forward) whenever the rows can't reach Databricks, and send it once they can again.
- **Confirmed writes**: a row leaves the connector only once Zerobus has acknowledged it; an optional
  write-ahead log keeps unacknowledged rows across a crash.
- **NBIRTH procedure call**: on each edge node birth it can call a stored procedure of yours, by default
  `create_edge_node_schema`, to (re)build per-model views.

Several sources can run in one connector (each target table has its own queue and writer). Running it,
Docker and the web UI: the [repository README](../../README.md).

## What gets stored

| Column | Value |
|---|---|
| `msg_id` | a new UUID per row |
| `msg_topic`, `namespace`, `group_id`, `message_type`, `edge_node_id`, `device_id` | from the topic `spBv1.0/<group>/<type>/<edge>[/<device>]` (`device_id` null for node messages) |
| `msg` | the payload as Tahu JSON (`tahu_json.py`; GZIP/DEFLATE-compressed payloads decompressed) |
| `inserted_at` | epoch milliseconds at receipt |

Stored: births, deaths and data of edge nodes and devices (NBIRTH, NDATA, NDEATH, DBIRTH, DDATA,
DDEATH). Skipped (counted; the last two also logged): host
STATE messages, other namespaces, commands (NCMD, DCMD), malformed `spBv1.0/...` topics and payloads that
can't be decoded. The payload is kept whole: splitting it into metrics is left to SQL.

## Databricks setup

The table (Unity Catalog; the names below are examples, any names work):

```sql
CREATE TABLE IF NOT EXISTS bridge_stage_db.stage_db.sparkplug_raw (
  msg_id        STRING,
  msg_topic     STRING,
  namespace     STRING,
  group_id      STRING,
  message_type  STRING,
  edge_node_id  STRING,
  device_id     STRING,
  msg           VARIANT,
  inserted_at   DECIMAL(38,0)
)
CLUSTER BY (message_type, group_id, edge_node_id, device_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

A service principal with an OAuth secret writes it through Zerobus (the Zerobus endpoint is
`https://<workspace-id>.zerobus.<region>.cloud.databricks.com`):

```sql
GRANT USE CATALOG ON CATALOG bridge_stage_db TO `<application id>`;
GRANT USE SCHEMA ON SCHEMA bridge_stage_db.stage_db TO `<application id>`;
GRANT MODIFY, SELECT ON TABLE bridge_stage_db.stage_db.sparkplug_raw TO `<application id>`;
```

(`SELECT` lets the NBIRTH call check that the birth has landed.) For the **NBIRTH call** the same
service principal also needs the workspace entitlement `databricks-sql-access`, `CAN USE` on the SQL
warehouse, and in each node catalog: `USE CATALOG` and `CREATE SCHEMA` on the catalog, `USE SCHEMA` on
the node schema, `EXECUTE` on the procedure and `SELECT` on what that procedure reads. The call is
`CALL <catalog>.<schema>.<procedure>(<prefix>, <group id>, <edge node id>, false)` for each procedure in
`edge_node_schemas.procedures` (positional, so any procedure taking `STRING, STRING, STRING, BOOLEAN`
works: the schema prefix, the group id, the edge node id and whether to recreate); the procedure is part of your Databricks
SQL, not of the connector. Only NBIRTHs call it: the template definitions the views are built from come
in the NBIRTH, and a DBIRTH is handled like data. The service principal then owns the
`<prefix>_<group>_<edge node>` schemas it creates, so readers need `USE CATALOG, USE SCHEMA, SELECT` on
the node catalog.

## Configuration

```yaml
sources:
  - name: chariot-sparkplug
    protocol: sparkplug
    endpoint: mqtt://chariot:1883          # or mqtts://host:8883 (TLS: security:, below)
    # target_table: bridge_stage_db.stage_db.sparkplug_raw   # its own table; default: zerobus.default_target
    client_id: databricks-sparkplug        # fixed and unique
    username: ...                          # ACL: subscribe spBv1.0/#, publish its own STATE topic
    password: ...
    # topics: ["spBv1.0/#"]                # default; qos defaults to 0 (Sparkplug publishes at QoS 0)
    primary_host_id: databricks-zerobus    # = the edges' Primary Host ID; omit: a plain subscriber
    state_format: v3                       # v3: spBv1.0/STATE/<id> JSON; v2: legacy STATE/<id> ONLINE/OFFLINE
    offline_after_seconds: 30              # OFFLINE (and disconnect) after Zerobus writes failed this long
    online_after_seconds: 30               # ONLINE again after Zerobus has been healthy this long
    edge_node_schemas:                     # on NBIRTH: CALL <procedure>(prefix, group, edge, false)
      enabled: true
      warehouse_id: ...                    # SQL warehouse; the Zerobus service principal runs the call
      procedures: [bridge_node_db.stage_db.create_edge_node_schema]   # catalog.schema.procedure, one or more
      schema_prefix: nb
    # security:                            # TLS (protocols/mqtt_security.py); on with mqtts:// or use_tls: true
    #   ca_cert_path: /certs/ca.pem        # a private CA (default: the system's)
    #   client_cert_path: /certs/client.pem   # client certificate and key: mutual TLS
    #   client_key_path: /certs/client.key
    #   tls_version: TLSv1_2               # the minimum
    #   check_hostname: true
    #   insecure: false                    # true: no certificate check (testing only)
zerobus:
  enabled: true
  zerobus_endpoint: https://<workspace-id>.zerobus.<region>.cloud.databricks.com
  auth: {client_id: ..., client_secret: ...}   # the service principal
  # drain_timeout_seconds: 15             # on stop: write what is queued first, for at most this long
  # wal:                                  # write-ahead log: rows on disk until Zerobus acknowledged them
  #   enabled: true                       # off without it; on in the example config.yaml
  #   path: wal                           # one subdirectory per target table
  #   fsync_interval_seconds: 0.2         # each row reaches the OS at once; fsync this often
  #   segment_mb: 16
  #   max_size_mb: 1024                   # full: rows are queued without the log (counted)
  # batch: {max_records: 500, linger_seconds: 0.2}
  default_target:                          # the table above
    workspace_host: https://<workspace>.cloud.databricks.com
    catalog: bridge_stage_db
    schema: stage_db
    table: sparkplug_raw
```

The package's `config/config.yaml` has this source as `sparkplug-example` (off): enable it and set its
table, endpoint and credentials there (its header lists the environment variables). Run **exactly one** primary host per Primary Host ID: a second
one with the same id fights over STATE, and two connectors with different ids write every row twice.

### The edge (MQTT Transmission)

- **Primary Host ID** = the source's `primary_host_id` (in MQTT Transmission 5.x it's set on the server
  set), and **store and forward** on with a history store sized for the longest outage to survive.
- **Convert UDTs off**: UDT instances go out as Sparkplug templates (members, parameters, definitions in
  the NBIRTH), stored as they arrive.
- **Aliased Tags off** (the default): aliases are not resolved (see *Limitations*).

## How it behaves

- **ONLINE:** connect with a will (OFFLINE, QoS 1, retained) stamped with the connect time T, subscribe
  (own STATE too), publish ONLINE with T. A live OFFLINE for its own id while ONLINE is answered with
  ONLINE again (`state_republished`); the retained copy of the previous session's state is not.
- **OFFLINE** (`host.py`): when Zerobus writes have failed, or a batch has waited for its
  acknowledgement, for `offline_after_seconds`, or the queue is above its high-water mark (80%), it
  publishes OFFLINE and disconnects (Sparkplug 3.0 doesn't allow ONLINE again on the same connection).
  The edges then send NDEATH, disconnect and buffer. Once writes have been healthy for
  `online_after_seconds` it reconnects with a new will and goes ONLINE; the edges rebirth and flush their
  history (flagged `isHistorical`).
- **Start-up:** OFFLINE until the first Zerobus stream has opened, then ONLINE at once.
- **Zerobus stopped** (the web page's switch, or not started yet): OFFLINE at once, since nothing is
  written; ONLINE again `online_after_seconds` after it writes again. A batch sent but not acknowledged
  when it stopped is sent first by the next writer.
- **Stop (SIGTERM):** OFFLINE first, so the edges start buffering, then up to `drain_timeout_seconds` to
  write what is queued (with the write-ahead log, what's left is written after the next start).
- **Tables:** a source writes to its `target_table` (`catalog.schema.table`; also in the web page's source
  form), else to `zerobus.source_targets.<name>`, else to `zerobus.default_target`. A table is opened when an
  enabled source that writes to it starts (a disabled source's table only once it is enabled and started).
- **Queues:** one per target table (`backpressure.memory_queue`: `max_size`, default 10000; `drop_policy`
  `oldest`, `newest` or `reject` when full), in memory; a source's rows go only to its own target's queue.
  A primary host goes OFFLINE at 80% full, so the edges buffer instead; a plain subscriber can fill it.
- **Writes** (`core/confirmed_writer.py`): each batch is kept until Zerobus acknowledges it; on any failure
  the stream is closed and the same batch resent on a fresh one (backoff 1 s doubling to 30 s), so rows
  are written at least once with their `msg_id`. A row over Zerobus's 10,420,224-byte record limit is
  skipped and counted (`rows_too_large`) instead of being retried forever.
- **Write-ahead log** (`core/wal.py`, `zerobus.wal`; off unless enabled, on in the example `config.yaml`): each row is appended to the
  target's log before it is queued and marked done once Zerobus acknowledged it (or the queue dropped it).
  After a crash, the next start queues the rows not done first, with their `msg_id`: the batch in flight
  and everything queued during a Databricks outage, which would otherwise be lost (Sparkplug data is QoS
  0, so the edge counts a received message as delivered). A row reaches the OS when appended (a process
  crash loses nothing) and is fsynced every 200 ms (a power loss loses at most that). Rows acknowledged
  out of order can be written twice after a crash (same `msg_id`).
- **NBIRTH** (`edge_node_schemas.py`): once Zerobus has acknowledged an NBIRTH and it is readable in the
  table, each NBIRTH procedure is called, off the MQTT and Zerobus paths; NBIRTHs of one
  edge node within 2 s make one call; a failed call is retried for ~5 minutes, then given up and counted.

## Health

`GET /api/sparkplug/health` (the web UI's port), per Sparkplug source: `state`, `mqtt_connected`,
`mqtt_online`, `state_topic`, `messages_received`, `rows_queued`, `messages_skipped`, `topics_malformed`,
`payloads_undecodable`, `state_republished`, `rows_failed`, `queue_depth`, `queue_high_water`, `queue_dropped`,
`zerobus_stream_open`, `rows_written`, `rows_too_large`, `rows_unconvertible`, `zerobus_failures`, `batch_in_flight`,
`zerobus_failing_since`, `batch_pending_since`, `zerobus_last_success`, `last_error`; with
`edge_node_schemas`: `nbirths_written`, `edge_node_schema_calls`, `..._failures`, `..._gave_up`,
`..._pending`, `..._last_error`; with the write-ahead log: `wal_pending`, `wal_rows_appended`,
`wal_rows_replayed`, `wal_rows_not_logged`, `wal_bytes`, `wal_failures`, `wal_last_error`. `/api/metrics`
has the same counters under `clients.<name>.sparkplug`, the writers under `zerobus_targets` and the logs
under `wal`.

## Web page

The connector's web UI (the [repository README](../../README.md#web-ui)) lists the sources with their state
(`yes · ONLINE`) and primary host, and adds, edits and deletes them (asking before a delete): endpoint,
target table, client id, primary host id, STATE format, offline/online seconds and the NBIRTH settings
(switch, SQL warehouse, schema prefix, the procedure). The username, password, topics and TLS settings stay
in the configuration file. A save restarts the source and writes the file the connector was started with
(`--config`): fields the form doesn't show and unchanged fields stay as written there, `${env:...}`
placeholders included; `GET /api/sources` masks passwords and secrets.

## Limitations

- **Aliases aren't resolved**: a metric sent by alias only is stored as received (no `name`). MQTT
  Transmission (5.0.4) doesn't send aliases in the setup above anyway: it refuses "Aliased Tags" while UDTs
  are published as templates and while store and forward is on.
- **Number spelling**: the JSON spells numbers as Tahu does on Java 17; from 2^63 up, a few floats and
  doubles would get a different last digit than on Java 19+ (`java_format.py`).
- **No live log view** on the web page (logs go to the console and the connector's log files).
- **No rebirth requests**: the connector never sends NCMD. As a plain subscriber it sees births only when
  the edges send them (press Rebirth after starting it).
- **No HTTPS proxy setting** for Zerobus.

## Files

`raw_row.py` (topics, skip rules, rows), `tahu_json.py` + `java_format.py` (Tahu's JSON; see `NOTICE.md`
for the EPL-2.0 parts), `host.py` (STATE, HostState, HostSupervisor), `edge_node_schemas.py` (NBIRTH
calls), `../protocols/sparkplug_client.py` (the source), `../core/confirmed_writer.py`, `../core/wal.py`.
Tests: `tests/` (Eclipse Tahu's Java implementation as the reference, through JPype:
`java_tahu.py`; a real Mosquitto for the primary host: `test_primary_host.py`; test-only dependencies in
`tests/requirements.txt`). The payload fixtures are synthetic, shaped like MQTT Transmission's
(`make_fixtures.py`); `make_expected.py` writes Java Tahu's JSON for them.
