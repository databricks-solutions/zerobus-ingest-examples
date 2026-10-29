# Sparkplug B source: the reference

A `sparkplug` source connects the connector to an MQTT server that carries **Sparkplug B** (for example
Ignition with MQTT Transmission publishing to a Chariot MQTT server, or any other
Sparkplug 3.0 edge) and writes **one row per Sparkplug message** to a Delta table through Zerobus.

- **One row per message**: the `sparkplug_raw` table below, `msg` holding the payload as Eclipse Tahu's
  JSON, byte for byte what Java Tahu produces, with Ignition's extra types (below); splitting it into metrics is
  left to SQL.
- **Sparkplug primary host application**: with `primary_host_id` set, the edges buffer their data
  (store and forward) whenever the rows can't reach Databricks, and send it once they can again.
- **Confirmed writes**: a row leaves the connector only once Zerobus has acknowledged it; an optional
  write-ahead log keeps unacknowledged rows across a crash.
- **NBIRTH procedure call**: on each edge node birth it can call a stored procedure of yours, by default
  `create_edge_node_schema`, to (re)build per-model views.
- **Strict Sparkplug sequencing** (by default): `seq` reordering, drop-until-rebirth and rebirth requests (NCMD)
  as Eclipse Tahu's host library decides them, or a row for every message (`sequence_handling`, below).

Several sources can run in one connector (each target table has its own queue and writer). Running it,
Docker and the web UI: the [repository README](../../README.md).

## What gets stored

| Column | Value |
|---|---|
| `msg_id` | a new UUID per row |
| `msg_topic`, `namespace`, `group_id`, `message_type`, `edge_node_id`, `device_id` | from the topic `spBv1.0/<group>/<type>/<edge>[/<device>]` (`device_id` null for node messages) |
| `msg` | the payload as Tahu JSON (`tahu_json.py`; GZIP/DEFLATE-compressed payloads decompressed) |
| `inserted_at` | epoch milliseconds at receipt (a message held for a missing `seq`: when it is put back in order) |

Stored: births, deaths and data of edge nodes and devices (NBIRTH, NDATA, NDEATH, DBIRTH, DDATA,
DDEATH); with `sequence_handling: strict` (the default) only those its checks pass (below). Skipped
(counted; the last two also logged): host STATE messages, other namespaces, commands (NCMD, DCMD), malformed
`spBv1.0/...` topics and payloads that can't be decoded. The payload is kept whole: splitting it into
metrics is left to SQL. A metric sent without a datatype (allowed outside births) gets its type from the
edge node's or device's last birth, as a Tahu host application decodes it.

**Ignition's extra types.** Ignition's MQTT Transmission sends types Sparkplug B doesn't define; they decode as an
Ignition-aware Java host decodes them (Eclipse Tahu alone drops the metric that carries one):

| In the payload | In `msg` |
|---|---|
| Property types DataSet (16), Array (-10001), Document (-10002) | `{"type":"Document","value":"<the property's string>"}` (likewise `DataSet`, `Array`) |
| DataSet column type Color (-10000) | `"types"` lists `"Color"`; each cell is a `java.awt.Color` as Jackson writes it: `red`, `green`, `blue`, `alpha` (always 255), `colorSpace` (Java's sRGB profile, ~9.5 KB, the same in every cell), `transparency`, `rgb`. Java orders these keys differently from one start to the next |
| `metaData.contentType` `Alarm` | a `String` metric with the alarm's text, whatever its datatype |
| `metaData.contentType` `AlarmReconciliation` | a `DataSet` metric, whatever its datatype |

Also as such a host does: a property that can't be decoded is left out (not its metric), and a null property value
is left out of the JSON (`{"type":"String"}`, where Tahu writes `"value":null`).

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
the node catalog. The check after the call (`check_views`) reads the node catalog's `information_schema`, where the
service principal sees the schemas and views it owns: no further grant.

## Configuration

```yaml
sources:
  - name: chariot-sparkplug
    protocol: sparkplug
    endpoint: mqtt://chariot:1883          # or mqtts://host:8883 (TLS: security:, below)
    # target_table: bridge_stage_db.stage_db.sparkplug_raw   # its own table; default: zerobus.default_target
    client_id: databricks-sparkplug        # fixed and unique
    username: ...                          # ACL: subscribe spBv1.0/#, publish its own STATE topic and spBv1.0/+/NCMD/+
    password: ...
    # topics: ["spBv1.0/#"]                # default; qos defaults to 0 (Sparkplug publishes at QoS 0)
    primary_host_id: databricks-zerobus    # = the edges' Primary Host ID; omit: a plain subscriber
    state_format: v3                       # v3: spBv1.0/STATE/<id> JSON; v2: legacy STATE/<id> ONLINE/OFFLINE
    offline_after_seconds: 30              # OFFLINE (and disconnect) after Zerobus writes failed this long
    online_after_seconds: 30               # ONLINE again after Zerobus has been healthy this long
    sequence_handling: strict              # strict: seq order, drops until the rebirth; keep_all: a row for every message
    sequence_reordering_timeout: 5000      # ms to wait for a missing seq
    block_auto_rebirth: false              # true: no rebirth requests (only with keep_all; see below)
    edge_node_schemas:                     # on NBIRTH: CALL <procedure>(prefix, group, edge, false)
      enabled: true
      warehouse_id: ...                    # SQL warehouse; the Zerobus service principal runs the call
      procedures: [bridge_node_db.stage_db.create_edge_node_schema]   # catalog.schema.procedure, one or more
      schema_prefix: nb
      check_views: true                    # after each call: the schema and the models' views exist (below)
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
table, endpoint and MQTT user there (its header says what to set; the secrets come from the environment). Run **exactly one** primary host per Primary Host ID: a second
one with the same id fights over STATE, and two connectors with different ids write every row twice.

### The edge (MQTT Transmission)

- **Primary Host ID** = the source's `primary_host_id` (in MQTT Transmission 5.x it's set on the server
  set), and **store and forward** on with a history store sized for the longest outage to survive.
- **Convert UDTs off**: UDT instances go out as Sparkplug templates (members, parameters, definitions in
  the NBIRTH), stored as they arrive.
- **Aliased Tags off** (the default): aliases are not resolved (see *Limitations*).

## How it behaves

- **ONLINE:** connect with a will (OFFLINE, QoS 1, retained) stamped with the connect time T, subscribe
  (its own STATE topic too, a filter of its own even under `spBv1.0/#`), publish ONLINE with T. A live OFFLINE
  for its own id while ONLINE is answered with ONLINE again (`state_republished`), once even when the MQTT
  server sends the OFFLINE once per matching filter; the retained copy of the previous session's state is not.
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
  edge node within 2 s make one call; one call at a time per edge node (an NBIRTH arriving during a call waits
  for it, then runs); a failed call is retried for ~5 minutes, then given up and counted. Sources sharing a
  table each call their own procedures for the NBIRTHs they received only (an NBIRTH no source received, the
  diagnostics' or one replayed from the write-ahead log, goes to the table's first source with NBIRTH calls).
  With `check_views` (on by default), after each call of a procedure named `create_edge_node_schema` (others in
  the list are called, not checked) the connector looks in the procedure's catalog
  (`information_schema`) for the edge node's schema, `<prefix>_<group>_<edge node>`, and for each model of the
  NBIRTH (each top-level Template metric) its views `<model>`, `<model>_vw` and `<model>_asof_vw`, named as
  `create_edge_node_schema` names them (lower case, each run of characters other than `[A-Za-z0-9]` -> `_`); an
  NBIRTH without models needs the schema only. Something missing counts as a failed call: the procedure is called
  again at the next retry. Switch it off for a `create_edge_node_schema` of your own that makes other objects.
- **Diagnostics** (`diagnostics.py`, `diagnostics.enabled`, off by default): the connector's own edge nodes,
  whose rows it writes into `zerobus.default_target` itself (nothing on MQTT), in the group `diagnostics.group_id`
  (default `BRIDGE`), each with one device named after its model; templates version `v2.0.0`:

  | Edge node (setting, default) | Device, instances | Metrics |
  |---|---|---|
  | `bridge_info_edge_node_id`, `Bridge Info` | `<bridge_model>` (`diagnostics.bridge_model`, default `BRIDGE`), `Core/<bridge_model>` | `<bridge_model> Version`, `PrimaryHostId` (the first source's), `ConnectedEdgeNodes`, `DisconnectedEdgeNodes`, `MQTTMessageCount`, `ModelCount`, `InstanceCount` |
  | `edge_node_info_edge_node_id`, `Edge Node Info` | `EdgeNode`, `Core/EdgeNode-<edge node id>` per edge node seen | `GroupId`, `EdgeNodeId`, `Connected`, `ConnectDateTime`, `DisconnectDateTime`, `BirthCount`, `DeathCount`, `MQTTMessageCount` |
  | `mqtt_client_info_edge_node_id`, `MQTT Client Info` | `MQTTClient`, `Core/MQTTClient` per Sparkplug source | `ClientID`, `Connected`, `ConnectDateTime`, `DisconnectDateTime`, `ServerUrl`, `ServerName` (the source's name) |

  At start an NBIRTH (`bdSeq`, the template definitions) and a DBIRTH (the instances) per edge node, then every
  10 s a DDATA per edge node if anything changed; no NDEATH. The JSON is the shape a Java host builds these payloads
  in, its quirks kept: each metric stamped with the time it was built; dates as Java's `Date.toString()`
  (`Tue Sep 29 11:48:45 EDT 2026`, the process's time zone; epoch 0 when unknown, `"null"` for an MQTT client never
  connected); `Connected` a String in the EdgeNode definition and in MQTTClient, a Boolean in EdgeNode instances; the
  Edge Node Info DBIRTH also carries an all-zero instance for itself. The counters follow the rows the sources hand
  on (in `strict` mode, the messages the sequencing checks pass) and their connections. Their NBIRTHs call the NBIRTH
  procedures like any edge node's; the check after the call needs their schema only. `GET /api/status`:
  `diagnostics.rows_queued`.

## Rows and rebirth requests

These are the decisions of Eclipse Tahu 1.0.21's host library; `sequencer.py` is a port of them, quirks included,
tested against Tahu's own classes. They hold as a plain subscriber and as primary host:

- **Reordering:** one expected `seq` per edge node, for its messages and its devices' (0-255, then 0 again;
  NBIRTH resets it to 0; NDEATH isn't counted). A message with another `seq` is held. When no message has come
  in order for `sequence_reordering_timeout` (checked every second), the edge node's held messages are dropped
  and a rebirth requested. An NBIRTH drops the held messages older than itself.
- **Drops until the rebirth:** after that timeout the edge node counts as offline: its data is dropped (and
  asked for a rebirth again, at most every 5 s) until its NBIRTH. Also dropped: data from an edge node or device
  never born since the connector started (it asks for a rebirth), an NDEATH from an unknown or offline edge node
  or with a `bdSeq` other than its NBIRTH's, a DDEATH for an offline device, a message without `seq`, and (a Tahu
  quirk) everything after a DDEATH for an offline device until the next NBIRTH.
- **Rebirth requests:** an NCMD on `spBv1.0/<group>/NCMD/<edge node>` (QoS 0, not retained) with a timestamp and
  `Node Control/Rebirth` = true, at most one per edge node per 5 s. A connector that starts while the edges
  publish therefore asks each edge node for a rebirth about 5 s after its first message. The MQTT user needs
  publish rights on `spBv1.0/+/NCMD/+`: Chariot closes the connection on a publish its ACL refuses, so when the
  connection is lost within 2 s of an NCMD twice in a row, the source sends no more until it restarts, logs an
  ERROR naming the ACL and writes every message from then on (`keep_all`).
- **On request:** an operator can ask an edge node for a rebirth (the web page's **Edge nodes**, or
  `POST /api/sources/<source>/rebirth` with `{"group_id": ..., "edge_node_id": ...}`; the start/stop
  permission): the same NCMD, sent at once (an automatic one for that edge node then waits the 5 s). It isn't an
  automatic request, so `block_auto_rebirth` doesn't stop it;
  after a refusal (above) none is sent. The connector sends no other commands: no DCMD and no metric writes.
- **`keep_all`:** every message gets its row at once; the same decisions run alongside, so the rebirth requests are
  the same and the health counts what strict would have dropped. `block_auto_rebirth: true` (no rebirth requests)
  needs it: strict ends an edge node's drops only with a rebirth.
- **Compressed payloads** (GZIP/DEFLATE) are checked decompressed: Tahu's own checks read them as received, where
  only `seq` is outside, so a compressed birth would never pass.

Messages held for a missing `seq` are in memory only: a crash or stop loses them (counted at a stop as
`shutdown`), as in a Tahu host application.

## Health

`GET /api/sparkplug/health` (the web UI's port), per Sparkplug source: `state`, `mqtt_connected`,
`mqtt_online`, `state_topic`, `messages_received`, `rows_queued`, `messages_skipped`, `topics_malformed`,
`payloads_undecodable`, `state_republished`, `rows_failed`, `queue_depth`, `queue_high_water`, `queue_dropped`,
`zerobus_stream_open`, `rows_written`, `rows_too_large`, `rows_unconvertible`, `zerobus_failures`, `batch_in_flight`,
`zerobus_failing_since`, `batch_pending_since`, `zerobus_last_success`, `last_error`; with
`edge_node_schemas`: `nbirths_written`, `edge_node_schema_calls`, `..._checked` (calls whose schema and views were
found), `..._failures`, `..._gave_up`, `..._pending`, `..._last_error`; with the write-ahead log: `wal_pending`, `wal_rows_appended`,
`wal_rows_replayed`, `wal_rows_not_logged`, `wal_bytes`, `wal_failures`, `wal_last_error`; the sequence
handling: `sequence_handling`, `messages_held`, `messages_reordered`, `messages_dropped` and `dropped` (by
reason: `gap_timeout`, `pruned_at_nbirth`, `replaced_seq`, `no_seq`, `unknown_node`, `node_offline`,
`unknown_device`, `device_offline`, `no_bdseq`, `bdseq_mismatch`, `seq_check`, `invalid_birth`, `undecodable`,
`shutdown`; in `keep_all` what strict would drop), `rebirth_requests` (decided), `rebirth_requests_sent`,
`..._rate_limited`, `..._blocked`, `..._failed` (not connected), `rebirth_requests_on_request` (the operator's), `..._not_sent_refused`, `rebirth_refused` (true
once the MQTT server refused them), `block_auto_rebirth`, `duplicate_aliases`, `sequence_errors` (messages written
as received because the sequencing failed). `/api/metrics`
has the same counters under `clients.<name>.sparkplug`, the writers under `zerobus_targets` and the logs
under `wal`.

`GET /api/sparkplug/edge-nodes`, per Sparkplug source: each edge node the sequencing knows (`group_id`,
`edge_node_id`, `online`, `since` in epoch ms and `since_iso`, `changed_by`, `bdseq`) and its `devices`
(`device_id`, `online`, `since`, `since_iso`, `changed_by`), with Sparkplug 3.0's times: a birth's own
timestamp (`NBIRTH`, `DBIRTH`), an NDEATH's receipt (the host's UTC time: the MQTT server may publish it as the
will; its devices go offline then too), a DDEATH's timestamp, and the host's time when the reorder timeout sets an
edge node offline (`reorder timeout`). The web page's **Edge nodes** shows the same. Metric values aren't kept,
so nothing is marked STALE here: that shows in the tables downstream.

## Web page

The connector's web page (the [repository README](../../README.md#web-ui)): **Sources** lists the sources with
their state and role (primary host or plain subscriber), starts and stops them, and adds, edits and deletes them
(asking before a delete): endpoint, username and password, client id, keep-alive, topics, target table, TLS files
(CA, client certificate and key, from the **Certificates** page, and hostname checking), primary host id, STATE format,
offline/online seconds, Rows (`sequence_handling`), Reordering timeout, Block auto rebirth, and the NBIRTH call
(switch, SQL warehouse, procedures, schema prefix, the views check). A save restarts the source and writes the file
the connector was started with (`--config`): fields the dialog doesn't show and unchanged fields stay as written
there, `${env:...}` placeholders included; the password is the name of an environment variable (saved as
`${env:NAME}`; the dialog shows whether it is set; left empty with a username: `SPARKPLUG_MQTT_PASSWORD`), never its value; `GET /api/sources` masks passwords and secrets. **Edge
nodes** lists each running source's edge nodes and devices (`GET /api/sparkplug/edge-nodes`, under *Health*) with a
**Request rebirth** button per edge node; **Status** shows each source's health. The diagnostics' settings are on
**Settings**.

## Limitations

- **Aliases aren't resolved**: a metric sent by alias only is stored as received (no `name`; its type comes
  from the birth). MQTT
  Transmission (5.0.4) doesn't send aliases in the setup above anyway: it refuses "Aliased Tags" while UDTs
  are published as templates and while store and forward is on.
- **Number spelling**: the JSON spells numbers as Tahu does on Java 17; from 2^63 up, a few floats and
  doubles would get a different last digit than on Java 19+ (`java_format.py`).
- **No live log view** on the web page (logs go to the console and the connector's log files).
- **No HTTPS proxy setting** for Zerobus.

## Files

`raw_row.py` (topics, skip rules, rows), `tahu_json.py` + `java_format.py` (Tahu's JSON; see `NOTICE.md`
for the EPL-2.0 parts), `host.py` (STATE, HostState, HostSupervisor), `edge_node_schemas.py` (NBIRTH
calls), `../protocols/sparkplug_client.py` (the source), `../core/confirmed_writer.py`, `../core/wal.py`.
Tests: `tests/` (Eclipse Tahu's Java implementation as the reference, through JPype:
`java_tahu.py`; a real Mosquitto for the primary host: `test_primary_host.py`; test-only dependencies in
`tests/requirements.txt`). The payload fixtures are synthetic, shaped like MQTT Transmission's
(`make_fixtures.py`); `make_expected.py` writes Java Tahu's JSON for them. Ignition's extra types:
`ignition_payloads.py`, with the JSON of a Java decoder that has them in `expected/ignition/`
(`test_ignition_extensions.py`); the Tahu tests run with them off (`tahu_json.plain_tahu()`).
