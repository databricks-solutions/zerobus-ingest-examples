# Paho MQTT (Python) to Zerobus Ingest

## Overview

A long-running Python publisher that sends JSON records to a Delta table through the Zerobus Ingest MQTT v5 endpoint, using [Eclipse Paho MQTT](https://eclipse.dev/paho/files/paho.mqtt.python/html/) 2.x. Use it when your application already publishes MQTT and you want its records in a Unity Catalog Delta table without running a message bus.

The example is a small dice-roll simulator. Each roll is one JSON object. The publisher demonstrates the parts a long-running service needs beyond the basic example in the [MQTT documentation](https://docs.databricks.com/aws/en/ingestion/zerobus-mqtt):

- **Reconnect with a new token**: Each connection fetches a fresh table-scoped OAuth token and sends it in new `CONNECT` properties. Connections have a bounded lifetime, so the server closes them, and the publisher reconnects through `ensure_connected()`, which your loop has to call (see [Delivery and failure behavior](#delivery-and-failure-behavior)).
- **PUBACK tracking**: Every QoS 1 record is tracked by its PUBLISH packet ID until its `PUBACK` arrives with a successful reason code. Failed or missing `PUBACK`s are retried with backoff.
- **Clean shutdown**: On `SIGINT` or `SIGTERM` the publisher stops producing, waits for outstanding records, sends `DISCONNECT`, and stops the network thread.

For the basic connect-and-publish flow, see the [Python example](https://docs.databricks.com/aws/en/ingestion/zerobus-mqtt#python-example). This example doesn't repeat it.

> The rescue column is required. Your table needs a nullable `rescue VARIANT` column tagged `zerobus-rescue`, as [Schema](#schema) shows. Without it, one record that doesn't match the table closes the MQTT connection. With it, a type mismatch or an extra field is stored in `rescue` and the connection stays open.

## Prerequisites

- MQTT enabled for your workspace, and Zerobus Ingest available in your region.
- An existing Unity Catalog Delta table with the rescue column (`rescue VARIANT`, tagged `zerobus-rescue`). It is required. See [Schema](#schema). Zerobus Ingest doesn't create tables.
- A service principal with `USE CATALOG`, `USE SCHEMA`, `SELECT`, and `MODIFY` on the table. See [Create a service principal and grant permissions](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest#service-principal).
- Python 3.10 or later, with `pip`. The pinned `requests` release needs Python 3.10.
- Outbound TLS to `<workspace-id>.zerobus.<region>.<cloud-domain>` on port `8883`.

## Configuration reference

Set these environment variables. `config.example.env` shows each one with a placeholder value.

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABRICKS_WORKSPACE_URL` | Yes | Workspace URL, for example `https://<workspace-url>`. Used to request the token. |
| `DATABRICKS_WORKSPACE_ID` | Yes | Workspace ID. Used in the token's `zerobusDirectWriteApi` resource. |
| `DATABRICKS_CLIENT_ID` | Yes | Service principal client ID. |
| `DATABRICKS_CLIENT_SECRET` | Yes | Service principal client secret. Never commit it. |
| `DATABRICKS_TABLE_NAME` | Yes | Target table as `catalog.schema.table`. The publish topic and the `x-databricks-zerobus-table-name` property use it. |
| `ZEROBUS_MQTT_HOST` | Yes | MQTT hostname, `<workspace-id>.zerobus.<region>.<cloud-domain>`, without `https://`. |

Command-line options for `src/dice_roller.py`:

| Option | Default | Description |
|--------|---------|-------------|
| `--rolls` | `20` | Number of rolls to publish. |
| `--interval` | `0.5` | Seconds between rolls. |
| `--force-reconnect-after` | `0` (off) | Drop the connection after this many rolls and reconnect with a new token. Use it to test reconnect. |
| `--send-invalid-record` | off | Also send the three bad records described in [Delivery and failure behavior](#delivery-and-failure-behavior): a type mismatch, an extra field, and a non-JSON payload. Use it to see how your table handles bad records. |
| `--seed` | random | Random seed, for repeatable rolls. |
| `--verbose` | off | Log each PUBLISH and PUBACK. Logs no tokens. |

Retry limits are constants in `src/zerobus_mqtt.py`:

| Constant | Value | Applies to |
|----------|-------|------------|
| `MAX_ATTEMPTS` | `5` | A record whose `PUBACK` has a failure reason code other than `0x99`. |
| `MAX_BAD_RECORD_ATTEMPTS` | `2` | A record whose `PUBACK` is `0x99` (`Payload format invalid`): the payload isn't valid JSON. |
| `MAX_CONNECTION_LOSS_ATTEMPTS` | `2` | A connection closed while this record was the only one in flight. Closes with several records in flight aren't charged to any of them. |
| `MAX_INFLIGHT` | `100` | Unacknowledged QoS 1 records per connection. The `CONNACK` Receive Maximum is 4,096. |

## Quick start

1. Create the table with the rescue column. Replace the placeholders in `scripts/create_table.sql`, then run both statements in a SQL warehouse or notebook as a principal with `CREATE TABLE` on the schema. Wait up to 5 minutes after the `ALTER TABLE` statement before you publish. A tag change can take up to 5 minutes to take effect.

2. Copy the config file and fill in your values:

   ```bash
   cp config.example.env config.env
   # edit config.env, then:
   source config.env
   ```

3. Install the dependencies and run the publisher:

   ```bash
   pip install -r requirements.txt
   cd src
   python dice_roller.py --rolls 20 --interval 0.5
   ```

   A successful run ends with a line like this:

   ```text
   connections=1 durable=20 failed=0 rejected=0 clean_shutdown=True
   ```

   Exit status is `0` when every record is durable and the shutdown was clean. Otherwise it is `1`. A run with `--send-invalid-record` exits `1`, because the non-JSON record isn't durable.

4. Query the table. Durable records can take a short time to appear. See [Durability and acknowledgments](https://docs.databricks.com/aws/en/ingestion/zerobus-mqtt#acknowledgments). Rows with a non-null `rescue` value came from records that didn't match the table:

   ```sql
   SELECT roll_id, sides, rescue FROM <catalog>.<schema>.dice_rolls WHERE rescue IS NOT NULL;
   ```

5. Run the offline tests. They don't need a broker or a network connection:

   ```bash
   pip install pytest
   PYTHONPATH=src python -m pytest tests
   ```

## Schema

The rescue column is required. This is the table the example expects. `scripts/create_table.sql` creates it:

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.dice_rolls
  (roll_id STRING, player STRING, sides INT, dice INT, total INT, rescue VARIANT);

ALTER TABLE <catalog>.<schema>.dice_rolls
  ALTER COLUMN rescue SET TAGS ('zerobus-rescue');
```

The example publishes one JSON object per PUBLISH packet. The object matches the first five columns:

| Field | Type | Description |
|-------|------|-------------|
| `roll_id` | STRING | UUID for the roll. Use it to find duplicates. |
| `player` | STRING | One of `ada`, `grace`, or `linus`. |
| `sides` | INT | Faces per die. |
| `dice` | INT | Number of dice rolled. |
| `total` | INT | Sum of the dice. |
| `rescue` | VARIANT | Required. Receives the fields that don't match a column: a field the table doesn't have, or a value of the wrong type in a nullable column. Zerobus Ingest writes it. Records must not set it. |

Why the rescue column is required: without it, a record with a field the table doesn't have, or a value of the wrong type, is rejected, and the server closes the MQTT connection instead of returning a `PUBACK`. With the rescue column, the record is stored and the connection stays open. See [Delivery and failure behavior](#delivery-and-failure-behavior).

Requirements for the rescue column, from the [rescue column documentation](https://docs.databricks.com/aws/en/ingestion/zerobus-rescue-column):

- It is nullable, uses the `VARIANT` type, and has the `zerobus-rescue` tag. Use at most one tagged column per table.
- Keep the other columns nullable as well. This example's rescue behavior was tested only with nullable columns.
- It applies to JSON payloads only. A payload that isn't valid JSON isn't stored in `rescue`.

The table name is your choice. Set it in `DATABRICKS_TABLE_NAME`.

## Delivery and failure behavior

- A record counts as durable only after its `PUBACK` has a successful reason code.
- Records in flight when a connection closes are retried after reconnecting. A retry can create a duplicate row, because the original record may already have been stored when its `PUBACK` was lost. Deduplicate by `roll_id` in the lakehouse if duplicates matter to you.
- The publisher applies `PUBACK`s and connection closes when your code calls `submit()`, `pump()`, `ensure_connected()`, or `flush()`. Paho's callbacks only queue events. Call `pump()` from your loop even when no record is waiting, so a close the server sent is noticed and its records are requeued.
- The loop in `src/dice_roller.py` reconnects through `ensure_connected()` when the connection is down.
- **A type mismatch or an extra field, with the rescue column**: the record gets a `PUBACK` with reason code `Success` (`0x00`). The field that doesn't match is stored in `rescue`, and the connection stays open. `--send-invalid-record` sends one record of each of these two kinds.
- **A type mismatch or an extra field, without the rescue column**: the server closes the connection with reason code `Implementation specific error` (`0x83`) right after the `PUBLISH`, and sends no `PUBACK`. The close is charged to this record only if it was the only record in flight. The example retries it on a new connection, and after `MAX_CONNECTION_LOSS_ATTEMPTS` (two) charged closes it gives up and reports the record as failed. If several records were in flight when the connection closed, none is charged. They are sent one at a time until the server answers each one, so a valid record isn't abandoned because of a bad record's closes. Don't publish to a table without the rescue column; it is required.
- **A payload that isn't valid JSON**: the server returns `PUBACK` with reason code `Payload format invalid` (`0x99`). The connection stays open, and nothing is stored. The example retries the record up to `MAX_BAD_RECORD_ATTEMPTS` (two attempts), then reports it as failed.
- Validate records in your application if you'd rather not store mismatched fields in `rescue` at all. See [How to extend](#how-to-extend).
- A payload larger than the packet size limit is rejected locally and isn't retried.

## How to extend

- **Your own events**: Replace `roll_event()` in `src/dice_roller.py` with your data, keeping one JSON object per record that matches your table.
- **Your own source**: Call `ZerobusMqttPublisher.submit(record)` from your own loop or callback. The publisher handles tokens and acknowledgments. Your loop also calls `pump()` and `ensure_connected()`, as described in [Delivery and failure behavior](#delivery-and-failure-behavior).
- **Proactive token refresh**: The publisher reconnects after the server closes a connection, when `ensure_connected()` runs (see [Delivery and failure behavior](#delivery-and-failure-behavior)). To rotate tokens on a timer, call `publisher.reconnect("scheduled")` from your loop before the token expires. A reconnect your code starts charges no record, and it can't overlap a recovery from a closed connection. It stops the old network loop first, then applies the acknowledgments that arrived on the old connection, and only then requeues the records still unacknowledged, so an acknowledged record isn't sent again. In testing, each token was valid for one hour (its `exp` claim was one hour after `iat`).
- **Local validation**: Check each record against your schema in `submit()` before queuing it, so bad records don't reach the endpoint.
- **Metrics**: Read `durable_count`, `failed_count`, `rejected_count`, and `connect_count` to watch delivery health. The counts are exact. The most recent 50 durable, failed, and rejected records are kept in `recent_durable`, `recent_failed`, and `recent_rejected`, so memory doesn't grow with the length of the run.
