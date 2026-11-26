# MQTT.js Zerobus example: dice-roll simulator

## Overview

This example publishes a stream of dice rolls from a Node.js application to a Delta table through the Zerobus Ingest MQTT endpoint. It uses [MQTT.js](https://github.com/mqttjs/MQTT.js) 5.x, an MQTT 5 client for Node.js. Each roll is one JSON object published at QoS 1, and the simulator waits for the PUBACK before it reports the roll.

Use this pattern when your Node.js application already publishes with MQTT.js and you want its messages in Unity Catalog Delta tables. You don't need to run a message bus between the application and the table.

The client gets a new OAuth token before every connection, including each reconnect. MQTT.js reads `client.options.properties` when it sends CONNECT, so the token is set there before the connection is made. See `src/zerobus-mqtt.js`.

## The rescue column is required

The target table must have a `rescue` column of type `VARIANT` with the `zerobus-rescue` tag applied. This is required, not optional. Without the rescue column, one record that doesn't match the table closes the MQTT connection before the record is acknowledged. A type mismatch, such as a string in an `INT` column, or a field the table doesn't have is enough to do it. With the rescue column, Zerobus Ingest stores the record, writes the value that didn't match into `rescue` as JSON, and keeps the connection open.

A payload that isn't JSON doesn't close the connection in either case. The server answers with PUBACK reason code `0x99` (Payload format invalid), with or without the rescue column, nothing is stored, and the connection stays open.

| Record | Without the rescue column | With the rescue column |
|--------|--------------------------|------------------------|
| Valid JSON that matches the table | Stored, PUBACK success (`0x00`) | Stored, PUBACK success (`0x00`) |
| Type mismatch, such as a string in an `INT` column | Connection closed with DISCONNECT `0x83` (131), no PUBACK | PUBACK success. The row is stored, the mismatched column is `NULL`, and the value is in `rescue` |
| Field the table doesn't have | Connection closed with DISCONNECT `0x83` (131), no PUBACK | PUBACK success. The row is stored, and the field is in `rescue` |
| Payload that isn't JSON | PUBACK `0x99`, nothing stored, connection stays open | PUBACK `0x99`, nothing stored, connection stays open |

## Prerequisites

- Node.js 20.6 or above, with npm. The `npm start` script reads `config.env` with `--env-file`.
- MQTT enabled for your workspace. The endpoint is in Beta, and a workspace admin turns it on from the **Previews** page. The Zerobus Ingest documentation, page "Use MQTT with Zerobus Ingest", lists the prerequisites.
- An existing Unity Catalog Delta table with the schema in [Schema](#schema), including the `rescue` column tagged `zerobus-rescue`. Zerobus Ingest doesn't create the table. Tag changes can take up to 5 minutes to take effect, so wait at least 5 minutes after you tag the column before you publish.
- A service principal with `USE CATALOG`, `USE SCHEMA`, `SELECT`, and `MODIFY` on the table, and its OAuth client ID and secret.
- Outbound TLS connectivity to your Zerobus Ingest hostname on port `8883`.

## Configuration reference

Copy `config.example.env` to `config.env` and fill in the values. Keep `config.env` out of version control, because it holds the client secret.

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABRICKS_WORKSPACE_URL` | Yes | Workspace URL, such as `https://<workspace-url>`. Used for the token request. |
| `DATABRICKS_WORKSPACE_ID` | Yes | Workspace ID. Used in the `zerobusDirectWriteApi` resource of the token request. |
| `DATABRICKS_CLIENT_ID` | Yes | Service principal client ID. |
| `DATABRICKS_CLIENT_SECRET` | Yes | Service principal OAuth secret. |
| `DATABRICKS_TABLE_NAME` | Yes | Three-level table name, such as `<catalog>.<schema>.dice_rolls`. Also the MQTT topic. |
| `ZEROBUS_MQTT_HOST` | Yes | Zerobus Ingest MQTT hostname, without `https://`. Port `8883`. |
| `ROLL_INTERVAL_MS` | No | Milliseconds between rolls. Default `1000`. |
| `ROLL_COUNT` | No | Number of rolls to send. `0` (the default) keeps going until you press Ctrl+C. |

## Quick start

1. Install the dependencies:

   ```bash
   npm ci
   ```

2. Create the table with its rescue column, tag the column, and grant the service principal access. Run `scripts/create_table.sql` in a SQL editor after you replace the table name and the service principal's application ID. Then wait at least 5 minutes before you start the simulator, so the tag takes effect.

3. Create `config.env` from `config.example.env` and fill in the values.

4. Start the simulator:

   ```bash
   npm start
   ```

   Each roll prints one line after its PUBACK. This output is illustrative: the names and rolls differ on each run.

   ```text
   CONNACK success
   PUBACK Ada rolled 5 on a d6
   PUBACK Grace rolled 3 on a d6
   ```

   Press Ctrl+C to stop.

5. Run the unit tests. They don't connect to the network:

   ```bash
   npm test
   ```

## Schema

The simulator sends this record shape. The table columns match the JSON keys:

| Column | Type | Value |
|--------|------|-------|
| `player_name` | `STRING` | One of `Ada`, `Grace`, or `Linus`, in turn. |
| `die_sides` | `INT` | Number of sides on the die. `6` in this example. |
| `roll` | `INT` | Result of the roll, from 1 to `die_sides`. |
| `rolled_at` | `TIMESTAMP` | The JSON number is epoch microseconds, so `Date.now() * 1000`. |
| `rescue` | `VARIANT` | Required, with the `zerobus-rescue` tag. Holds the fields that don't match the table. The simulator doesn't send it. Don't send a non-null `rescue` value in a record. The server rejects it. |

Keep every column nullable. The rescue behavior in the table above is documented for nullable columns, and this example's table is nullable.

An example record:

```json
{"player_name":"Ada","die_sides":6,"roll":5,"rolled_at":1763996400000000}
```

A string for a `TIMESTAMP` column, such as an ISO 8601 date, and a string for an `INT` column are type mismatches. With the rescue column, the value is stored in `rescue`. Without it, the connection closes with reason code `0x83` (131). Send each value with the JSON type that matches its column.

## How to extend

- **Other data:** replace `makeRoll` in `src/dice.js` with your own record builder. Keep the JSON keys equal to the column names of the table, and change `scripts/create_table.sql` to match. Check the `rescue` column after a change: a key that doesn't match a column lands there instead of in its own column, so query `WHERE rescue IS NOT NULL` to find those records.
- **Retries:** `publish()` in `src/zerobus-mqtt.js` rejects when the PUBACK has an error reason code, when the connection closes before the PUBACK, or when a record doesn't complete within 30 seconds (`PUBLISH_ACK_TIMEOUT_MS`). The 30 seconds covers the wait for a connection and the wait for the PUBACK together. If the connection is down, `publish()` waits for the next connection and then sends the record one time. It doesn't hand the record to MQTT.js while the connection is down, because MQTT.js would queue it and send it after the reconnect. A record whose wait runs out before the connection comes back is rejected and is not sent. If a record was sent and no PUBACK arrives within the timeout, the example removes the record from MQTT.js so it isn't resent, and the connection stays open. A PUBACK that arrives after the timeout is ignored. A PUBACK with `0x99` means the record was not stored, so fix the payload rather than retrying the same bytes. A record that was sent and then times out or closes the connection before its PUBACK can already be durable, so a retry can create a duplicate. Deduplicate in the lakehouse, or accept duplicates.
- **More tables:** each MQTT connection targets one table. Create one connection per table.
- **Token lifetime:** the token is requested before every connection, so a reconnect doesn't reuse an old token. The connection has a bounded server-side lifetime, so long-running clients reconnect on a schedule the server sets.

## Files

```
example_clients/mqtt_mqttjs_zerobus/
├── README.md
├── config.example.env          # Placeholders only. Copy to config.env.
├── package.json
├── package-lock.json
├── scripts/
│   └── create_table.sql        # Table with the rescue column, its tag, and grants
├── src/
│   ├── main.js                 # Simulator loop
│   ├── dice.js                 # Roll records
│   ├── token.js                # OAuth token request for the table
│   └── zerobus-mqtt.js         # MQTT connection with a fresh token per connect
└── tests/                      # Unit tests (node --test), no network
```
