# Eclipse Paho Java + Zerobus Ingest over MQTT v5

This example shows a Java application that publishes JSON sensor readings to a Unity Catalog Delta table through the Zerobus Ingest MQTT v5 endpoint, using the [Eclipse Paho Java MQTT v5 client](https://github.com/eclipse-paho/paho.mqtt.java). The app is a small sensor simulator. It produces one reading every second, sends each one at QoS 1, and refreshes the OAuth token every time it reconnects.

```
[ Sensor simulator (Paho Java, MQTT v5) ]
     |
     | MQTT v5 over TLS, port 8883
     | CONNECT: Authorization (Bearer token), x-databricks-zerobus-table-name
     v
[ Zerobus Ingest ] --> [ Delta table in Unity Catalog ]
```

> The rescue column is required. The target table must have a nullable `rescue VARIANT` column with the `zerobus-rescue` tag. Without it, one record whose fields don't match the table (for example, a string in an `INT` column, or a field the table doesn't have) closes the MQTT connection with DISCONNECT reason code `0x83` (Implementation specific error), and that record isn't stored. With the rescue column, the same record gets a successful `PUBACK`, the mismatched fields are stored in `rescue`, and the connection stays up. Create the table with [`scripts/create_table.sql`](scripts/create_table.sql), which adds the column and the tag. See [Schema](#schema).

## Overview

Use this client when your application is written in Java and already uses MQTT v5, or when you want a Java producer for IoT, edge, or telemetry data. Zerobus Ingest writes each message's JSON object directly to one existing table. You don't need a message broker between the application and the table.

The Zerobus Ingest MQTT behavior is described in [Use MQTT with Zerobus Ingest](https://docs.databricks.com/aws/en/ingestion/zerobus-mqtt). This example implements that page's rules in Java:

- The OAuth token and the table name are sent as User Properties on `CONNECT`.
- Each connection targets one table. Open another connection for another table.
- Each record is a single UTF-8 JSON object that matches the table schema.
- A record counts as stored only after its `PUBACK` with a successful reason code.
- A new token is fetched for every connection, including reconnects.

## Prerequisites

- Java Development Kit 17 and above, and Maven.
- MQTT enabled for your workspace. MQTT is in Beta, and a workspace admin turns it on from the [Previews](https://docs.databricks.com/aws/en/admin/workspace-settings/manage-previews) page. See the prerequisites in [Use MQTT with Zerobus Ingest](https://docs.databricks.com/aws/en/ingestion/zerobus-mqtt#prerequisites).
- An existing Unity Catalog table with the schema in [Schema](#schema), including the required `rescue` column. Create it with [`scripts/create_table.sql`](scripts/create_table.sql). Without the rescue column, a record that doesn't match the table closes the connection.
- A service principal with `USE CATALOG`, `USE SCHEMA`, `SELECT`, and `MODIFY` on the table. See [Create a service principal and grant permissions](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest#service-principal).
- Your workspace URL, workspace ID, and Zerobus Ingest hostname. See [Get your workspace URL and Zerobus Ingest endpoint](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest#get-your-workspace-url-and-zerobus-ingest-endpoint).
- Outbound TLS access to the Zerobus Ingest hostname on port 8883.

## Configuration reference

Copy `config.example.properties` to `config.properties` and replace the placeholders. The file is git-ignored.

| Setting | Environment variable | Description |
|---|---|---|
| `workspace.url` | `DATABRICKS_WORKSPACE_URL` | Your workspace URL, including `https://`. Used for the OAuth token request. |
| `workspace.id` | `DATABRICKS_WORKSPACE_ID` | Your workspace ID. Used in the token's resource name. |
| `table.name` | `DATABRICKS_TABLE_NAME` | The target table, as `catalog.schema.table`. |
| `mqtt.host` | `ZEROBUS_MQTT_HOST` | The Zerobus Ingest hostname without `https://` or a port. The client connects to port 8883. |
| (none) | `DATABRICKS_CLIENT_ID` | The service principal's application ID. Environment variable only. |
| (none) | `DATABRICKS_CLIENT_SECRET` | The service principal's OAuth secret. Environment variable only. |
| `sensor.device` | none | The `device_name` value for each reading. Default `sensor-1`. |
| `sensor.records` | none | How many readings to send. Default `20`. |
| `sensor.interval.ms` | none | Pause between readings, in milliseconds. Default `1000`. |
| `sensor.reconnect.every` | none | Reconnect with a new token every N readings. Default `0`, which turns this off. |

An environment variable with the same name as a file setting overrides the file. Keep the client secret in the environment, not in the file.

## Quick start

1. Create the table. Replace `<catalog>.<schema>` in [`scripts/create_table.sql`](scripts/create_table.sql) and run it in Databricks SQL. The script creates the `rescue` column and applies the `zerobus-rescue` tag. Wait up to 5 minutes after the tag is applied before you run the simulator, because tag changes can take up to 5 minutes to take effect.

2. Set the service principal credentials and the endpoint values:

   ```bash
   export DATABRICKS_CLIENT_ID="<service-principal-application-id>"
   export DATABRICKS_CLIENT_SECRET="<service-principal-client-secret>"
   export DATABRICKS_WORKSPACE_URL="https://<workspace-url>"
   export DATABRICKS_WORKSPACE_ID="<workspace-id>"
   export DATABRICKS_TABLE_NAME="<catalog>.<schema>.mqtt_paho_java"
   export ZEROBUS_MQTT_HOST="<workspace-id>.zerobus.<region>.<cloud-domain>"
   ```

3. Build the simulator. The Maven build writes `target/sensor-simulator.jar`:

   ```bash
   mvn -B package
   ```

4. Run it:

   ```bash
   java -jar target/sensor-simulator.jar config.properties
   ```

   The output looks like this. Token fingerprints are the first bytes of a SHA-256 digest, so they identify a token without revealing it:

   ```text
   Connected to <catalog>.<schema>.mqtt_paho_java (token sha256:<fingerprint>)
   Record 1 acknowledged (PUBACK)
   ...
   ```

5. Check the table:

   ```sql
   SELECT count(*) FROM <catalog>.<schema>.mqtt_paho_java;
   ```

   With no retries and no `--bad-records`, the count equals `sensor.records`, 20 by default.

To refresh the token during a long run, set `sensor.reconnect.every` to a number such as `100`. The simulator then reconnects with a new token after every `sensor.reconnect.every` readings.

To see how the endpoint handles records that don't match the schema, add `--bad-records`:

```bash
java -jar target/sensor-simulator.jar config.properties --bad-records
```

After the normal records, the simulator sends these on the same connection, then one valid record:

| Payload | With the rescue column |
|---|---|
| `temp` is a string (`"hot"`) | `PUBACK` reason code `0`. The row is stored, `temp` is `NULL`, and `{"temp":"hot"}` is in `rescue`. |
| An extra field (`extra_field`) | `PUBACK` reason code `0`. The field is stored in `rescue`. |
| Not JSON | `PUBACK` reason code `0x99` (Payload format invalid). Nothing is stored. |
| One valid record after the three above | `PUBACK` reason code `0`. The connection is still open. |

Without the rescue column, the type mismatch or the extra field closes the connection with DISCONNECT `0x83` and no `PUBACK`, so the payloads after it in the same run can't be sent on that connection. A non-JSON payload gets `0x99` with or without the rescue column, as long as the connection is open. Run `--bad-records` against a table without the rescue column to see the `0x83` behavior.

Run the tests with `mvn -B test`. The tests don't contact the workspace.

## Schema

Each reading is one JSON object with three fields that match the table's data columns. The table also needs the `rescue` column:

```sql
CREATE TABLE <catalog>.<schema>.mqtt_paho_java (
  device_name STRING,
  temp INT,
  humidity INT,
  rescue VARIANT
);

ALTER TABLE <catalog>.<schema>.mqtt_paho_java
  ALTER COLUMN rescue SET TAGS ('zerobus-rescue');
```

| Column | Type | Example | Notes |
|---|---|---|---|
| `device_name` | `STRING` | `"sensor-1"` | Matches the JSON field. |
| `temp` | `INT` | `22` | Matches the JSON field. |
| `humidity` | `INT` | `55` | Matches the JSON field. |
| `rescue` | `VARIANT` | (not sent) | Required. Must be nullable and tagged `zerobus-rescue`. Receives any field that doesn't match a column. A record can't set it to a non-null value. |

Keep every column nullable, as the script does. This example doesn't test `NOT NULL` columns with the rescue column.

For example, `{"device_name":"sensor-1","temp":22,"humidity":55}` is one record. The simulator generates temperatures from 18 to 26 and humidity from 40 to 70.

If a record has a field the table doesn't define, or a value of the wrong type, Zerobus Ingest stores the non-matching fields in `rescue` as a JSON object. The record's other fields are stored in their columns. A payload that isn't valid JSON is not stored.

## How it works

- `TokenClient` requests a table-scoped OAuth token with the service principal client credentials flow. The request uses the `zerobusDirectWriteApi` resource and `authorization_details` for the catalog, schema, and table.
- `ZerobusMqttPublisher` connects to `ssl://<mqtt.host>:8883`. Each `connect()` fetches a fresh token and sends it, with the table name, as User Properties on `CONNECT`. Automatic reconnect is off, so the caller decides when to reconnect, and each reconnect sends a new token.
- `publish()` sends one record at QoS 1 and waits for the `PUBACK`. A `PUBACK` with a reason code of `0x80` or higher, or no `PUBACK` before the timeout, throws an exception.
- `SensorSimulator` makes up to three attempts at an unacknowledged record in total: the first send, then up to two retries. Each retry opens a new connection with a new token. A retry can store a duplicate if an earlier attempt was stored before the connection closed. Use the table's data to deduplicate if that matters for your use case. With the rescue column, a record with a type mismatch or an extra field is acknowledged, so it isn't retried.
- `BadRecords` holds the payloads for `--bad-records`.

## How to extend

- **Other sensors or fields:** Add the field to `SensorReading` and to the table. Give the JSON field the same name as its column.
- **Other tables:** Each connection targets one table. For a second table, create another `ZerobusMqttPublisher` with its own `Settings`.
- **Your own data source:** Replace the loop in `SensorSimulator` with your source of readings. Keep `publish()` for each record, and keep the reconnect step.
- **Stricter retry policy:** Change `MAX_ATTEMPTS` in `SensorSimulator` and add a delay before each reconnect in `publishWithRetry`. Track unacknowledged records in your own store if you need to replay them after a restart.
- **Other MQTT v5 clients:** The CONNECT User Properties and the QoS 1 rules in [Use MQTT with Zerobus Ingest](https://docs.databricks.com/aws/en/ingestion/zerobus-mqtt) apply to any MQTT v5 client.

## Project layout

```
pom.xml                              Maven build (Paho MQTT v5 client 1.2.5, Jackson, JUnit)
config.example.properties            Non-secret settings with placeholders
scripts/create_table.sql             Target table, with the rescue column and its tag
src/main/java/.../Settings.java      Settings from the file and the environment
src/main/java/.../TokenClient.java   OAuth token request
src/main/java/.../ZerobusMqttPublisher.java  MQTT connection, reconnect, publish
src/main/java/.../SensorReading.java JSON record
src/main/java/.../SensorSimulator.java  Simulator loop, retry, and --bad-records
src/main/java/.../BadRecords.java    Payloads that don't match the schema
src/test/java/...                    Unit tests (no network)
```
