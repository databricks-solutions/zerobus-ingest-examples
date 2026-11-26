# Example configurations

Each file in this folder is one connector configuration for one common way to use the sink. Start with the pattern that matches your data, copy its file, and replace the placeholders. Every example uses the same connection settings, so only the routing, mode, and table settings change between them.

| File | Pattern | Routing | Mode | Tables |
|---|---|---|---|---|
| `01-one-to-one-auto-create.json` | One topic to one table, tables created for you | `zerobus.topic2table.map` with a regex group | `typed` | Created from the record schema |
| `02-fan-in-envelope.json` | Many topics into one table | `topics.regex` and `zerobus.table` | `envelope` | One fixed-shape table, created for you |
| `03-fan-out-static.json` | One topic into several tables, chosen by a field | `zerobus.tables` with a route regex per table | `typed` | Created from the record schema |
| `04-fan-out-dynamic.json` | One topic into tables named by a field | `zerobus.tables.dynamic.enabled` and a route field | `typed` | Created from the record schema, inside one namespace |
| `05-dead-letter.json` | One topic to one table, with rejected rows kept | `zerobus.table` | `typed` | Created by you, with a dead-letter table |

## Before you register any example

1. Set the connection values. The worker resolves these from its environment, so no secret is in a file: `ZEROBUS_SERVER_ENDPOINT`, `DATABRICKS_WORKSPACE_URL`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, and `SQL_WAREHOUSE_ID`. The package quick start puts them in `.env`.
2. Replace the placeholders. `<catalog>.<schema>` is your catalog and schema. The examples use `kafka:9092` as the Kafka bootstrap servers; replace it with yours.
3. Check the warehouse. Auto-create runs `CREATE TABLE IF NOT EXISTS` through the SQL warehouse set in `zerobus.table.service.warehouse.id`. The warehouse also runs the dead-letter table check when that table is set.
4. Create the control topic. Examples 1 to 4 set `zerobus.table.auto.create` to `true`. With auto-create on, the connector needs its control topic (`zerobus.control.topic`) to exist before it starts, and the topic needs at least as many partitions as tasks. Create it first, for example with `kafka-topics.sh --create --partitions 3`.
5. Use schemas. Typed mode maps fields to columns, and auto-create takes its column types from the record schema. Send records with a schema (for example, JSON with `schemas.enable=true`). Envelope mode does not need field types, because its columns are fixed.

To register an example, copy it, replace the placeholders, and register it:

```bash
cp examples/01-one-to-one-auto-create.json config.json
# edit config.json: replace <catalog> and <schema>
scripts/register.sh config.json
curl -s http://localhost:8083/connectors/retail-typed/status
```

`register.sh` refuses a file that still has `<placeholders>`.

## 1. One topic to one table (typed, auto-create)

Use it when each source topic has its own table, and you want the connector to create the tables from the first record. This is the usual starting point.

Key settings:

- `topics`: the topics to read, comma-separated.
- `zerobus.topic2table.map`: `topic-regex=>catalog.schema.table_template`. The first full match wins, and `$1` is the first regex group. In the example, `retail\\.(.*)=><catalog>.<schema>.retail_$1` sends `retail.orders` to `retail_orders` and `retail.payments` to `retail_payments`. In JSON, a backslash is written `\\`.
- `zerobus.table.auto.create`: `true`. Missing tables are created from the record schema.
- `zerobus.mode`: `typed`.

A topic that matches no rule goes to `zerobus.table`, if you set it. Without a default, a topic that matches no rule is an error.

## 2. Fan-in: many topics into one table (envelope)

Use it when many topics carry events of the same kind, and you want one table for all of them. Each row says which topic and partition it came from. The table shape stays the same, so new topics need no schema work.

Key settings:

- `topics.regex`: a Java regular expression. Every topic whose full name matches is read, including topics created later. Use `topics` or `topics.regex`, not both. The example reads every `telemetry.*` topic.
- `zerobus.table`: the one target table.
- `zerobus.mode`: `envelope`. The columns are `record_key` (STRING), `record_value` (VARIANT, the record value as JSON text), `record_headers` (MAP, when `zerobus.envelope.headers.enabled` is `true`), and `record_timestamp` (TIMESTAMP). The `_kafka` struct carries the topic, partition, offset, and key.
- `zerobus.table.auto.create`: `true`.

Use typed mode instead when the fields need their own typed columns for queries. Envelope mode stores the whole value in `record_value` as a VARIANT, so you read the fields out of it at query time.

## 3. Fan-out, static: one topic into several tables

Use it when one stream feeds several tables, each a subset of the records. A record goes to every table whose regex matches its route field, so one record can land in two tables.

Key settings:

- `zerobus.tables`: the list of target tables.
- `zerobus.tables.route.field`: the record field whose value is matched.
- `zerobus.tables.<catalog>.<schema>.<table>.route.regex`: one regex per table. The value must match the whole pattern. In the example, a `click` record goes to `clickstream_clicks` and to `clickstream_all`, and a `purchase` record goes to `clickstream_purchases` and to `clickstream_all`.

Every table in `zerobus.tables` needs a route regex when a route field is set. A record that no regex matches goes to the dead-letter table, or fails the task when no dead-letter table is set.

Topic-level regex groups (the `$1` form in pattern 1) choose one table per topic. Fan-out chooses several tables per record, so it uses the route field instead.

## 4. Fan-out, dynamic: one topic into tables named by a field

Use it when the set of tables is not known in advance, for example one table per tenant or per device group. The record's route field holds the table name, and every table is created inside one namespace.

Key settings:

- `zerobus.tables.dynamic.enabled`: `true`.
- `zerobus.tables.dynamic.namespace`: the `catalog.schema` that dynamic routing may write to. It is the only namespace the connector accepts.
- `zerobus.tables.route.field`: the field that holds the table name, for example `target_table` with the value `tenant_acme`. The value must be a plain identifier (letters, digits, and underscores).
- `zerobus.table.auto.create`: `true`, so each new name gets a table.

A record whose route field is missing or not a valid identifier goes to the dead-letter table, or fails the task when no dead-letter table is set. Because any value in the field names a table, check upstream that only expected values reach the connector.

## 5. Dead-letter table

Use it when you want rejected records kept for review and replay, instead of stopping the task. A record can be rejected for several reasons: its routing fails, its fields do not fit the table (for example, a field that the table does not have), or the server refuses it.

Key settings:

- `zerobus.dlq.table`: the dead-letter table. Empty (the default) turns the dead-letter path off.
- `zerobus.dlq.include.metadata`: `true` (the default). Rows carry `cluster`, `topic`, `partition`, `offset`, `timestamp`, and `headers`.
- `zerobus.dlq.include.key`: `true`. Rows also carry `key` (BINARY). It is off by default to keep rows small.
- `zerobus.dlq.oversize.metadata.only`: `true` (the default). A record too large for Zerobus is kept as its metadata only, with `value` null and `value_truncated` true. With `false`, such a record is logged and skipped.

The dead-letter table must have every column the options need. The connector checks this at start and names any missing column, but it does not change the table. Create it with the columns for the options you set. For this example (metadata and key on):

```sql
CREATE TABLE <catalog>.<schema>.ledger_dlq (
  cluster STRING,
  topic STRING,
  `partition` INT,
  `offset` BIGINT,
  timestamp TIMESTAMP,
  key BINARY,
  value BINARY,
  value_original_bytes BIGINT,
  value_truncated BOOLEAN,
  headers MAP<STRING, STRING>,
  error_message STRING,
  failed_at TIMESTAMP,
  connector STRING
);
```

Each dead-letter row also carries the error text (`error_message`), the time it failed (`failed_at`), and the connector name (`connector`). Rows count toward the offset commit: a record is committed after it is acknowledged or written to the dead-letter table.

Example 5 creates its target table by hand as well, because auto-create is off. With auto-create off, the target table must exist before records arrive.

## Limits to check before you use a pattern

- A record can be delivered more than one time. After a restart or rebalance, records after the last committed offset are sent again. Deduplicate on `_kafka.partition` and `_kafka.offset` if you need each record counted one time.
- Auto-create and schema evolution need the control topic and the warehouse. Without them, the connector refuses to start.
- The Kafka JSON converter turns text in an integer field into `0`. Use a strict converter when a bad value must be rejected rather than stored as `0`.
