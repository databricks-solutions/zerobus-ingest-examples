-- Run once, before the first publish. Zerobus Ingest doesn't create tables.
-- Replace <catalog>.<schema>.dice_rolls with your table name, then run both statements as a
-- principal with CREATE TABLE on the schema. The service principal needs USE CATALOG, USE SCHEMA,
-- SELECT and MODIFY on the table.
--
-- The rescue column is required. Without it, one record that doesn't match the table closes the
-- MQTT connection. With it, Zerobus Ingest writes the non-matching fields to `rescue` and the
-- connection stays open. Keep the other columns nullable: the rescue behavior was tested only
-- with nullable columns.
--
-- Changes to the tag can take up to 5 minutes to take effect. Wait before you publish.

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.dice_rolls
  (roll_id STRING, player STRING, sides INT, dice INT, total INT, rescue VARIANT);

ALTER TABLE <catalog>.<schema>.dice_rolls
  ALTER COLUMN rescue SET TAGS ('zerobus-rescue');
