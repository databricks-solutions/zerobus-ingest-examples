-- Create the example table. Run once in a SQL editor or with the Databricks CLI, before you start the flow.
-- Replace <catalog>.<schema>.mqtt_node_red with your own three-level table name (keep it the same as DATABRICKS_TABLE_NAME).
--
-- The rescue column is required. A record that doesn't match the table's columns is stored in rescue
-- instead of closing the MQTT connection. Without the rescue column, one mismatched record closes the connection.
-- The tag tells Zerobus Ingest to use this column. Wait at least 5 minutes after the ALTER before you publish.

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
