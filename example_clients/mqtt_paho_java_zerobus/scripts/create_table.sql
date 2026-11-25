-- Target table for the simulator. Zerobus Ingest doesn't create tables, so create it first.
-- Replace <catalog>.<schema> with your own. The table name must match table.name in config.properties.
-- The rescue column is required. Without it, one record whose fields don't match the table closes the
-- MQTT connection. Keep every column nullable, and tag the rescue column so Zerobus Ingest uses it.
-- Tag changes can take up to 5 minutes to take effect. Wait before you run the simulator.
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.mqtt_paho_java (
  device_name STRING,
  temp INT,
  humidity INT,
  rescue VARIANT
);

ALTER TABLE <catalog>.<schema>.mqtt_paho_java
  ALTER COLUMN rescue SET TAGS ('zerobus-rescue');
