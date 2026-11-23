-- Run once in your SQL warehouse (or with: databricks api post /api/2.0/sql/statements ...).
-- Replace the placeholders. The connector does not create these tables in this setup (auto-create is off).

-- Target table: typed columns for the demo record, plus the _kafka metadata struct (zerobus.metadata.enabled=true).
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.demo_events (
  id BIGINT,
  name STRING,
  amount INT,
  _kafka STRUCT<cluster: STRING, topic: STRING, partition: INT, offset: BIGINT, key: STRING, timestamp: TIMESTAMP>
);

-- Dead-letter table: fixed schema for the default dead-letter options
-- (zerobus.dlq.include.metadata=true, zerobus.dlq.include.key=false, zerobus.dlq.oversize.metadata.only=true).
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.demo_events_dlq (
  cluster STRING,
  topic STRING,
  `partition` INT,
  `offset` BIGINT,
  timestamp TIMESTAMP,
  value BINARY,
  value_original_bytes BIGINT,
  value_truncated BOOLEAN,
  headers MAP<STRING, STRING>,
  error_message STRING,
  failed_at TIMESTAMP,
  connector STRING
);

-- Grants for the service principal in DATABRICKS_CLIENT_ID (replace the placeholder with its application ID).
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-client-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-client-id>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.demo_events TO `<service-principal-client-id>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.demo_events_dlq TO `<service-principal-client-id>`;
