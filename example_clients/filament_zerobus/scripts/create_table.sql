-- Delta tables for the demo, created by you (Filament doesn't need to create them).
-- Replace <catalog>, <schema>, <cdc-schema> and <service-principal-uuid> (TARGET_CATALOG, TARGET_SCHEMA,
-- TARGET_CDC_SCHEMA and DATABRICKS_CLIENT_ID), then run it in Databricks SQL.
-- The columns follow the PostgreSQL tables in seed_source.sql. Keep each NOT NULL exactly where the source
-- column is NOT NULL: a mismatch fails the stream with Error Code 8001. Filament's lineage columns come last.

-- Full append: matches the orders table in the source.
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.orders (
  id         BIGINT NOT NULL,
  customer   STRING NOT NULL,
  amount     DECIMAL(10,2),
  status     STRING,
  created_at TIMESTAMP NOT NULL,
  _filament_run_id         STRING,
  _filament_run_started_at TIMESTAMP
) USING DELTA;

-- cdc_append: a change history, so every column is nullable. Lineage columns come after the source columns,
-- then the three CDC columns.
CREATE TABLE IF NOT EXISTS <catalog>.<cdc-schema>.payments (
  id         BIGINT,
  order_id   BIGINT,
  amount     DECIMAL(10,2),
  state      STRING,
  updated_at TIMESTAMP,
  _filament_run_id          STRING,
  _filament_run_started_at  TIMESTAMP,
  _filament_operation       STRING,
  _filament_source_position STRING,
  _filament_sequence        BIGINT
) USING DELTA;

-- Grants for the service principal, before the first run (Zerobus Ingest binds the table when a stream opens).
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-uuid>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-uuid>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<cdc-schema> TO `<service-principal-uuid>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.orders TO `<service-principal-uuid>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<cdc-schema>.payments TO `<service-principal-uuid>`;
