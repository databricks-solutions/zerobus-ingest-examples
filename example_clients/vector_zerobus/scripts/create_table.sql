-- Target table for vector/vector.yaml. Column names match the fields set in the
-- shape_for_table transform. Keep columns nullable: if a column is NOT NULL and one
-- event in a batch is missing it, the sink drops the whole batch.
-- Replace <catalog>.<schema> and <service-principal-uuid>, then run in Databricks SQL.

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.vector_logs (
  event_id STRING,
  timestamp TIMESTAMP,
  host STRING,
  service STRING,
  severity STRING,
  message STRING
);

GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-uuid>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-uuid>`;
GRANT MODIFY, SELECT ON TABLE <catalog>.<schema>.vector_logs TO `<service-principal-uuid>`;
