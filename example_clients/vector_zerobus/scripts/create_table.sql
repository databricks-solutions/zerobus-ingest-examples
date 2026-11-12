-- Target tables for vector/vector.yaml, one per signal. Column names match the fields
-- set in the shape_logs, shape_metrics and shape_traces transforms. Keep columns
-- nullable: if a column is NOT NULL and one event in a batch is missing it, the sink
-- drops the whole batch.
-- Replace <catalog>.<schema> and <service-principal-uuid>, then run in Databricks SQL.

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.dice_logs (
  event_id  STRING,
  timestamp TIMESTAMP,
  service   STRING,
  severity  STRING,
  message   STRING,
  player    STRING,
  result    INT,
  trace_id  STRING,
  span_id   STRING
);

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.dice_metrics (
  event_id  STRING,
  timestamp TIMESTAMP,
  service   STRING,
  metric    STRING,
  kind      STRING,
  value     DOUBLE,
  result    STRING
);

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.dice_traces (
  trace_id       STRING,
  span_id        STRING,
  parent_span_id STRING,
  name           STRING,
  start_time     TIMESTAMP,
  end_time       TIMESTAMP,
  duration_ms    DOUBLE,
  service        STRING,
  player         STRING,
  result         INT,
  status_code    INT
);

GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-uuid>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-uuid>`;
GRANT MODIFY, SELECT ON TABLE <catalog>.<schema>.dice_logs TO `<service-principal-uuid>`;
GRANT MODIFY, SELECT ON TABLE <catalog>.<schema>.dice_metrics TO `<service-principal-uuid>`;
GRANT MODIFY, SELECT ON TABLE <catalog>.<schema>.dice_traces TO `<service-principal-uuid>`;
