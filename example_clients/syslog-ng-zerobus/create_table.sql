-- Create the Delta table that Zerobus Ingest will write OTel log records into.
--
-- Before running:
--   1. Replace <catalog>, <schema>, and <table> with your values.
--   2. Replace <service-principal-uuid> with your service principal's Application ID
--      (found under Settings > Identity and Access > Service Principals > your SP > Configurations tab).
--   3. Run this script in Databricks SQL.
--
-- Prerequisites:
--   - DBR 15.3 or higher (required for VARIANT columns)
--   - DBR 17.2 or higher (optional — enables variant shredding for faster queries)

CREATE TABLE <catalog>.<schema>.<table> (
  record_id STRING,
  time TIMESTAMP,
  date DATE,
  service_name STRING,
  event_name STRING,
  trace_id STRING,
  span_id STRING,
  time_unix_nano LONG,
  observed_time_unix_nano LONG,
  severity_number STRING,
  severity_text STRING,
  body VARIANT,
  attributes VARIANT,
  dropped_attributes_count INT,
  flags INT,
  resource STRUCT<
    attributes: VARIANT,
    dropped_attributes_count: INT
  >,
  resource_schema_url STRING,
  instrumentation_scope STRUCT<
    name: STRING,
    version: STRING,
    attributes: VARIANT,
    dropped_attributes_count: INT
  >,
  log_schema_url STRING
) USING DELTA
CLUSTER BY (time, service_name)
TBLPROPERTIES (
  'otel.schemaVersion'                     = 'v2',
  'delta.checkpointPolicy'                 = 'classic',
  'delta.enableVariantShredding'           = 'true',        -- optional, DBR 17.2+
  'delta.feature.variantShredding-preview' = 'supported',   -- optional, DBR 17.2+
  'delta.feature.variantType-preview'      = 'supported'
);

-- Grant the service principal access.
-- Note: Granting ALL PRIVILEGES is not sufficient — explicit grants are required.
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-uuid>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-uuid>`;
GRANT MODIFY, SELECT ON TABLE <catalog>.<schema>.<table> TO `<service-principal-uuid>`;
