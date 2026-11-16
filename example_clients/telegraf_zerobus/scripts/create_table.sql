CREATE TABLE <catalog>.<schema>.<table> (
  record_id STRING,
  time TIMESTAMP,
  date DATE,
  service_name STRING,
  start_time_unix_nano LONG,
  time_unix_nano LONG,
  name STRING,
  description STRING,
  unit STRING,
  metric_type STRING,
  gauge STRUCT<
    value: DOUBLE,
    exemplars: ARRAY<STRUCT<
      time_unix_nano: LONG,
      value: DOUBLE,
      span_id: STRING,
      trace_id: STRING,
      filtered_attributes: VARIANT
    >>,
    attributes: VARIANT,
    flags: INT
  >,
  sum STRUCT<
    value: DOUBLE,
    exemplars: ARRAY<STRUCT<
      time_unix_nano: LONG,
      value: DOUBLE,
      span_id: STRING,
      trace_id: STRING,
      filtered_attributes: VARIANT
    >>,
    attributes: VARIANT,
    flags: INT,
    aggregation_temporality: STRING,
    is_monotonic: BOOLEAN
  >,
  histogram STRUCT<
    count: LONG,
    sum: DOUBLE,
    bucket_counts: ARRAY<LONG>,
    explicit_bounds: ARRAY<DOUBLE>,
    exemplars: ARRAY<STRUCT<
      time_unix_nano: LONG,
      value: DOUBLE,
      span_id: STRING,
      trace_id: STRING,
      filtered_attributes: VARIANT
    >>,
    attributes: VARIANT,
    flags: INT,
    min: DOUBLE,
    max: DOUBLE,
    aggregation_temporality: STRING
  >,
  exponential_histogram STRUCT<
    attributes: VARIANT,
    count: LONG,
    sum: DOUBLE,
    scale: INT,
    zero_count: LONG,
    positive_bucket: STRUCT<
      offset: INT,
      bucket_counts: ARRAY<LONG>
    >,
    negative_bucket: STRUCT<
      offset: INT,
      bucket_counts: ARRAY<LONG>
    >,
    flags: INT,
    exemplars: ARRAY<STRUCT<
      time_unix_nano: LONG,
      value: DOUBLE,
      span_id: STRING,
      trace_id: STRING,
      filtered_attributes: VARIANT
    >>,
    min: DOUBLE,
    max: DOUBLE,
    zero_threshold: DOUBLE,
    aggregation_temporality: STRING
  >,
  summary STRUCT<
    count: LONG,
    sum: DOUBLE,
    quantile_values: ARRAY<STRUCT<
      quantile: DOUBLE,
      value: DOUBLE
    >>,
    attributes: VARIANT,
    flags: INT
  >,
  metadata VARIANT,
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
  metric_schema_url STRING
) USING DELTA
CLUSTER BY (time, service_name)
TBLPROPERTIES (
  'otel.schemaVersion' = 'v2',
  'delta.checkpointPolicy' = 'classic',
  'delta.enableVariantShredding' = 'true', -- optional
  'delta.feature.variantShredding' = 'supported', -- optional
  'delta.feature.variantType' = 'supported' -- optional
);
