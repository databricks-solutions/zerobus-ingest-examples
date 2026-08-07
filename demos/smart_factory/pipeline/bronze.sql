-- Bronze: Validated raw sensor events from ZeroBus landing zone
-- Source table is fully qualified; output uses pipeline's target schema

CREATE OR REFRESH STREAMING TABLE raw_sensor_events (
  CONSTRAINT valid_value EXPECT (value IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_machine EXPECT (machine_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_sensor EXPECT (sensor_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp EXPECT (timestamp IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Validated IoT sensor events from ZeroBus landing zone'
AS SELECT
  machine_id,
  machine_type,
  sensor_name,
  CAST(value AS DOUBLE) AS value,
  unit,
  CAST(timestamp AS TIMESTAMP) AS timestamp,
  COALESCE(is_fault, false) AS is_fault,
  current_timestamp() AS ingested_at
FROM STREAM(main.smartfactory.raw_sensor_events);
