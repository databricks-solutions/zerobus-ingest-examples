-- Silver: Enriched events with anomaly scoring
-- Uses a threshold LIVE TABLE joined against streaming events for anomaly detection

-- Sensor thresholds reference table (the "ML model" — simple, explainable, reliable)
CREATE OR REFRESH LIVE TABLE sensor_thresholds
COMMENT 'Anomaly detection thresholds per machine and sensor'
AS SELECT * FROM VALUES
  -- CNC Mill
  ('CNC_Mill_01', 'temperature_c',    55.0,  75.0, '°C'),
  ('CNC_Mill_01', 'vibration_mm_s',    4.0,   6.5, 'mm/s'),
  ('CNC_Mill_01', 'spindle_rpm',    4200.0, 4800.0, 'RPM'),
  -- Hydraulic Press (some sensors fault LOW, so we use value < threshold)
  ('Hydraulic_Press_01', 'pressure_bar',    210.0, 270.0, 'bar'),
  ('Hydraulic_Press_01', 'temperature_c',    70.0,  90.0, '°C'),
  ('Hydraulic_Press_01', 'cycle_count',       6.0,   3.0, 'cycles/min'),
  -- Conveyor Belt
  ('Conveyor_Belt_01', 'speed_m_min',       9.0,   6.0, 'm/min'),
  ('Conveyor_Belt_01', 'load_weight_kg',  350.0, 430.0, 'kg'),
  ('Conveyor_Belt_01', 'motor_current_a',  12.0,  17.0, 'A')
AS t(machine_id, sensor_name, warning_threshold, critical_threshold, unit);


-- Enriched streaming table with anomaly status
CREATE OR REFRESH STREAMING TABLE enriched_events
COMMENT 'Sensor events enriched with anomaly detection scores'
AS
SELECT
  r.machine_id,
  r.machine_type,
  r.sensor_name,
  r.value,
  r.unit,
  r.timestamp,
  r.is_fault,
  r.ingested_at,
  t.warning_threshold,
  t.critical_threshold,
  CASE
    -- Sensors where HIGH values indicate faults (temp, pressure, vibration, load, current)
    WHEN t.warning_threshold < t.critical_threshold THEN
      CASE
        WHEN r.value >= t.critical_threshold THEN 'CRITICAL'
        WHEN r.value >= t.warning_threshold THEN 'WARNING'
        ELSE 'NORMAL'
      END
    -- Sensors where LOW values indicate faults (speed, cycle_count)
    ELSE
      CASE
        WHEN r.value <= t.critical_threshold THEN 'CRITICAL'
        WHEN r.value <= t.warning_threshold THEN 'WARNING'
        ELSE 'NORMAL'
      END
  END AS anomaly_status,
  current_timestamp() AS processed_at
FROM STREAM(raw_sensor_events) r
LEFT JOIN LIVE.sensor_thresholds t
  ON r.machine_id = t.machine_id AND r.sensor_name = t.sensor_name;
