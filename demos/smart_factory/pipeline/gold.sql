-- Gold: Aggregated health KPIs for dashboards

-- Anomaly timeline — streaming table for instant anomaly visibility
CREATE OR REFRESH STREAMING TABLE anomaly_timeline
COMMENT 'Real-time anomaly events for timeline visualization'
AS
SELECT
  machine_id,
  machine_type,
  sensor_name,
  value,
  unit,
  anomaly_status,
  warning_threshold,
  critical_threshold,
  timestamp,
  processed_at
FROM STREAM(enriched_events)
WHERE anomaly_status != 'NORMAL';


-- Machine health scores (MV, refreshes on pipeline interval)
CREATE OR REFRESH MATERIALIZED VIEW machine_health_kpis
COMMENT 'Machine health KPIs for dashboards'
AS
SELECT
  machine_id,
  machine_type,
  sensor_name,
  COUNT(*) AS total_readings,
  COUNT(CASE WHEN anomaly_status = 'CRITICAL' THEN 1 END) AS critical_count,
  COUNT(CASE WHEN anomaly_status = 'WARNING' THEN 1 END) AS warning_count,
  ROUND(AVG(value), 2) AS avg_value,
  ROUND(MAX(value), 2) AS max_value,
  ROUND(MIN(value), 2) AS min_value,
  MAX(timestamp) AS last_reading_at,
  GREATEST(0, 100
    - (COUNT(CASE WHEN anomaly_status = 'CRITICAL' THEN 1 END) * 10)
    - (COUNT(CASE WHEN anomaly_status = 'WARNING' THEN 1 END) * 3)
  ) AS health_score
FROM enriched_events
WHERE timestamp > current_timestamp() - INTERVAL 5 MINUTES
GROUP BY machine_id, machine_type, sensor_name;


-- Overall machine health summary (MV, one row per machine)
CREATE OR REFRESH MATERIALIZED VIEW machine_summary
COMMENT 'One-row-per-machine health summary'
AS
SELECT
  machine_id,
  machine_type,
  MIN(health_score) AS worst_sensor_health,
  ROUND(AVG(health_score), 0) AS avg_health_score,
  SUM(critical_count) AS total_criticals,
  SUM(warning_count) AS total_warnings,
  MAX(last_reading_at) AS last_activity
FROM machine_health_kpis
GROUP BY machine_id, machine_type;
