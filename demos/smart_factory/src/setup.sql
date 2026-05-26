-- One-time setup for dilan_catalog workspace
-- Schemas already created; this creates the landing table for ZeroBus

CREATE TABLE IF NOT EXISTS dilan_catalog.smartfactory_landing.raw_sensor_events (
  machine_id STRING NOT NULL,
  machine_type STRING NOT NULL,
  sensor_name STRING NOT NULL,
  value DOUBLE NOT NULL,
  unit STRING NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  is_fault BOOLEAN DEFAULT false
)
USING DELTA
COMMENT 'Raw IoT sensor events ingested via ZeroBus';
