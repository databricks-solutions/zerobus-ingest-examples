-- ============================================================
-- Databricks Delta Table Setup for OPC UA Telemetry
-- Run this in a Databricks SQL editor or notebook.
-- Replace <catalog> and <schema> with your actual names.
-- ============================================================

-- 1. Create catalog and schema if they don't exist
CREATE CATALOG IF NOT EXISTS <catalog>;
CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>;

-- 2. Create the target Delta table (wide format, one column per tag)
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.opcua_telemetry (

  -- Row identifier
  line                      STRING    COMMENT 'Production line (LineA, LineB, ...)',

  -- LineA + LineB tags
  Temperature               DOUBLE    COMMENT 'Temperature sensor reading',
  Humidity                  DOUBLE    COMMENT 'Humidity sensor reading',
  Status                    STRING    COMMENT 'Process status',

  -- LineA-only tags
  Pressure                  DOUBLE    COMMENT 'Pressure sensor reading',
  Running                   BOOLEAN   COMMENT 'Running state indicator',

  -- Timestamp
  ts                        BIGINT    COMMENT 'Epoch microseconds UTC'
)
COMMENT 'OPC UA sensor telemetry ingested via Zerobus (wide format, LineB rows have NULLs for LineA-only columns)';

-- 3. Grant permissions to the service principal used by the client
-- Replace <service_principal_name> with your Azure AD / Entra SPN name.
GRANT USE_CATALOG ON CATALOG <catalog> TO `<service_principal_name>`;
GRANT USE_SCHEMA  ON SCHEMA  <catalog>.<schema> TO `<service_principal_name>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.opcua_telemetry TO `<service_principal_name>`;

-- 4. Quick verification queries (run after data starts flowing)
SELECT COUNT(*) FROM <catalog>.<schema>.opcua_telemetry;
SELECT * FROM <catalog>.<schema>.opcua_telemetry ORDER BY ts DESC LIMIT 20;
