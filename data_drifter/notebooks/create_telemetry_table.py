# Databricks notebook source
# MAGIC %md
# MAGIC # Create Sailboat Telemetry Table
# MAGIC
# MAGIC This notebook creates the Delta table for storing real-time sailboat telemetry data.
# MAGIC Data is ingested via Zerobus Ingest (SDK with gRPC).
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `telemetry_table`: Full table name (catalog.schema.table)

# COMMAND ----------

# Get parameters from DAB
telemetry_table = dbutils.widgets.get("telemetry_table")

print(f"Creating telemetry table: {telemetry_table}")

# COMMAND ----------

# Ensure schema exists
table_parts = telemetry_table.split(".")
if len(table_parts) != 3:
    raise ValueError(
        f"telemetry_table must be in format 'catalog.schema.table', got: {telemetry_table}"
    )

schema_id = ".".join(table_parts[:2])
print(f"Ensuring schema exists: {schema_id}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_id}")

# COMMAND ----------

# Create the telemetry table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {telemetry_table} (
  -- Boat identification
  boat_id STRING COMMENT 'Unique identifier for the sailboat',
  boat_name STRING COMMENT 'Name of the sailboat',
  boat_type STRING COMMENT 'Type/class of the sailboat',

  -- Timestamp
  timestamp BIGINT COMMENT 'Unix timestamp in microseconds when telemetry was recorded',

  -- GPS position data
  latitude DOUBLE COMMENT 'GPS latitude in decimal degrees',
  longitude DOUBLE COMMENT 'GPS longitude in decimal degrees',

  -- Speed and course data
  speed_over_ground_knots DOUBLE COMMENT 'Speed over ground in knots',
  course_over_ground_degrees DOUBLE COMMENT 'Course over ground in degrees (0-360)',

  -- Wind data
  wind_speed_knots DOUBLE COMMENT 'True wind speed in knots',
  wind_direction_degrees DOUBLE COMMENT 'True wind direction in degrees (where wind comes FROM, 0-360)',
  apparent_wind_speed_knots DOUBLE COMMENT 'Apparent wind speed in knots',
  apparent_wind_angle_degrees DOUBLE COMMENT 'Apparent wind angle in degrees',

  -- Boat attitude
  heading_degrees DOUBLE COMMENT 'Boat heading in degrees (0-360)',
  heel_angle_degrees DOUBLE COMMENT 'Boat heel angle in degrees',

  -- Additional metadata
  sail_configuration ARRAY<STRING> COMMENT 'Current sail configuration (e.g., main, jib, spinnaker)',
  autopilot_engaged BOOLEAN COMMENT 'Whether autopilot is engaged',
  water_temperature_celsius DOUBLE COMMENT 'Water temperature in Celsius',
  air_temperature_celsius DOUBLE COMMENT 'Air temperature in Celsius',
  battery_voltage DOUBLE COMMENT 'Battery voltage in volts',

  -- Race statistics
  distance_traveled_nm DOUBLE COMMENT 'Total distance traveled in nautical miles',
  distance_to_destination_nm DOUBLE COMMENT 'Distance remaining to current destination in nautical miles',
  vmg_knots DOUBLE COMMENT 'Velocity Made Good toward destination in knots',

  -- Mark progress
  current_mark_index INT COMMENT 'Index of the current mark the boat is sailing toward',
  marks_rounded INT COMMENT 'Number of marks successfully rounded',
  total_marks INT COMMENT 'Total number of marks in the race course',
  distance_to_current_mark_nm DOUBLE COMMENT 'Distance to the current mark in nautical miles',

  -- Race status
  has_started BOOLEAN COMMENT 'Whether the boat has crossed the start line',
  has_finished BOOLEAN COMMENT 'Whether the boat has crossed the finish line',
  race_status STRING COMMENT 'Current race status: not_started, racing, finished, or dnf'
)
USING DELTA
COMMENT 'Sailboat telemetry data for real-time race tracking - contains GPS position, speed, wind data, and race progress'
""")

print(f"✅ Telemetry table created successfully: {telemetry_table}")
