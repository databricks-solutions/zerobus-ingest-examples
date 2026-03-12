# Databricks notebook source
# MAGIC %md
# MAGIC # Create Weather Station Table
# MAGIC
# MAGIC This notebook creates the Delta table for storing weather data from the IoT weather station.
# MAGIC Data is ingested via Zerobus Ingest REST API (only when conditions change significantly).
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `weather_table`: Full table name (catalog.schema.table)

# COMMAND ----------

# Get parameters from DAB
weather_table = dbutils.widgets.get("weather_table")

print(f"Creating weather station table: {weather_table}")

# COMMAND ----------

# Ensure schema exists
table_parts = weather_table.split(".")
if len(table_parts) != 3:
    raise ValueError(
        f"weather_table must be in format 'catalog.schema.table', got: {weather_table}"
    )

schema_id = ".".join(table_parts[:2])
print(f"Ensuring schema exists: {schema_id}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_id}")

# COMMAND ----------

# Create the weather station table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {weather_table} (
  -- Station identification
  station_id STRING COMMENT 'Unique identifier for the weather station instance',
  station_name STRING COMMENT 'Name of the weather station',
  station_location STRING COMMENT 'Location description of the weather station',

  -- Timestamp
  timestamp BIGINT COMMENT 'Unix timestamp in microseconds when weather was recorded',

  -- Wind conditions
  wind_speed_knots DOUBLE COMMENT 'Wind speed in knots',
  wind_direction_degrees DOUBLE COMMENT 'Wind direction in degrees (where wind comes FROM, 0-360)',

  -- Weather event information
  event_type STRING COMMENT 'Type of weather event: stable, frontal_passage, gradual_shift, gust',
  in_transition BOOLEAN COMMENT 'Whether weather is currently transitioning to new conditions',
  time_in_state_seconds DOUBLE COMMENT 'How long current weather state has persisted (seconds)',
  next_change_in_seconds DOUBLE COMMENT 'Estimated time until next weather change (seconds, 0 if in transition)'
)
USING DELTA
COMMENT 'Weather station data for sailboat race course - emits records only when weather conditions change significantly'
""")

print(f"✅ Weather station table created successfully: {weather_table}")
