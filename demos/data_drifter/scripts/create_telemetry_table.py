#!/usr/bin/env python3
"""
Create the sailboat_telemetry table using Databricks SDK.
This script is called by deploy.sh to ensure the telemetry table exists.

Authentication:
  Uses DATABRICKS_CONFIG_PROFILE environment variable set by deploy.sh
  to authenticate via .databrickscfg profile.
"""

import sys
import os
import toml
from databricks.sdk import WorkspaceClient

def load_config():
    """Load configuration from config.toml"""
    config_path = os.path.join(os.path.dirname(__file__), "..", "config.toml")
    return toml.load(config_path)

def create_telemetry_table():
    """Create the sailboat telemetry table if it doesn't exist"""
    try:
        config = load_config()

        # Get configuration
        table_name = config["zerobus"].get("table_name")
        if not table_name:
            print("❌ table_name not found in config.toml")
            return 1

        warehouse_id = config["warehouse"]["sql_warehouse_id"]

        print(f"📋 Checking telemetry table: {table_name}")

        # Create WorkspaceClient - let it use credentials from environment
        # (DATABRICKS_CONFIG_PROFILE is set by deploy.sh)
        w = WorkspaceClient()

        # Validate and parse catalog/schema/table from full name
        table_parts = table_name.split(".")
        if len(table_parts) != 3:
            print(
                "❌ table_name must be in format 'catalog.schema.table', "
                f"got: {table_name}"
            )
            return 1

        schema_id = ".".join(table_parts[:2])

        # Ensure schema exists before creating table
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_id}"
        print(f"🗂️ Ensuring schema exists: {schema_id}")
        schema_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=create_schema_sql,
            wait_timeout="30s"
        )

        if schema_result.status.state.value != "SUCCEEDED":
            print(f"❌ Schema creation failed with state: {schema_result.status.state.value}")
            if schema_result.status.error:
                print(f"   Error: {schema_result.status.error.message}")
            return 1

        # SQL to create table
        create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
  boat_id STRING COMMENT 'Unique identifier for the sailboat',
  boat_name STRING COMMENT 'Name of the sailboat',
  boat_type STRING COMMENT 'Type/class of the sailboat',
  timestamp BIGINT COMMENT 'Unix timestamp in microseconds when telemetry was recorded',

  latitude DOUBLE COMMENT 'GPS latitude in decimal degrees',
  longitude DOUBLE COMMENT 'GPS longitude in decimal degrees',

  speed_over_ground_knots DOUBLE COMMENT 'Speed over ground in knots',
  course_over_ground_degrees DOUBLE COMMENT 'Course over ground in degrees (0-360)',

  wind_speed_knots DOUBLE COMMENT 'True wind speed in knots',
  wind_direction_degrees DOUBLE COMMENT 'True wind direction in degrees (where wind comes FROM, 0-360)',
  apparent_wind_speed_knots DOUBLE COMMENT 'Apparent wind speed in knots',
  apparent_wind_angle_degrees DOUBLE COMMENT 'Apparent wind angle in degrees',

  heading_degrees DOUBLE COMMENT 'Boat heading in degrees (0-360)',
  heel_angle_degrees DOUBLE COMMENT 'Boat heel angle in degrees',

  sail_configuration ARRAY<STRING> COMMENT 'Current sail configuration (e.g., main, jib, spinnaker)',
  autopilot_engaged BOOLEAN COMMENT 'Whether autopilot is engaged',
  water_temperature_celsius DOUBLE COMMENT 'Water temperature in Celsius',
  air_temperature_celsius DOUBLE COMMENT 'Air temperature in Celsius',
  battery_voltage DOUBLE COMMENT 'Battery voltage in volts',

  distance_traveled_nm DOUBLE COMMENT 'Total distance traveled in nautical miles',
  distance_to_destination_nm DOUBLE COMMENT 'Distance remaining to current destination in nautical miles',
  vmg_knots DOUBLE COMMENT 'Velocity Made Good toward destination in knots',

  current_mark_index INT COMMENT 'Index of the current mark the boat is sailing toward',
  marks_rounded INT COMMENT 'Number of marks successfully rounded',
  total_marks INT COMMENT 'Total number of marks in the race course',
  distance_to_current_mark_nm DOUBLE COMMENT 'Distance to the current mark in nautical miles',

  has_started BOOLEAN COMMENT 'Whether the boat has crossed the start line',
  has_finished BOOLEAN COMMENT 'Whether the boat has crossed the finish line',
  race_status STRING COMMENT 'Current race status: not_started, racing, finished, or dnf'
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Sailboat telemetry data for real-time race tracking - contains GPS position, speed, wind data, and race progress'
"""

        # Execute SQL statement
        print(f"📝 Creating telemetry table if it doesn't exist...")

        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=create_table_sql,
            wait_timeout="30s"
        )

        if result.status.state.value == "SUCCEEDED":
            print(f"✅ Telemetry table created/verified successfully: {table_name}")
            return 0
        else:
            print(f"❌ Table creation failed with state: {result.status.state.value}")
            if result.status.error:
                print(f"   Error: {result.status.error.message}")
            return 1

    except Exception as e:
        print(f"❌ Error creating telemetry table: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(create_telemetry_table())
