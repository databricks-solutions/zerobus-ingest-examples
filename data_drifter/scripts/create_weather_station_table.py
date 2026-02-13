#!/usr/bin/env python3
"""
Create the weather_station table using Databricks SDK.
This script is called by deploy.sh to ensure the weather station table exists.

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

def create_weather_station_table():
    """Create the weather_station table if it doesn't exist"""
    try:
        config = load_config()

        # Get configuration
        weather_table_name = config["zerobus"].get("weather_station_table_name")
        if not weather_table_name:
            print("⚠️  weather_station_table_name not found in config.toml")
            return 0

        warehouse_id = config["warehouse"]["sql_warehouse_id"]

        print(f"📋 Checking weather station table: {weather_table_name}")

        # Create WorkspaceClient - let it use credentials from environment
        # (DATABRICKS_CONFIG_PROFILE is set by deploy.sh)
        w = WorkspaceClient()

        # SQL to create table
        create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {weather_table_name} (
  station_id STRING COMMENT 'Unique identifier for the weather station instance',
  station_name STRING COMMENT 'Name of the weather station',
  station_location STRING COMMENT 'Location description of the weather station',
  timestamp BIGINT COMMENT 'Unix timestamp in microseconds when weather was recorded',
  wind_speed_knots DOUBLE COMMENT 'Wind speed in knots',
  wind_direction_degrees DOUBLE COMMENT 'Wind direction in degrees (where wind comes FROM, 0-360)',
  event_type STRING COMMENT 'Type of weather event: stable, frontal_passage, gradual_shift, gust',
  in_transition BOOLEAN COMMENT 'Whether weather is currently transitioning to new conditions',
  time_in_state_seconds DOUBLE COMMENT 'How long current weather state has persisted (seconds)',
  next_change_in_seconds DOUBLE COMMENT 'Estimated time until next weather change (seconds, 0 if in transition)'
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Weather station data for sailboat race course - emits records only when weather conditions change significantly'
"""

        # Execute SQL statement
        print(f"📝 Creating weather station table if it doesn't exist...")

        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=create_table_sql,
            wait_timeout="30s"
        )

        if result.status.state.value == "SUCCEEDED":
            print(f"✅ Weather station table created/verified successfully: {weather_table_name}")
            return 0
        else:
            print(f"❌ Table creation failed with state: {result.status.state.value}")
            if result.status.error:
                print(f"   Error: {result.status.error.message}")
            return 1

    except Exception as e:
        print(f"❌ Error creating weather station table: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(create_weather_station_table())
