#!/usr/bin/env python3
"""
Parse config.toml and extract variables for deploy.sh
Outputs shell-compatible KEY=value pairs for evaluation by bash script

Usage:
    python3 scripts/parse_config.py

Output format (for bash eval):
    TELEMETRY_TABLE="catalog.schema.table"
    WEATHER_TABLE="catalog.schema.weather"
    CATALOG_NAME="catalog"
    SCHEMA_NAME="schema"
    WAREHOUSE_ID="abc123"
"""

import sys
import os
import toml

def parse_config():
    """Parse config.toml and extract deployment variables"""
    try:
        # Load config.toml
        config_path = os.path.join(os.path.dirname(__file__), "..", "config.toml")
        config = toml.load(config_path)

        # Extract table names
        telemetry_table = config.get("zerobus", {}).get("table_name", "")
        weather_table = config.get("zerobus", {}).get("weather_station_table_name", "")
        warehouse_id = config.get("warehouse", {}).get("sql_warehouse_id", "")
        workspace_url = config.get("zerobus", {}).get("workspace_url", "")

        # Extract race configuration
        race_start_time = config.get("telemetry", {}).get("race_start_time", "")
        race_duration_seconds = config.get("telemetry", {}).get("race_duration_seconds", "")
        real_time_duration_seconds = config.get("telemetry", {}).get("real_time_duration_seconds", "")

        # Extract race course configuration
        race_course_start_lat = config.get("race_course", {}).get("start_lat", "")
        race_course_start_lon = config.get("race_course", {}).get("start_lon", "")
        race_course_marks = config.get("race_course", {}).get("marks", [])

        # Extract fleet configuration
        num_boats = config.get("fleet", {}).get("num_boats", "")

        if not telemetry_table:
            print("ERROR: table_name not found in config.toml [zerobus] section", file=sys.stderr)
            return 1

        if not warehouse_id:
            print("ERROR: sql_warehouse_id not found in config.toml [warehouse] section", file=sys.stderr)
            return 1

        if not workspace_url:
            print("ERROR: workspace_url not found in config.toml [zerobus] section", file=sys.stderr)
            return 1

        # Parse catalog and schema from telemetry table name
        # Format: catalog.schema.table
        parts = telemetry_table.split(".")
        if len(parts) != 3:
            print(f"ERROR: table_name must be in format 'catalog.schema.table', got: {telemetry_table}", file=sys.stderr)
            return 1

        catalog_name = parts[0]
        schema_name = parts[1]
        table_name = parts[2]

        # Convert race_course_marks to JSON string
        import json
        race_course_marks_json = json.dumps(race_course_marks).replace(" ", "")

        # Output shell-compatible variables
        print(f'TELEMETRY_TABLE="{telemetry_table}"')
        print(f'WEATHER_TABLE="{weather_table}"')
        print(f'CATALOG_NAME="{catalog_name}"')
        print(f'SCHEMA_NAME="{schema_name}"')
        print(f'TABLE_NAME="{table_name}"')
        print(f'WAREHOUSE_ID="{warehouse_id}"')
        print(f'WORKSPACE_URL="{workspace_url}"')

        # Race configuration
        print(f'RACE_START_TIME="{race_start_time}"')
        print(f'RACE_DURATION_SECONDS="{race_duration_seconds}"')
        print(f'REAL_TIME_DURATION_SECONDS="{real_time_duration_seconds}"')

        # Race course configuration
        print(f'RACE_COURSE_START_LAT="{race_course_start_lat}"')
        print(f'RACE_COURSE_START_LON="{race_course_start_lon}"')
        # For JSON, use double quotes for bash compatibility
        print(f'RACE_COURSE_MARKS="{race_course_marks_json}"')

        # Fleet configuration
        print(f'NUM_BOATS="{num_boats}"')

        # Output full table names for convenience
        print(f'TELEMETRY_TABLE_FULL="{telemetry_table}"')
        if weather_table:
            # Parse weather table parts
            weather_parts = weather_table.split(".")
            if len(weather_parts) == 3:
                print(f'WEATHER_TABLE_FULL="{weather_table}"')
            else:
                print(f'WEATHER_TABLE_FULL=""')
        else:
            print(f'WEATHER_TABLE_FULL=""')

        return 0

    except FileNotFoundError:
        print("ERROR: config.toml not found", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"ERROR: Failed to parse config.toml: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(parse_config())
