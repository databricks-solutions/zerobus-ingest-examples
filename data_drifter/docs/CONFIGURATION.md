# Configuration Guide

## Overview

The project uses a single `config.toml` file for all configuration. This file controls:
- Databricks connection details
- Race course setup
- Fleet composition
- Telemetry generation parameters

## Configuration File Structure

### Zerobus Connection

```toml
[zerobus]
server_endpoint = "your-zerobus-endpoint.cloud.databricks.com"
workspace_url = "https://your-workspace.cloud.databricks.com"
table_name = "catalog.schema.table_name"
weather_station_table_name = "catalog.schema.weather_station"
```

**Required Fields:**
- `server_endpoint`: Zerobus ingest endpoint URL
- `workspace_url`: Your Databricks workspace URL
- `table_name`: Fully qualified table name for telemetry (catalog.schema.table)
- `weather_station_table_name`: Fully qualified table name for weather station data

**OAuth Credentials:**
OAuth credentials (`client_id` and `client_secret`) are now passed as command-line arguments to prevent accidental exposure:
```bash
python3 main.py --client-id <your-client-id> --client-secret <your-secret>
```

### Race Course Configuration

```toml
[race_course]
start_lat = 37.8044
start_lon = -122.2712
line_length_nm = 0.1

# Course marks (latitude, longitude)
marks = [
    [37.85, -122.35],    # Mark 1
    [37.82, -122.40],    # Mark 2
    [37.79, -122.38],    # Mark 3 (also finish line)
]

mark_rounding_radius_nm = 0.05
```

**Fields:**
- `start_lat`/`start_lon`: Center point of start line
- `line_length_nm`: Length of start/finish line in nautical miles
- `marks`: Array of [latitude, longitude] coordinates
  - Last mark is always the finish line
  - Boats must round all marks except the last one
- `mark_rounding_radius_nm`: How close boats must get to "round" a mark

### Fleet Configuration

```toml
[fleet]
num_boats = 8

# Boat types and their performance characteristics
[[fleet.boat_types]]
name = "Sunfish"
base_speed_knots = 4.5
speed_variation = 0.3
upwind_angle_deg = 45
downwind_angle_deg = 160

[[fleet.boat_types]]
name = "Laser"
base_speed_knots = 5.5
speed_variation = 0.4
upwind_angle_deg = 42
downwind_angle_deg = 165
```

**Fields:**
- `num_boats`: Total number of boats in the fleet
- `boat_types`: Array of boat type definitions
  - `name`: Boat class name
  - `base_speed_knots`: Average speed in ideal conditions
  - `speed_variation`: Random speed variation (+/- percentage)
  - `upwind_angle_deg`: Optimal angle when sailing upwind
  - `downwind_angle_deg`: Optimal angle when sailing downwind

### Wind Conditions

```toml
[wind]
base_direction_deg = 270.0
direction_variation_deg = 15.0
min_speed_knots = 8.0
max_speed_knots = 18.0
```

**Fields:**
- `base_direction_deg`: Primary wind direction (where wind comes FROM)
  - 0° = North, 90° = East, 180° = South, 270° = West
- `direction_variation_deg`: Wind direction shifts (+/- degrees)
- `min_speed_knots`: Minimum wind speed
- `max_speed_knots`: Maximum wind speed

### Telemetry Generation

```toml
[telemetry]
race_start_time = "2024-01-20T10:00:00Z"
race_duration_hours = 4.0
simulated_time_step_seconds = 1.0
time_acceleration = 60.0
send_interval_seconds = 0.1
batch_size = 10
```

**Fields:**
- `race_start_time`: ISO 8601 timestamp for race start
- `race_duration_hours`: Total race duration
- `simulated_time_step_seconds`: Simulation time per update
  - 1.0 = 1 second of race time per update
- `time_acceleration`: Real-time speedup factor
  - 60.0 = 60x faster than real time
- `send_interval_seconds`: Delay between sending batches
- `batch_size`: Number of records per batch

### Racing Strategy

```toml
[[fleet.racing_strategies]]
name = "aggressive"
tack_angle_variation = 5.0
tack_frequency_multiplier = 1.3
risk_tolerance = 0.8

[[fleet.racing_strategies]]
name = "conservative"
tack_angle_variation = 2.0
tack_frequency_multiplier = 0.8
risk_tolerance = 0.3
```

**Fields:**
- `name`: Strategy name
- `tack_angle_variation`: Deviation from optimal angle
- `tack_frequency_multiplier`: How often boat tacks
  - < 1.0 = less frequent tacking
  - > 1.0 = more frequent tacking
- `risk_tolerance`: Willingness to take risks (0.0-1.0)

## Example: Simple Configuration

Minimal configuration for quick start:

```toml
[zerobus]
server_endpoint = "your-endpoint.cloud.databricks.com"
workspace_url = "https://your-workspace.cloud.databricks.com"
table_name = "main.sailboat.telemetry"
weather_station_table_name = "main.sailboat.weather_station"

[warehouse]
sql_warehouse_id = "your-warehouse-id"

[race_course]
start_lat = 37.8044
start_lon = -122.2712
marks = [
    [37.85, -122.35],
    [37.79, -122.38]
]

[fleet]
num_boats = 5

[telemetry]
race_start_time = "2024-01-20T10:00:00Z"
race_duration_hours = 2.0
time_acceleration = 60.0
```

**Note:** OAuth credentials are passed as command-line arguments, not in config:
```bash
python3 main.py --client-id <your-client-id> --client-secret <your-secret>
```

## Environment-Specific Configuration

For different environments (dev, staging, prod), you can:

1. **Use separate config files:**
   ```bash
   cp config.toml config.prod.toml
   # Edit config.prod.toml
   ```

2. **Use environment variables to override:**
   ```bash
   export ZEROBUS_TARGET_TABLE="prod_catalog.schema.table"
   ```

## Validation

The application validates configuration on startup and will error if:
- Required fields are missing
- Invalid data types
- Coordinates out of range
- Negative values for positive-only fields
