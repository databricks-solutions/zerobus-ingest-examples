# Telemetry Data Schema

## Table Schema

The telemetry data is stored in a Unity Catalog table with the following schema:

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<table> (
  boat_id STRING COMMENT 'Unique identifier for each boat',
  boat_name STRING COMMENT 'Name of the boat',
  boat_type STRING COMMENT 'Type/class of boat',
  timestamp BIGINT COMMENT 'Unix timestamp in microseconds',
  latitude DOUBLE COMMENT 'GPS latitude in decimal degrees',
  longitude DOUBLE COMMENT 'GPS longitude in decimal degrees',
  speed_over_ground_knots DOUBLE COMMENT 'Speed over ground in knots',
  course_over_ground_degrees DOUBLE COMMENT 'Course over ground in degrees',
  wind_speed_knots DOUBLE COMMENT 'True wind speed in knots',
  wind_direction_degrees DOUBLE COMMENT 'True wind direction in degrees (where wind comes FROM)',
  apparent_wind_speed_knots DOUBLE COMMENT 'Apparent wind speed in knots',
  apparent_wind_angle_degrees DOUBLE COMMENT 'Apparent wind angle in degrees',
  heading_degrees DOUBLE COMMENT 'True heading in degrees (0-360)',
  heel_angle_degrees DOUBLE COMMENT 'Boat heel angle in degrees',
  sail_configuration ARRAY<STRING> COMMENT 'Current sail configuration',
  autopilot_engaged BOOLEAN COMMENT 'Whether autopilot is engaged',
  water_temperature_celsius DOUBLE COMMENT 'Water temperature in Celsius',
  air_temperature_celsius DOUBLE COMMENT 'Air temperature in Celsius',
  battery_voltage DOUBLE COMMENT 'Battery voltage',
  distance_traveled_nm DOUBLE COMMENT 'Total distance traveled in nautical miles',
  distance_to_destination_nm DOUBLE COMMENT 'Distance to current destination (mark or finish) in nautical miles',
  vmg_knots DOUBLE COMMENT 'Velocity Made Good in knots',
  current_mark_index INT COMMENT 'Index of next mark to round (0-based)',
  marks_rounded INT COMMENT 'Number of marks rounded so far',
  total_marks INT COMMENT 'Total number of marks in race',
  distance_to_current_mark_nm DOUBLE COMMENT 'Distance to next mark in nautical miles',
  has_started BOOLEAN COMMENT 'Whether boat has started the race',
  has_finished BOOLEAN COMMENT 'Whether boat has finished the race',
  race_status STRING COMMENT 'Current race status: not_started, racing, finished, dnf'
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);
```

## Field Descriptions

### Boat Identity
- **boat_id**: Unique identifier (UUID format)
- **boat_name**: Human-readable name (e.g., "Sarah's Sunfish")
- **boat_type**: Boat class (e.g., "Sunfish", "Laser", "470")

### Position Data
- **timestamp**: Microsecond precision Unix timestamp
- **latitude**: Decimal degrees, positive = North
- **longitude**: Decimal degrees, positive = East

### Motion Data
- **speed_over_ground_knots**: Boat speed relative to ground/water
- **course_over_ground_degrees**: Course over ground (direction of travel)
- **heading_degrees**: True heading (0° = North, 90° = East, etc.)
- **vmg_knots**: Velocity Made Good - effective speed toward destination

### Wind Data
- **wind_speed_knots**: True wind speed
- **wind_direction_degrees**: True wind direction - where wind comes FROM (0° = North wind)
- **apparent_wind_speed_knots**: Wind speed as experienced by the boat (includes boat speed)
- **apparent_wind_angle_degrees**: Angle of apparent wind relative to boat heading

### Boat Attitude
- **heel_angle_degrees**: Angle of boat tilt/heel

### Environmental Data
- **water_temperature_celsius**: Water temperature
- **air_temperature_celsius**: Air temperature

### Equipment & Systems
- **sail_configuration**: Array of current sail setup (e.g., ["mainsail", "jib"], ["spinnaker", "mainsail"])
- **autopilot_engaged**: Whether autopilot system is active
- **battery_voltage**: Onboard battery voltage

### Race Progress
- **distance_traveled_nm**: Cumulative distance sailed
- **distance_to_destination_nm**: Distance to next mark or finish
- **distance_to_current_mark_nm**: Direct distance to the next mark to round
- **current_mark_index**: Which mark to round next (0 = first mark)
- **marks_rounded**: How many marks have been rounded
- **total_marks**: Total marks in the course
- **has_started**: Boolean flag for race start
- **has_finished**: Boolean flag for race completion
- **race_status**: Current status string

## Race Status Values

| Status | Description |
|--------|-------------|
| `not_started` | Boat has not crossed start line |
| `racing` | Boat is actively racing |
| `finished` | Boat has crossed finish line |
| `dnf` | Did Not Finish (retired from race) |

## Sample Data

```json
{
  "boat_id": "550e8400-e29b-41d4-a716-446655440000",
  "boat_name": "Sarah's Sunfish",
  "boat_type": "Sunfish",
  "timestamp": 1705689600000000,
  "latitude": 37.8044,
  "longitude": -122.2712,
  "speed_over_ground_knots": 5.2,
  "course_over_ground_degrees": 47.3,
  "wind_speed_knots": 12.5,
  "wind_direction_degrees": 270.0,
  "apparent_wind_speed_knots": 15.8,
  "apparent_wind_angle_degrees": 38.5,
  "heading_degrees": 45.0,
  "heel_angle_degrees": 12.3,
  "sail_configuration": ["mainsail", "jib"],
  "autopilot_engaged": false,
  "water_temperature_celsius": 15.4,
  "air_temperature_celsius": 21.2,
  "battery_voltage": 13.8,
  "distance_traveled_nm": 2.3,
  "distance_to_destination_nm": 1.7,
  "vmg_knots": 4.8,
  "current_mark_index": 1,
  "marks_rounded": 1,
  "total_marks": 3,
  "distance_to_current_mark_nm": 0.45,
  "has_started": true,
  "has_finished": false,
  "race_status": "racing"
}
```

## Data Update Frequency

- Default: 1 record per second per boat
- Configurable via `simulated_time_step_seconds` in config.toml
- Higher frequency = more granular tracking, higher data volume

---

## Weather Station Table Schema

The weather station data is stored in a separate Unity Catalog table with the following schema:

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.weather_station (
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
COMMENT 'Weather station data for sailboat race course - emits records only when weather conditions change significantly';
```

### Weather Event Types

| Event Type | Description |
|-----------|-------------|
| `stable` | Wind conditions are stable, no changes in progress |
| `frontal_passage` | Significant wind shift (20-40° direction change, 5-10 knots speed change) |
| `gradual_shift` | Slow, steady wind change (5-15° direction, 2-5 knots speed) |
| `gust` | Temporary wind speed spike (5-8 knots increase, decays over ~1 minute) |

### Weather Data Emission

Unlike telemetry data (continuous), weather station data is **event-driven**:
- Emits records only when wind conditions change significantly (>2 knots or >5°)
- Emits when weather event type changes
- Uses REST API for ingestion (vs SDK/gRPC for telemetry)
