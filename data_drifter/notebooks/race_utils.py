"""
Race Analysis Utilities

Common functions used across sailboat telemetry analysis notebooks.
"""

import math
import os
import yaml
from pyspark.sql.types import DoubleType, StringType
import pyspark.sql.functions as F

# Try Python 3.11+ tomllib, fallback to tomli
try:
    import tomllib
except ImportError:
    import tomli as tomllib


def _read_config_path_from_databricks_yml():
    """
    Read the config file path from databricks.yml.

    Returns:
        str: Path to config file as specified in databricks.yml

    Raises:
        FileNotFoundError: If databricks.yml is not found
        KeyError: If config_file_path variable is not defined
    """
    # databricks.yml is always at ../databricks.yml relative to the notebooks directory
    databricks_yml_path = os.path.join(os.path.dirname(__file__), "..", "databricks.yml")

    if not os.path.exists(databricks_yml_path):
        raise FileNotFoundError(
            f"ERROR: databricks.yml not found at {databricks_yml_path}. "
            "Cannot determine config file path."
        )

    # Read databricks.yml
    with open(databricks_yml_path, "r") as f:
        dab_config = yaml.safe_load(f)

    # Get config_file_path variable
    try:
        config_path = dab_config["variables"]["config_file_path"]["default"]
    except KeyError:
        raise KeyError(
            "ERROR: 'variables.config_file_path.default' not found in databricks.yml"
        )

    # Resolve ${workspace.file_path} variable
    if "${workspace.file_path}" in config_path:
        # workspace.file_path is the directory containing databricks.yml
        workspace_path = os.path.dirname(os.path.abspath(databricks_yml_path))
        config_path = config_path.replace("${workspace.file_path}", workspace_path)

    return config_path


def load_race_course_config(config_path=None):
    """
    Load race course configuration from config.toml.

    The config file path is read from databricks.yml unless explicitly provided.

    Args:
        config_path: Optional explicit path to config.toml file.
                    If not provided, reads path from databricks.yml.

    Returns:
        tuple: (table_name, marks, config_dict) where:
            - table_name is the telemetry table name (catalog.schema.table format)
            - marks is list of [lat, lon] coordinates
            - config_dict is the full configuration dictionary

    Raises:
        FileNotFoundError: If config.toml or databricks.yml is not found
        KeyError: If required keys are missing from config files
    """
    print("Loading race course configuration...")

    # Get config path from databricks.yml if not explicitly provided
    if config_path is None:
        config_path = _read_config_path_from_databricks_yml()
        print(f"  Config path from databricks.yml: {config_path}")
    else:
        print(f"  Using explicitly provided config path: {config_path}")

    try:
        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        # Extract table name and marks from config
        table_name = config["zerobus"]["table_name"]
        marks = config["race_course"]["marks"]

        print(f"✓ Loaded configuration from {config_path}")
        print(f"  Table: {table_name}")
        print(f"  Marks: {len(marks)} waypoints")

        return table_name, marks, config

    except FileNotFoundError:
        raise FileNotFoundError(
            f"ERROR: config.toml not found at {config_path}. "
            "Cannot proceed without configuration."
        )
    except KeyError as e:
        raise KeyError(
            f"ERROR: Missing required key in config.toml: {e}. "
            "Cannot proceed without configuration."
        )


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate great-circle distance between two points using Haversine formula.

    Args:
        lat1: Latitude of first point in degrees
        lon1: Longitude of first point in degrees
        lat2: Latitude of second point in degrees
        lon2: Longitude of second point in degrees

    Returns:
        float: Distance in nautical miles, or None if any input is None
    """
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    R = 3440.065  # Earth radius in nautical miles

    # Convert to radians
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    # Haversine formula
    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return R * c


def create_remaining_distance_udf(marks):
    """
    Create a UDF for calculating remaining distance to finish line.

    Args:
        marks: List of [lat, lon] coordinates for race course marks

    Returns:
        pyspark.sql.Column: UDF that can be used in withColumn()
    """
    def calculate_remaining_distance(lat, lon, current_mark_index):
        """Calculate remaining distance from current position to finish line"""
        if lat is None or lon is None or current_mark_index is None:
            return None

        # Convert to int (may be float from Spark)
        current_mark_index = int(current_mark_index)

        # If already finished or past all marks, remaining distance is 0
        if current_mark_index >= len(marks):
            return 0.0

        total_distance = 0.0

        # Distance from current position to next mark
        next_mark = marks[current_mark_index]
        total_distance += haversine_distance(lat, lon, next_mark[0], next_mark[1])

        # Distance between all remaining marks to finish
        for i in range(current_mark_index, len(marks) - 1):
            mark_from = marks[i]
            mark_to = marks[i + 1]
            total_distance += haversine_distance(mark_from[0], mark_from[1], mark_to[0], mark_to[1])

        return total_distance

    # Register and return UDF
    return F.udf(calculate_remaining_distance, DoubleType())


def classify_point_of_sail(wind_dir, heading):
    """
    Classify point of sail based on angle to wind.

    Point of sail categories (by angle to true wind):
    - Close-hauled: 30-50° - sailing as close to wind as possible
    - Close reach: 50-70° - sailing slightly off the wind
    - Beam reach: 70-110° - sailing perpendicular to wind
    - Broad reach: 110-150° - sailing downwind at an angle
    - Running: 150-180° - sailing directly downwind

    Args:
        wind_dir: True wind direction in degrees (0-360)
        heading: Boat heading in degrees (0-360)

    Returns:
        str: Point of sail category or "Unknown"
    """
    if wind_dir is None or heading is None:
        return "Unknown"

    # Calculate angle between wind direction and heading
    angle = abs(wind_dir - heading) % 360
    if angle > 180:
        angle = 360 - angle

    # Classify based on angle to wind
    if angle < 30:
        return "No-Go Zone"  # Too close to wind, boat cannot sail here
    elif angle < 50:
        return "Close-hauled"
    elif angle < 70:
        return "Close Reach"
    elif angle < 110:
        return "Beam Reach"
    elif angle < 150:
        return "Broad Reach"
    else:
        return "Running"


def create_point_of_sail_udf():
    """
    Create a UDF for classifying point of sail.

    Returns:
        pyspark.sql.Column: UDF that can be used in withColumn()
    """
    return F.udf(classify_point_of_sail, StringType())


def add_remaining_distance_column(df, marks):
    """
    Add remaining_distance_nm column to dataframe.

    Args:
        df: PySpark DataFrame with latitude, longitude, and current_mark_index columns
        marks: List of [lat, lon] coordinates for race course marks

    Returns:
        DataFrame: Input dataframe with remaining_distance_nm column added
    """
    remaining_distance_udf = create_remaining_distance_udf(marks)
    return df.withColumn(
        "remaining_distance_nm",
        remaining_distance_udf(F.col("latitude"), F.col("longitude"), F.col("current_mark_index"))
    )


def add_point_of_sail_column(df):
    """
    Add point_of_sail column to dataframe.

    Args:
        df: PySpark DataFrame with wind_direction_degrees and heading_degrees columns

    Returns:
        DataFrame: Input dataframe with point_of_sail column added
    """
    point_of_sail_udf = create_point_of_sail_udf()
    return df.withColumn(
        "point_of_sail",
        point_of_sail_udf(F.col("wind_direction_degrees"), F.col("heading_degrees"))
    )


def load_weather_station_data(config):
    """
    Load weather station data from the configured table.

    Args:
        config: Configuration dictionary with weather_station_table_name

    Returns:
        DataFrame: Weather station data, or None if table not configured or doesn't exist
    """
    from pyspark.sql import SparkSession

    weather_table = config.get("zerobus", {}).get("weather_station_table_name")

    if not weather_table:
        print("⚠️  Weather station table not configured in config.toml")
        return None

    try:
        spark = SparkSession.builder.getOrCreate()
        df = spark.table(weather_table)
        print(f"✓ Loaded weather station data from {weather_table}")
        print(f"  Records: {df.count():,}")
        return df
    except Exception as e:
        print(f"⚠️  Could not load weather station table: {e}")
        return None


def join_telemetry_with_weather(telemetry_df, weather_df):
    """
    Join telemetry data with weather events using timestamp-based matching.

    Each telemetry record is matched to the most recent weather event
    that occurred before or at the same time as the telemetry record.

    Args:
        telemetry_df: Telemetry DataFrame with timestamp column (microseconds)
        weather_df: Weather DataFrame with timestamp column (microseconds)

    Returns:
        DataFrame: Telemetry data enriched with weather event information
    """
    from pyspark.sql.window import Window

    # Rename weather columns to avoid conflicts
    weather_renamed = weather_df.select(
        F.col("timestamp").alias("weather_timestamp"),
        F.col("wind_speed_knots").alias("weather_wind_speed"),
        F.col("wind_direction_degrees").alias("weather_wind_direction"),
        F.col("event_type").alias("weather_event_type"),
        F.col("in_transition").alias("weather_in_transition"),
        F.col("station_id"),
        F.col("station_name")
    )

    # Join telemetry with weather where weather timestamp <= telemetry timestamp
    joined = telemetry_df.alias("t").join(
        weather_renamed.alias("w"),
        F.col("t.timestamp") >= F.col("w.weather_timestamp"),
        "left"
    )

    # For each telemetry record, get the most recent weather event
    # (the one with the latest weather_timestamp that's still <= telemetry timestamp)
    window = Window.partitionBy("t.boat_id", "t.timestamp").orderBy(F.desc("w.weather_timestamp"))

    result = joined.withColumn("row_num", F.row_number().over(window)) \
                   .filter(F.col("row_num") == 1) \
                   .drop("row_num")

    return result


def summarize_weather_events(weather_df):
    """
    Summarize weather events for the race.

    Args:
        weather_df: Weather station DataFrame

    Returns:
        dict: Summary statistics about weather events
    """
    if weather_df is None:
        return None

    # Count events by type
    event_counts = weather_df.groupBy("event_type").count().collect()
    event_summary = {row["event_type"]: row["count"] for row in event_counts}

    # Calculate wind statistics
    wind_stats = weather_df.agg(
        F.min("wind_speed_knots").alias("min_wind"),
        F.avg("wind_speed_knots").alias("avg_wind"),
        F.max("wind_speed_knots").alias("max_wind"),
        F.min("wind_direction_degrees").alias("min_dir"),
        F.avg("wind_direction_degrees").alias("avg_dir"),
        F.max("wind_direction_degrees").alias("max_dir")
    ).collect()[0]

    # Get first and last event
    ordered = weather_df.orderBy("timestamp")
    first_event = ordered.first()
    last_event = ordered.select(F.last("event_type"), F.last("timestamp")).first()

    return {
        "event_counts": event_summary,
        "total_events": weather_df.count(),
        "wind_speed_min": wind_stats["min_wind"],
        "wind_speed_avg": wind_stats["avg_wind"],
        "wind_speed_max": wind_stats["max_wind"],
        "wind_direction_min": wind_stats["min_dir"],
        "wind_direction_avg": wind_stats["avg_dir"],
        "wind_direction_max": wind_stats["max_dir"],
        "first_event_type": first_event["event_type"] if first_event else None,
        "last_event_type": last_event[0] if last_event else None
    }


def load_race_data(table_name):
    """
    Load telemetry data and print basic statistics.

    Args:
        table_name: Full table name (catalog.schema.table format)

    Returns:
        DataFrame: Telemetry data
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.table(table_name)

    print(f"Total records loaded: {df.count():,}")

    return df


def get_finished_boats(df):
    """
    Get boats that finished the race with rankings by finish time.

    Args:
        df: Telemetry DataFrame with has_finished, distance_traveled_nm, timestamp columns

    Returns:
        DataFrame: Finished boats with rank, total_distance, finish_time columns
    """
    from pyspark.sql.window import Window

    finished = df.filter(F.col("has_finished") == True).groupBy("boat_id", "boat_name").agg(
        F.max("distance_traveled_nm").alias("total_distance"),
        F.min("remaining_distance_nm").alias("final_remaining_distance"),
        F.max("timestamp").alias("finish_time"),
        F.min(F.when(F.col("has_started"), F.col("timestamp"))).alias("start_time")
    ).withColumn(
        "race_duration_microseconds",
        F.col("finish_time") - F.col("start_time")
    ).withColumn(
        "race_duration_seconds",
        F.col("race_duration_microseconds") / 1000000
    ).withColumn(
        "race_duration_hours",
        F.col("race_duration_seconds") / 3600
    ).withColumn(
        "rank",
        F.row_number().over(Window.orderBy("finish_time"))
    ).orderBy("rank")

    return finished


def get_schema_prefix(table_name):
    """Extract catalog.schema prefix from a fully-qualified table name (catalog.schema.table)."""
    return ".".join(table_name.split(".")[:2])


def categorize_wind(wind_speed_knots, config=None):
    """Categorize wind speed into Light/Moderate/Heavy.

    Thresholds come from config.toml [analysis] section if available,
    otherwise defaults to 8/15 knots.
    """
    if config and "analysis" in config:
        light = config["analysis"].get("wind_light_threshold_knots", 8)
        moderate = config["analysis"].get("wind_moderate_threshold_knots", 15)
    else:
        light = 8
        moderate = 15

    if wind_speed_knots < light:
        return "Light"
    elif wind_speed_knots < moderate:
        return "Moderate"
    return "Heavy"
