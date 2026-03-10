#!/usr/bin/env python3
"""
Sailboat Telemetry to Zerobus Demo

Generates sailboat telemetry data for a fleet and sends it to Databricks Zerobus for ingestion.
"""

import os
import sys
import time
import asyncio
import argparse
import logging
from typing import Dict, Any
from datetime import datetime, timezone

# Handle TOML for different Python versions
try:
    import tomllib  # Python 3.11+
except ImportError:
    import tomli as tomllib  # Earlier versions

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from fleet import SailboatFleet
from weather_station import WeatherStation
from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from databricks import sql as dbsql
from databricks.sdk.core import Config, oauth_service_principal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.toml") -> dict:
    """Load configuration from TOML file"""
    try:
        with open(config_path, 'rb') as f:
            config = tomllib.load(f)
        logger.info(f"✓ Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"✗ Configuration file not found: {config_path}")
        raise
    except Exception as e:
        logger.error(f"✗ Failed to load configuration: {e}")
        raise


class RaceSimulator:
    """Manages the race simulation and telemetry streaming to Zerobus"""

    def __init__(self, fleet: SailboatFleet, stream: Any, config: Dict[str, Any], weather_station: WeatherStation = None):
        """
        Initialize the race simulator

        Args:
            fleet: SailboatFleet instance
            stream: Zerobus stream for sending telemetry
            config: Configuration dictionary
            weather_station: Optional WeatherStation instance for emitting weather data
        """
        self.fleet = fleet
        self.stream = stream
        self.config = config
        self.weather_station = weather_station

        # Extract configuration values
        race_start_str = config["telemetry"]["race_start_time"]
        self.race_start_time = datetime.fromisoformat(race_start_str.replace('Z', '+00:00'))
        self.race_duration = config["telemetry"]["race_duration_seconds"]
        self.real_time_duration = config["telemetry"]["real_time_duration_seconds"]
        self.emission_interval_race_time = config["telemetry"]["emission_interval_seconds"]
        self.stats_interval = config["telemetry"]["stats_interval_seconds"]

        # Calculate time acceleration and real-time intervals
        if self.race_duration > 0 and self.real_time_duration > 0:
            self.time_acceleration = self.race_duration / self.real_time_duration
            self.emission_interval_real_time = self.emission_interval_race_time / self.time_acceleration
        else:
            # No acceleration if race_duration or real_time_duration is 0
            self.time_acceleration = 1.0
            self.emission_interval_real_time = self.emission_interval_race_time

        # Speed control from control table
        self.speed_multiplier = 1.0
        self.last_control_check = 0
        self.control_check_interval = 5.0  # Check every 5 seconds of real time
        self.control_table = None
        self.sql_connection = None

        # Statistics tracking
        self.records_sent = 0
        self.records_failed = 0
        self.real_start_time = None
        self.last_stats_time = None
        self.elapsed_race_time = 0.0

    def _init_control_table(self, config, client_id, client_secret):
        """Initialize SQL connection for reading the speed control table"""
        try:
            schema_prefix = ".".join(config["zerobus"]["table_name"].split(".")[:2])
            self.control_table = f"{schema_prefix}.race_control"

            workspace_url = config["zerobus"]["workspace_url"]
            server_hostname = workspace_url.replace("https://", "").replace("http://", "")
            warehouse_id = config["warehouse"]["sql_warehouse_id"]

            def credential_provider():
                config_obj = Config(
                    host=workspace_url,
                    client_id=client_id,
                    client_secret=client_secret,
                )
                return oauth_service_principal(config_obj)

            self.sql_connection = dbsql.connect(
                server_hostname=server_hostname,
                http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                credentials_provider=credential_provider,
            )
            logger.info(f"✓ Connected to control table: {self.control_table}")
        except Exception as e:
            logger.warning(f"⚠️ Could not connect to control table: {e}")
            logger.warning("  Speed control from app will not be available")
            self.control_table = None

    def _check_speed_control(self):
        """Read speed multiplier from control table"""
        if not self.control_table or not self.sql_connection:
            return

        current_time = time.time()
        if current_time - self.last_control_check < self.control_check_interval:
            return

        self.last_control_check = current_time
        try:
            cursor = self.sql_connection.cursor()
            cursor.execute(f"SELECT speed_multiplier FROM {self.control_table} LIMIT 1")
            row = cursor.fetchone()
            cursor.close()
            if row and row[0] != self.speed_multiplier:
                old = self.speed_multiplier
                self.speed_multiplier = float(row[0])
                if self.speed_multiplier <= 0:
                    self.speed_multiplier = 1.0
                logger.info(f"⚡ Speed changed: {old:.1f}x → {self.speed_multiplier:.1f}x")
        except Exception as e:
            # Silently ignore - control table might not exist yet
            pass

    async def run(self):
        """Run the race simulation"""
        logger.info(f"\nStarting race simulation...\n")
        logger.info(f"Race starts at: {self.race_start_time.isoformat()}")
        logger.info(f"Time acceleration: {self.time_acceleration:.1f}x")
        logger.info(f"Emitting telemetry every {self.emission_interval_race_time}s of race time ({self.emission_interval_real_time:.3f}s real time)\n")

        self.real_start_time = time.time()
        self.last_stats_time = self.real_start_time

        try:
            while True:
                # Check if we've reached the race duration
                if self.race_duration > 0 and self.elapsed_race_time >= self.race_duration:
                    logger.info(f"\n✓ Race duration reached: {self.elapsed_race_time:.1f}s race time ({self.elapsed_race_time/3600:.2f} hours)")
                    break

                # Check if all boats have finished the race
                if self._all_boats_finished():
                    logger.info(f"\n✓ Race complete: All boats have finished! Race time: {self.elapsed_race_time:.1f}s ({self.elapsed_race_time/3600:.2f} hours)")
                    break

                # Calculate current race timestamp
                current_race_timestamp = self.race_start_time.timestamp() + self.elapsed_race_time

                # Generate telemetry data for all boats in the fleet with current race time
                fleet_telemetry = self.fleet.generate_fleet_telemetry(current_race_timestamp)

                # Send each boat's telemetry to Zerobus
                await self._send_telemetry(fleet_telemetry)

                # Emit weather data if weather station is configured
                if self.weather_station:
                    self.weather_station.emit_weather(current_race_timestamp)

                # Increment elapsed race time
                self.elapsed_race_time += self.emission_interval_race_time

                # Check if it's time to print stats
                current_real_time = time.time()
                if current_real_time - self.last_stats_time >= self.stats_interval:
                    self._print_stats()
                    self.last_stats_time = current_real_time

                # Check for speed control updates from the app
                self._check_speed_control()

                # Apply speed multiplier to sleep interval
                adjusted_interval = self.emission_interval_real_time / self.speed_multiplier
                await asyncio.sleep(adjusted_interval)

        except KeyboardInterrupt:
            logger.info("\n\nInterrupted by user")

        # Print final summary
        self._print_final_summary()

    async def _send_telemetry(self, fleet_telemetry: list):
        """Send telemetry data to Zerobus with retry logic"""
        max_retries = 3
        for telemetry in fleet_telemetry:
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    await self.stream.ingest_record(telemetry)
                    self.records_sent += 1
                    break
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        backoff = 2 ** attempt  # 1s, 2s, 4s
                        logger.warning(f"Failed to send record from {telemetry['boat_name']} (attempt {attempt + 1}/{max_retries + 1}), retrying in {backoff}s: {e}")
                        await asyncio.sleep(backoff)
                    else:
                        self.records_failed += 1
                        logger.error(f"✗ Failed to send record from {telemetry['boat_name']} after {max_retries + 1} attempts: {e}")
                        raise last_exception

    def _all_boats_finished(self) -> bool:
        """Check if all boats have finished the race"""
        return all(boat.has_finished for boat in self.fleet.boats)

    def _print_stats(self):
        """Print current simulation statistics"""
        elapsed_real_time = time.time() - self.real_start_time
        rate = self.records_sent / elapsed_real_time if elapsed_real_time > 0 else 0

        # Calculate current race date/time
        current_race_time = self.race_start_time.timestamp() + self.elapsed_race_time
        current_race_datetime = datetime.fromtimestamp(current_race_time, tz=timezone.utc)

        # Calculate race progress
        boats_not_started = sum(1 for boat in self.fleet.boats if not boat.has_started)
        boats_racing = sum(1 for boat in self.fleet.boats if boat.has_started and not boat.has_finished and not boat.has_dnf)
        boats_finished = sum(1 for boat in self.fleet.boats if boat.has_finished)
        boats_dnf = sum(1 for boat in self.fleet.boats if boat.has_dnf)

        logger.info("\n" + "-" * 60)
        logger.info("STATS")
        logger.info("-" * 60)
        logger.info(f"Real time elapsed: {elapsed_real_time:.1f}s")
        logger.info(f"Race time: {current_race_datetime.strftime('%Y-%m-%d %H:%M:%S')} UTC ({self.elapsed_race_time/3600:.2f} hours into race)")
        if self.race_duration > 0:
            progress_pct = (self.elapsed_race_time / self.race_duration) * 100
            logger.info(f"Race progress: {progress_pct:.1f}%")
        logger.info(f"Active boats: {self.fleet.get_boat_count()}")
        logger.info(f"Race status: {boats_not_started} not started | {boats_racing} racing | {boats_finished} finished | {boats_dnf} DNF")

        # Mark progress (if marks to round exist)
        if self.fleet.boats and self.fleet.boats[0].marks_to_round:
            marks_completed = sum(1 for boat in self.fleet.boats for rounded in boat.marks_rounded if rounded)
            total_possible = len(self.fleet.boats) * len(self.fleet.boats[0].marks_to_round)
            logger.info(f"Marks rounded: {marks_completed}/{total_possible}")

        logger.info(f"Records sent: {self.records_sent}")
        logger.info(f"Records failed: {self.records_failed}")
        logger.info(f"Success rate: {(self.records_sent/(self.records_sent+self.records_failed)*100) if (self.records_sent+self.records_failed) > 0 else 0:.1f}%")
        speed_str = f" | Speed: {self.speed_multiplier:.1f}x" if self.speed_multiplier != 1.0 else ""
        logger.info(f"Throughput: {rate:.2f} records/sec (real time){speed_str}")
        logger.info("-" * 60 + "\n")

    def _print_final_summary(self):
        """Print final simulation summary"""
        elapsed_real_time = time.time() - self.real_start_time
        rate = self.records_sent / elapsed_real_time if elapsed_real_time > 0 else 0

        # Calculate final race date/time
        final_race_time = self.race_start_time.timestamp() + self.elapsed_race_time
        final_race_datetime = datetime.fromtimestamp(final_race_time, tz=timezone.utc)

        # Calculate final race status
        boats_not_started = sum(1 for boat in self.fleet.boats if not boat.has_started)
        boats_racing = sum(1 for boat in self.fleet.boats if boat.has_started and not boat.has_finished and not boat.has_dnf)
        boats_finished = sum(1 for boat in self.fleet.boats if boat.has_finished)
        boats_dnf = sum(1 for boat in self.fleet.boats if boat.has_dnf)

        logger.info("\n" + "=" * 60)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Real time elapsed: {elapsed_real_time:.1f}s ({elapsed_real_time/60:.1f} minutes)")
        logger.info(f"Race started: {self.race_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        logger.info(f"Race ended: {final_race_datetime.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        logger.info(f"Race duration: {self.elapsed_race_time:.1f}s ({self.elapsed_race_time/3600:.2f} hours)")
        logger.info(f"Time acceleration: {self.time_acceleration:.1f}x")
        logger.info(f"Boats in fleet: {self.fleet.get_boat_count()}")
        logger.info(f"Final race status: {boats_not_started} not started | {boats_racing} racing | {boats_finished} finished | {boats_dnf} DNF")

        # Show DNF boats if any
        if boats_dnf > 0:
            logger.info("\nDNF boats (Did Not Finish):")
            dnf_boats = [(boat.boat_name, boat.boat_id, boat.dnf_reason)
                         for boat in self.fleet.boats if boat.has_dnf]
            for i, (name, boat_id, reason) in enumerate(dnf_boats, 1):
                logger.info(f"  {i}. {name} ({boat_id}) - {reason}")

        # Show finished boats if any
        if boats_finished > 0:
            logger.info("\nFinished boats:")
            finished_boats = [(boat.boat_name, boat.boat_id, boat.distance_traveled_nm)
                             for boat in self.fleet.boats if boat.has_finished]
            # Sort by distance traveled (shorter is better)
            finished_boats.sort(key=lambda x: x[2])
            for i, (name, boat_id, distance) in enumerate(finished_boats, 1):
                logger.info(f"  {i}. {name} ({boat_id}) - {distance:.2f} nm traveled")

        logger.info(f"\nTelemetry records successfully sent: {self.records_sent}")
        logger.info(f"Telemetry records failed: {self.records_failed}")
        logger.info(f"Success rate: {(self.records_sent/(self.records_sent+self.records_failed)*100) if (self.records_sent+self.records_failed) > 0 else 0:.1f}%")
        logger.info(f"Average throughput: {rate:.2f} records/sec (real time)")
        logger.info("=" * 60)


def parse_cli_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Sailboat Telemetry Generator - Streams telemetry data to Zerobus"
    )
    parser.add_argument(
        '--client-id',
        required=True,
        help='Zerobus OAuth client ID'
    )
    parser.add_argument(
        '--client-secret',
        required=True,
        help='Zerobus OAuth client secret'
    )
    return parser.parse_args()


async def main():
    """Main function to generate and send sailboat telemetry to Zerobus"""

    # Print ASCII art banner
    print("""
        ════════════════════════════════════════════════════════════

            🌊  D A T A   D R I F T E R   R E G A T T A  🌊

                    Real-Time Sailing Competition

                ⛵  Presented by Lakeflow Connect  ⛵
                  Made possible with Zerobus Ingest

        ════════════════════════════════════════════════════════════
""")

    # Parse CLI arguments
    cli_args = parse_cli_args()

    # Load configuration from TOML file (no longer contains secrets)
    config = load_config("config.toml")

    # Get credentials from CLI arguments only
    CLIENT_ID = cli_args.client_id
    CLIENT_SECRET = cli_args.client_secret

    # Extract configuration values from file
    SERVER_ENDPOINT = config["zerobus"]["server_endpoint"]
    WORKSPACE_URL = config["zerobus"]["workspace_url"]
    TABLE_NAME = config["zerobus"]["table_name"]
    NUM_BOATS = config["fleet"]["num_boats"]
    START_LAT = config["race_course"]["start_lat"]
    START_LON = config["race_course"]["start_lon"]
    LINE_LENGTH_NM = config["race_course"]["line_length_nm"]
    MARKS = config["race_course"].get("marks", [])
    MARK_ROUNDING_RADIUS = config["race_course"].get("mark_rounding_radius_nm", 0.05)
    BASE_WIND_DIRECTION = config["weather"]["base_wind_direction"]
    MIN_WIND_SPEED = config["weather"]["min_wind_speed"]
    MAX_WIND_SPEED = config["weather"]["max_wind_speed"]
    WIND_DIRECTION_VARIATION = config["weather"]["wind_direction_variation"]
    MIN_WEATHER_STABILITY = config["weather"]["min_weather_stability_seconds"]
    MAX_WEATHER_STABILITY = config["weather"]["max_weather_stability_seconds"]
    WIND_SPEED_CHANGE_RATE = config["weather"]["wind_speed_change_rate"]
    WIND_DIRECTION_CHANGE_RATE = config["weather"]["wind_direction_change_rate"]
    FRONTAL_PROBABILITY = config["weather"]["frontal_passage_probability"]
    GRADUAL_PROBABILITY = config["weather"]["gradual_shift_probability"]
    GUST_PROBABILITY = config["weather"]["sudden_gust_probability"]
    FRONTAL_DIRECTION_CHANGE_MIN = config["weather"]["frontal_direction_change_min"]
    FRONTAL_DIRECTION_CHANGE_MAX = config["weather"]["frontal_direction_change_max"]
    FRONTAL_SPEED_CHANGE_MIN = config["weather"]["frontal_speed_change_min"]
    FRONTAL_SPEED_CHANGE_MAX = config["weather"]["frontal_speed_change_max"]
    GRADUAL_DIRECTION_CHANGE_MIN = config["weather"]["gradual_direction_change_min"]
    GRADUAL_DIRECTION_CHANGE_MAX = config["weather"]["gradual_direction_change_max"]
    GRADUAL_SPEED_CHANGE_MIN = config["weather"]["gradual_speed_change_min"]
    GRADUAL_SPEED_CHANGE_MAX = config["weather"]["gradual_speed_change_max"]
    GUST_SPEED_INCREASE_MIN = config["weather"]["gust_speed_increase_min"]
    GUST_SPEED_INCREASE_MAX = config["weather"]["gust_speed_increase_max"]
    GUST_DURATION = config["weather"]["gust_duration_seconds"]

    # Telemetry and race timing configuration
    RACE_START_TIME = config["telemetry"]["race_start_time"]
    RACE_DURATION = config["telemetry"]["race_duration_seconds"]
    REAL_TIME_DURATION = config["telemetry"]["real_time_duration_seconds"]
    EMISSION_INTERVAL = config["telemetry"]["emission_interval_seconds"]
    STATS_INTERVAL = config["telemetry"]["stats_interval_seconds"]

    # Calculate time acceleration
    if RACE_DURATION > 0 and REAL_TIME_DURATION > 0:
        TIME_ACCELERATION = RACE_DURATION / REAL_TIME_DURATION
    else:
        TIME_ACCELERATION = 1.0

    # Print configuration
    logger.info("=" * 60)
    logger.info("Sailboat Telemetry to Zerobus Demo (Async)")
    logger.info("=" * 60)
    logger.info(f"Server Endpoint: {SERVER_ENDPOINT}")
    logger.info(f"Workspace URL: {WORKSPACE_URL}")
    logger.info(f"Table: {TABLE_NAME}")
    logger.info(f"Number of boats: {NUM_BOATS}")
    if MARKS:
        finish_lat, finish_lon = MARKS[-1]
        logger.info(f"Race Course: Start ({START_LAT}, {START_LON}) -> Finish ({finish_lat}, {finish_lon})")
        if len(MARKS) > 1:
            logger.info(f"  with {len(MARKS)-1} mark(s) to round")
    else:
        logger.info(f"Race Course: Start ({START_LAT}, {START_LON}) -> Finish (default)")
    logger.info(f"Weather: Wind from {BASE_WIND_DIRECTION}° ({MIN_WIND_SPEED}-{MAX_WIND_SPEED} knots), variation ±{WIND_DIRECTION_VARIATION}°")
    logger.info(f"  Stability: {MIN_WEATHER_STABILITY}-{MAX_WEATHER_STABILITY}s | Change rates: {WIND_SPEED_CHANGE_RATE} kt/s, {WIND_DIRECTION_CHANGE_RATE}°/s")
    logger.info(f"  Events: Frontal {FRONTAL_PROBABILITY:.0%} | Gradual {GRADUAL_PROBABILITY:.0%} | Gust {GUST_PROBABILITY:.0%}")
    logger.info(f"Race timing:")
    logger.info(f"  Start time: {RACE_START_TIME}")
    if RACE_DURATION > 0:
        logger.info(f"  Race duration: {RACE_DURATION}s ({RACE_DURATION/3600:.1f} hours / {RACE_DURATION/86400:.1f} days)")
        logger.info(f"  Playback duration: {REAL_TIME_DURATION}s ({REAL_TIME_DURATION/60:.1f} minutes)")
        logger.info(f"  Time acceleration: {TIME_ACCELERATION:.1f}x")
    else:
        logger.info(f"  Race duration: Unlimited (until all boats finish or Ctrl+C)")
    logger.info(f"  Telemetry interval: {EMISSION_INTERVAL}s of race time")
    logger.info(f"  Stats interval: {STATS_INTERVAL}s of real time")
    logger.info("=" * 60)

    # Initialize Zerobus SDK
    logger.info("\nInitializing Zerobus SDK...")
    try:
        sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
        logger.info("✓ Zerobus SDK initialized successfully")
    except Exception as e:
        logger.error(f"✗ Failed to initialize Zerobus SDK: {e}")
        return 1

    # Create stream with JSON record type
    logger.info("\nCreating Zerobus stream...")
    try:
        table_properties = TableProperties(TABLE_NAME)
        options = StreamConfigurationOptions(
            record_type=RecordType.JSON,
            max_inflight_records=10000
        )
        stream = await sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)
        logger.info(f"✓ Stream created successfully (ID: {stream.stream_id})")
    except Exception as e:
        logger.error(f"✗ Failed to create stream: {e}")
        return 1

    # Initialize sailboat fleet
    logger.info(f"\nInitializing fleet with {NUM_BOATS} boats...")
    logger.info(f"Start line: ({START_LAT}, {START_LON}), length: {LINE_LENGTH_NM} nm")

    # Log marks if present
    if MARKS:
        logger.info(f"Racing course with {len(MARKS)} total marks:")
        for i, mark in enumerate(MARKS, 1):
            if i == len(MARKS):
                logger.info(f"  Mark {i} (FINISH): ({mark[0]}, {mark[1]})")
            else:
                logger.info(f"  Mark {i}: ({mark[0]}, {mark[1]})")
    else:
        logger.info(f"Simple race: Start to default finish")

    fleet = SailboatFleet(
        num_boats=NUM_BOATS,
        start_lat=START_LAT,
        start_lon=START_LON,
        base_wind_direction=BASE_WIND_DIRECTION,
        min_wind_speed=MIN_WIND_SPEED,
        max_wind_speed=MAX_WIND_SPEED,
        wind_direction_variation=WIND_DIRECTION_VARIATION,
        simulated_time_step_seconds=EMISSION_INTERVAL,  # Use emission interval as time step
        line_length_nm=LINE_LENGTH_NM,
        marks=MARKS,
        mark_rounding_radius_nm=MARK_ROUNDING_RADIUS,
        min_weather_stability_seconds=MIN_WEATHER_STABILITY,
        max_weather_stability_seconds=MAX_WEATHER_STABILITY,
        wind_speed_change_rate=WIND_SPEED_CHANGE_RATE,
        wind_direction_change_rate=WIND_DIRECTION_CHANGE_RATE,
        frontal_probability=FRONTAL_PROBABILITY,
        gradual_probability=GRADUAL_PROBABILITY,
        gust_probability=GUST_PROBABILITY,
        frontal_direction_change=(FRONTAL_DIRECTION_CHANGE_MIN, FRONTAL_DIRECTION_CHANGE_MAX),
        frontal_speed_change=(FRONTAL_SPEED_CHANGE_MIN, FRONTAL_SPEED_CHANGE_MAX),
        gradual_direction_change=(GRADUAL_DIRECTION_CHANGE_MIN, GRADUAL_DIRECTION_CHANGE_MAX),
        gradual_speed_change=(GRADUAL_SPEED_CHANGE_MIN, GRADUAL_SPEED_CHANGE_MAX),
        gust_speed_increase=(GUST_SPEED_INCREASE_MIN, GUST_SPEED_INCREASE_MAX),
        gust_duration=GUST_DURATION
    )
    logger.info(f"✓ Fleet initialized with {fleet.get_boat_count()} boats positioned behind start line (0-0.15 nm)")
    for i, boat in enumerate(fleet.boats, 1):
        logger.info(f"  Boat {i}: {boat.boat_name} ({boat.boat_id})")
        logger.info(f"          Type: {boat.boat_type} | Strategy: {boat.racing_strategy.strategy_type}")
        logger.info(f"          Crew Experience: {boat.crew_experience:.2f} (reaction time: {180 - boat.crew_experience*120:.0f}s base)")

    # Initialize weather station if configured
    weather_station = None
    if "weather_station_table_name" in config.get("zerobus", {}):
        logger.info("\n🌤️  Initializing weather station...")
        try:
            weather_station = WeatherStation(config, fleet.weather, CLIENT_ID, CLIENT_SECRET)
            station_info = weather_station.get_station_info()
            logger.info(f"✓ Weather station initialized")
            logger.info(f"  Station: {station_info['station_name']}")
            logger.info(f"  Location: {station_info['station_location']}")
            logger.info(f"  Table: {station_info['table_name']}")
            logger.info(f"  Emission thresholds: ±{weather_station.wind_speed_threshold} knots, ±{weather_station.wind_direction_threshold}°")
        except Exception as e:
            logger.warning(f"⚠️  Failed to initialize weather station: {e}")
            logger.warning("   Weather data will not be emitted (continuing with telemetry only)")
            weather_station = None
    else:
        logger.info("\n⚠️  Weather station table not configured (skipping weather data emission)")

    # Create and run the race simulator
    simulator = RaceSimulator(fleet, stream, config, weather_station)
    simulator._init_control_table(config, CLIENT_ID, CLIENT_SECRET)
    await simulator.run()

    # Close stream
    logger.info("\nClosing Zerobus stream...")
    try:
        await stream.close()
        logger.info("✓ Stream closed")
    except Exception as e:
        logger.error(f"✗ Error closing stream: {e}")

    if simulator.sql_connection:
        try:
            simulator.sql_connection.close()
        except:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
