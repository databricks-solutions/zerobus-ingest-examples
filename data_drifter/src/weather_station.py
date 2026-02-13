"""
Weather Station Module

Emits weather data to Zerobus REST API when weather conditions change.
Acts as a "weather station" that only reports when conditions differ significantly.
"""

import time
import uuid
import json
import logging
import requests
from typing import Dict, Optional
from weather import Weather

logger = logging.getLogger(__name__)


class WeatherStation:
    """Weather station that emits data via Zerobus REST API on weather changes"""

    def __init__(self, config: Dict, weather: Weather, client_id, client_secret):
        """
        Initialize weather station

        Args:
            config: Configuration dictionary with zerobus settings
            weather: Shared Weather instance to monitor
        """
        self.config = config
        self.weather = weather

        # OAuth and Zerobus configuration
        self.client_id = client_id
        self.client_secret = client_secret
        self.workspace_url = config["zerobus"]["workspace_url"]
        self.server_endpoint = config["zerobus"]["server_endpoint"]
        self.table_name = config["zerobus"].get(
            "weather_station_table_name",
            "main.sailboat.weather_station"
        )

        # Extract workspace ID from server endpoint
        # Format: 6051921418418893.zerobus.us-east-1.staging.cloud.databricks.com
        self.workspace_id = self.server_endpoint.split('.')[0]

        # OAuth token caching
        self.oauth_token = None
        self.token_expiry = 0

        # Weather change detection
        self.last_emitted_wind_speed = None
        self.last_emitted_wind_direction = None
        self.last_emitted_event_type = None

        # Thresholds for detecting significant changes
        self.wind_speed_threshold = 2.0  # knots
        self.wind_direction_threshold = 5.0  # degrees

        # Station metadata
        self.station_id = str(uuid.uuid4())  # Unique ID for this weather station instance
        self.station_name = "Race Course Weather Station"
        self.station_location = "Caribbean Sea"

    def _get_oauth_token(self) -> Optional[str]:
        """
        Fetch OAuth token for Zerobus REST API using client credentials

        Returns:
            OAuth token string, or None if fetch failed
        """
        # Check if cached token is still valid (with 60 second buffer)
        if self.oauth_token and time.time() < (self.token_expiry - 60):
            return self.oauth_token

        try:
            # Parse catalog, schema, table from table name
            parts = self.table_name.split('.')
            if len(parts) != 3:
                logger.error(f"Invalid table name format: {self.table_name}")
                return None

            catalog, schema, table = parts

            # Authorization details for Unity Catalog permissions
            authorization_details = [
                {
                    "type": "unity_catalog_privileges",
                    "privileges": ["USE CATALOG"],
                    "object_type": "CATALOG",
                    "object_full_path": catalog
                },
                {
                    "type": "unity_catalog_privileges",
                    "privileges": ["USE SCHEMA"],
                    "object_type": "SCHEMA",
                    "object_full_path": f"{catalog}.{schema}"
                },
                {
                    "type": "unity_catalog_privileges",
                    "privileges": ["SELECT", "MODIFY"],
                    "object_type": "TABLE",
                    "object_full_path": self.table_name
                }
            ]

            # Prepare OAuth request
            token_url = f"{self.workspace_url}/oidc/v1/token"
            form_data = {
                "grant_type": "client_credentials",
                "scope": "all-apis",
                "resource": f"api://databricks/workspaces/{self.workspace_id}/zerobusDirectWriteApi",
                "authorization_details": json.dumps(authorization_details)
            }

            # Make OAuth request
            response = requests.post(
                token_url,
                auth=(self.client_id, self.client_secret),
                data=form_data,
                timeout=30,
            )

            if response.status_code != 200:
                logger.error(f"OAuth token fetch failed: HTTP {response.status_code}")
                return None

            token_data = response.json()
            self.oauth_token = token_data.get('access_token')
            # Cache token for 50 minutes (tokens typically expire in 1 hour)
            self.token_expiry = time.time() + 3000

            logger.info("Weather station OAuth token fetched successfully")
            return self.oauth_token

        except Exception as e:
            logger.error(f"Failed to fetch OAuth token for weather station: {str(e)}")
            return None

    def _should_emit(self, conditions: Dict) -> bool:
        """
        Determine if weather conditions have changed enough to emit

        Args:
            conditions: Current weather conditions dict

        Returns:
            True if should emit, False otherwise
        """
        wind_speed = conditions["wind_speed"]
        wind_direction = conditions["wind_direction"]

        # Always emit first reading
        if self.last_emitted_wind_speed is None:
            return True

        # Check wind speed change
        speed_change = abs(wind_speed - self.last_emitted_wind_speed)
        if speed_change >= self.wind_speed_threshold:
            return True

        # Check wind direction change (accounting for 360° wrap)
        direction_change = abs(wind_direction - self.last_emitted_wind_direction)
        if direction_change > 180:
            direction_change = 360 - direction_change
        if direction_change >= self.wind_direction_threshold:
            return True

        # Check event type change
        detailed = self.weather.get_detailed_conditions()
        if detailed["event_type"] != self.last_emitted_event_type:
            return True

        return False

    def emit_weather(self, current_race_timestamp: float) -> bool:
        """
        Emit weather data to Zerobus REST API if conditions have changed

        Args:
            current_race_timestamp: Current race time as Unix timestamp (seconds since epoch)

        Returns:
            True if emission succeeded, False otherwise
        """
        # Get current weather conditions
        conditions = self.weather.get_conditions()
        detailed_conditions = self.weather.get_detailed_conditions()

        # Check if we should emit
        if not self._should_emit(conditions):
            return True  # Not an error, just no emission needed

        # Get OAuth token
        oauth_token = self._get_oauth_token()
        if not oauth_token:
            logger.error("Failed to get OAuth token for weather emission")
            return False

        try:
            # Prepare weather data record
            weather_data = {
                "station_id": self.station_id,
                "station_name": self.station_name,
                "station_location": self.station_location,
                "timestamp": int(current_race_timestamp * 1_000_000),  # microseconds
                "wind_speed_knots": round(conditions["wind_speed"], 2),
                "wind_direction_degrees": round(conditions["wind_direction"], 1),
                "event_type": detailed_conditions["event_type"],
                "in_transition": detailed_conditions["in_transition"],
                "time_in_state_seconds": round(detailed_conditions["time_in_state"], 1),
                "next_change_in_seconds": round(detailed_conditions["next_change_in"], 1)
            }

            # Prepare Zerobus REST API request
            ingest_url = f"https://{self.server_endpoint}/ingest-record?table_name={self.table_name}"

            # Make request
            response = requests.post(
                ingest_url,
                json=weather_data,
                headers={
                    "Authorization": f"Bearer {oauth_token}",
                    "unity-catalog-endpoint": self.workspace_url,
                    "x-databricks-zerobus-table-name": self.table_name
                },
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"Weather emission failed: HTTP {response.status_code} - {response.text}")
                return False

            # Update last emitted values
            self.last_emitted_wind_speed = conditions["wind_speed"]
            self.last_emitted_wind_direction = conditions["wind_direction"]
            self.last_emitted_event_type = detailed_conditions["event_type"]

            logger.info(
                f"Weather emitted: {conditions['wind_speed']:.1f} knots @ "
                f"{conditions['wind_direction']:.0f}° (event: {detailed_conditions['event_type']})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to emit weather data: {str(e)}")
            return False

    def get_station_info(self) -> Dict:
        """Get weather station metadata"""
        return {
            "station_id": self.station_id,
            "station_name": self.station_name,
            "station_location": self.station_location,
            "table_name": self.table_name,
            "last_emitted_wind_speed": self.last_emitted_wind_speed,
            "last_emitted_wind_direction": self.last_emitted_wind_direction,
            "last_emitted_event_type": self.last_emitted_event_type
        }
