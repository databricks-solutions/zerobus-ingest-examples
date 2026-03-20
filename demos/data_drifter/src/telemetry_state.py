"""
Boat State Data Structure

Represents complete boat state at a point in time.
Separates data from behavior for cleaner architecture.
"""

from dataclasses import dataclass, asdict
from typing import Optional
from navigation_utils import PointOfSail, SailingMode
from boat import Boat


@dataclass
class BoatState:
    """
    Complete boat state at a point in time.

    Immutable snapshot of boat position, motion, sailing parameters,
    and race status. Used for telemetry generation and state tracking.
    """

    # Boat Identity & Characteristics
    boat: Boat  # Contains identity, type, modifiers, racing strategy

    # Position (geographic coordinates)
    latitude: float
    longitude: float
    previous_lat: float
    previous_lon: float

    # Motion
    heading: float  # Direction boat is pointing (0-360°)
    speed_knots: float  # Speed over ground
    distance_traveled_nm: float  # Cumulative distance

    # Wind Conditions
    wind_speed: float  # True wind speed (knots)
    wind_direction: float  # True wind direction (0-360°, where wind comes FROM)
    apparent_wind_speed: float  # Wind speed experienced by boat
    apparent_wind_angle: float  # Angle of apparent wind relative to boat

    # Sailing Parameters
    heel_angle: float  # Boat tilt angle (degrees)
    point_of_sail: Optional[PointOfSail]  # Current point of sail classification
    vmg_knots: float  # Velocity Made Good toward destination
    current_sail_config: list[str]  # Active sails (e.g., ["main", "jib"])

    # Navigation
    destination_bearing: float  # Bearing to current destination (0-360°)
    distance_to_destination: float  # Distance to destination (nm)

    # Race State
    current_mark_index: int  # Index of next mark to round
    has_started: bool  # Crossed start line
    has_finished: bool  # Crossed finish line
    has_dnf: bool  # Did Not Finish
    dnf_reason: Optional[str]  # Reason for DNF if applicable

    # Performance & Maneuvers
    current_penalty_factor: float  # Speed penalty multiplier (0.4-1.0)
    current_penalty_duration: float  # Penalty duration remaining (seconds)
    last_maneuver_type: Optional[str]  # Type of last maneuver (tack/jibe/etc)
    time_since_last_tack: float  # Seconds since last tack/jibe

    # Weather Shift Tracking
    weather_shift_detected: bool  # Significant weather change detected
    weather_shift_reaction_delay: float  # Crew reaction time remaining (seconds)
    wind_shift_magnitude: float  # Magnitude of wind shift (degrees)

    # Land Avoidance
    land_distance_nm: float  # Distance to nearest land
    approaching_land: bool  # Getting close to land

    # Time Tracking
    race_time_seconds: float  # Elapsed race time

    def to_telemetry_dict(self, timestamp_microseconds: int) -> dict:
        """
        Convert boat state to telemetry dictionary for transmission.

        Args:
            timestamp_microseconds: Event timestamp in microseconds since epoch

        Returns:
            Dictionary with all telemetry fields in the expected schema
        """
        return {
            # Boat identification
            "boat_id": self.boat.get_boat_id(),
            "boat_name": self.boat.get_boat_name(),
            "boat_type": self.boat.get_boat_type(),

            # Timestamp
            "timestamp": timestamp_microseconds,

            # Position
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),

            # Motion
            "heading_degrees": round(self.heading, 1),
            "speed_over_ground_knots": round(self.speed_knots, 2),
            "distance_traveled_nm": round(self.distance_traveled_nm, 3),

            # Wind (true wind)
            "wind_speed_knots": round(self.wind_speed, 1),
            "wind_direction_degrees": round(self.wind_direction, 1),

            # Apparent wind (what the boat experiences)
            "apparent_wind_speed_knots": round(self.apparent_wind_speed, 1),
            "apparent_wind_angle_degrees": round(self.apparent_wind_angle, 1),

            # Sailing parameters
            "heel_angle_degrees": round(self.heel_angle, 1),
            "point_of_sail": self.point_of_sail.value if self.point_of_sail else "unknown",
            "vmg_knots": round(self.vmg_knots, 2),
            "current_sail_configuration": self.current_sail_config,

            # Navigation
            "destination_bearing_degrees": round(self.destination_bearing, 1),
            "distance_to_destination_nm": round(self.distance_to_destination, 3),

            # Race status
            "current_mark_index": self.current_mark_index,
            "has_started": self.has_started,
            "has_finished": self.has_finished,
            "has_dnf": self.has_dnf,
            "dnf_reason": self.dnf_reason,

            # Performance
            "current_penalty_factor": round(self.current_penalty_factor, 2),
            "current_penalty_duration": round(self.current_penalty_duration, 1),
            "last_maneuver_type": self.last_maneuver_type,
            "time_since_last_tack": round(self.time_since_last_tack, 1),

            # Weather shift info
            "weather_shift_detected": self.weather_shift_detected,
            "weather_shift_reaction_delay": round(self.weather_shift_reaction_delay, 1),
            "wind_shift_magnitude": round(self.wind_shift_magnitude, 1),

            # Land proximity
            "land_distance_nm": round(self.land_distance_nm, 2),
            "approaching_land": self.approaching_land,

            # Race time
            "race_time_seconds": round(self.race_time_seconds, 1)
        }

    def copy_with(self, **changes) -> 'BoatState':
        """
        Create a new BoatState with updated fields.

        Args:
            **changes: Fields to update

        Returns:
            New BoatState instance with changes applied

        Example:
            >>> new_state = state.copy_with(latitude=37.85, longitude=-122.35)
        """
        current_dict = asdict(self)
        current_dict.update(changes)
        return BoatState(**current_dict)
