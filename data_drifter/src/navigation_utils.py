"""
Navigation and Geography Utility Functions

Provides Haversine distance and bearing calculations for maritime navigation.
All distances in nautical miles, all angles in degrees (0-360).
"""

import math
from enum import Enum
from typing import NamedTuple


class PointOfSail(Enum):
    """
    Point of sail categories describing boat position relative to wind.

    The point of sail determines how a sailboat should be trimmed and sailed:
    - IN_IRONS: Too close to wind (<35°), boat cannot sail effectively
    - CLOSE_HAULED: 35-50°, sailing as close to wind as possible
    - CLOSE_REACH: 50-70°, sailing slightly off the wind
    - BEAM_REACH: 70-110°, sailing perpendicular to wind (fastest point)
    - BROAD_REACH: 110-150°, sailing downwind at an angle
    - RUNNING: >150°, sailing directly downwind
    """
    IN_IRONS = "in_irons"
    CLOSE_HAULED = "close_hauled"
    CLOSE_REACH = "close_reach"
    BEAM_REACH = "beam_reach"
    BROAD_REACH = "broad_reach"
    RUNNING = "running"


class SailingMode(Enum):
    """
    Sailing mode relative to wind direction and destination.

    Determines optimal sailing strategy:
    - UPWIND: Destination <70° from wind, requires tacking at 45° angles
    - DOWNWIND: Destination >110° from wind, requires jibing at 160° angles
    - REACHING: Destination 70-110° from wind, can sail direct at 100° angles
    """
    UPWIND = "upwind"
    DOWNWIND = "downwind"
    REACHING = "reaching"


class DestinationClassification(NamedTuple):
    """
    Classification of destination relative to wind direction.

    Attributes:
        mode: Sailing mode (UPWIND, DOWNWIND, or REACHING)
        angle_to_wind: Angle between destination and wind (0-180 degrees)
    """
    mode: SailingMode
    angle_to_wind: float


EARTH_RADIUS_NM = 3440.065  # Earth radius in nautical miles


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two points using Haversine formula.

    Args:
        lat1: Latitude of point 1 (degrees)
        lon1: Longitude of point 1 (degrees)
        lat2: Latitude of point 2 (degrees)
        lon2: Longitude of point 2 (degrees)

    Returns:
        Distance in nautical miles

    Example:
        >>> distance = calculate_distance(16.981578, -61.798147, 16.955598, -61.755800)
        >>> print(f"{distance:.2f} nm")
        2.14 nm
    """
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    lon1_rad = math.radians(lon1)
    lon2_rad = math.radians(lon2)

    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))

    return c * EARTH_RADIUS_NM


def calculate_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate bearing from point 1 to point 2.

    Args:
        lat1: Latitude of point 1 (degrees)
        lon1: Longitude of point 1 (degrees)
        lat2: Latitude of point 2 (degrees)
        lon2: Longitude of point 2 (degrees)

    Returns:
        Bearing in degrees (0-360), where 0° is North, 90° is East, etc.

    Example:
        >>> bearing = calculate_bearing(16.981578, -61.798147, 16.955598, -61.755800)
        >>> print(f"{bearing:.1f}°")
        125.3°
    """
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    lon1_rad = math.radians(lon1)
    lon2_rad = math.radians(lon2)

    # Calculate bearing
    dlon = lon2_rad - lon1_rad
    x = math.sin(dlon) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon)
    bearing = math.atan2(x, y)

    # Convert to degrees and normalize to 0-360
    bearing_deg = math.degrees(bearing)
    bearing_deg = (bearing_deg + 360) % 360

    return bearing_deg


def normalize_angle_difference(angle1: float, angle2: float) -> float:
    """
    Calculate the smallest angle difference between two bearings.

    Handles wraparound at 0°/360° boundary to always return the acute angle.

    Args:
        angle1: First bearing in degrees (0-360)
        angle2: Second bearing in degrees (0-360)

    Returns:
        Smallest angle between the two bearings (0-180 degrees)

    Example:
        >>> normalize_angle_difference(10, 350)  # 20° difference, not 340°
        20.0
        >>> normalize_angle_difference(180, 0)
        180.0
    """
    diff = abs(angle1 - angle2) % 360
    if diff > 180:
        diff = 360 - diff
    return diff


def get_point_of_sail(wind_direction: float, boat_heading: float) -> PointOfSail:
    """
    Determine point of sail based on boat heading relative to wind.

    Point of sail categories describe how a sailboat is positioned relative
    to the wind direction:
    - IN_IRONS: Too close to wind (<35°), boat cannot sail effectively
    - CLOSE_HAULED: 35-50°, sailing as close to wind as possible
    - CLOSE_REACH: 50-70°, sailing slightly off the wind
    - BEAM_REACH: 70-110°, sailing perpendicular to wind
    - BROAD_REACH: 110-150°, sailing downwind at an angle
    - RUNNING: >150°, sailing directly downwind

    Args:
        wind_direction: Direction wind is coming FROM in degrees (0-360)
        boat_heading: Direction boat is pointing in degrees (0-360)

    Returns:
        PointOfSail enum value

    Example:
        >>> get_point_of_sail(270, 315)  # 45° to wind
        PointOfSail.CLOSE_HAULED
        >>> get_point_of_sail(270, 0)  # 90° to wind
        PointOfSail.BEAM_REACH
    """
    angle_to_wind = normalize_angle_difference(wind_direction, boat_heading)

    if angle_to_wind < 35:
        return PointOfSail.IN_IRONS
    elif angle_to_wind < 50:
        return PointOfSail.CLOSE_HAULED
    elif angle_to_wind < 70:
        return PointOfSail.CLOSE_REACH
    elif angle_to_wind < 110:
        return PointOfSail.BEAM_REACH
    elif angle_to_wind < 150:
        return PointOfSail.BROAD_REACH
    else:
        return PointOfSail.RUNNING


def classify_destination_relative_to_wind(
    destination_bearing: float,
    wind_direction: float
) -> DestinationClassification:
    """
    Classify whether a destination requires upwind or downwind sailing.

    Upwind: Destination <70° from wind direction (requires tacking)
    Downwind: Destination >110° from wind direction (requires jibing)
    Reaching: Destination 70-110° from wind (can sail direct course)

    Args:
        destination_bearing: Bearing to destination in degrees (0-360)
        wind_direction: Direction wind is coming FROM in degrees (0-360)

    Returns:
        DestinationClassification with:
        - mode: SailingMode enum (UPWIND, DOWNWIND, or REACHING)
        - angle_to_wind: Angle between destination and wind (0-180 degrees)

    Example:
        >>> result = classify_destination_relative_to_wind(280, 270)  # 10° from wind
        >>> result.mode
        SailingMode.UPWIND
        >>> result.angle_to_wind
        10.0
    """
    angle_to_wind = normalize_angle_difference(destination_bearing, wind_direction)

    if angle_to_wind < 70:
        mode = SailingMode.UPWIND
    elif angle_to_wind > 110:
        mode = SailingMode.DOWNWIND
    else:
        mode = SailingMode.REACHING

    return DestinationClassification(mode=mode, angle_to_wind=angle_to_wind)


def get_optimal_sailing_angles(
    wind_direction: float,
    sailing_mode: SailingMode
) -> tuple[float, float]:
    """
    Calculate optimal sailing headings for upwind/downwind/reaching.

    Returns the best headings for starboard and port tack/jibe based on
    sailing mode:
    - Upwind: Tack at 45° to wind (starboard: wind+45°, port: wind-45°)
    - Downwind: Jibe at 160° to wind (starboard: wind+160°, port: wind-160°)
    - Reaching: Sail at 100° to wind (starboard: wind+100°, port: wind-100°)

    Args:
        wind_direction: Direction wind is coming FROM in degrees (0-360)
        sailing_mode: SailingMode enum (UPWIND, DOWNWIND, or REACHING)

    Returns:
        Tuple of (starboard_heading, port_heading) in degrees (0-360)

    Example:
        >>> get_optimal_sailing_angles(270, SailingMode.UPWIND)  # Upwind in westerly
        (315.0, 225.0)  # 45° on either side
        >>> get_optimal_sailing_angles(270, SailingMode.DOWNWIND)  # Downwind
        (70.0, 110.0)  # 160° on either side (normalized)
    """
    if sailing_mode == SailingMode.UPWIND:
        # Tack at 45° to wind
        optimal_angle = 45
    elif sailing_mode == SailingMode.DOWNWIND:
        # Jibe at 160° to wind
        optimal_angle = 160
    else:  # SailingMode.REACHING
        # Reach at 100° to wind
        optimal_angle = 100

    starboard_heading = (wind_direction + optimal_angle) % 360
    port_heading = (wind_direction - optimal_angle) % 360

    return starboard_heading, port_heading
