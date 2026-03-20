"""
Navigation and Geography Utility Functions

Provides Haversine distance and bearing calculations for maritime navigation.
All distances in nautical miles, all angles in degrees (0-360).
"""

import math


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
