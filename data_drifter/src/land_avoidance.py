"""
Land Avoidance System

Uses global-land-mask library for offline land/water detection.
Helps boats avoid running aground by maintaining safe distance from shore.
"""

import math
import logging
from typing import Optional, List
from enum import Enum
from dataclasses import dataclass
from global_land_mask import globe

logger = logging.getLogger(__name__)


class LandProximityZone(Enum):
    """Proximity zones for progressive land avoidance"""
    SAFE = "safe"           # > 2.5 nm from land
    WARNING = "warning"     # 1.5-2.5 nm - gentle corrections
    CAUTION = "caution"     # 1.0-1.5 nm - stronger corrections
    DANGER = "danger"       # 0.5-1.0 nm - emergency maneuvers
    CRITICAL = "critical"   # < 0.5 nm - allow reverse


@dataclass
class CourseCorrection:
    """Represents a course correction recommendation"""
    heading_change: float
    speed_penalty: float
    correction_type: str
    duration: int = 0


@dataclass
class PathEvaluation:
    """Results of lookahead path check"""
    is_safe: bool
    unsafe_at_step: Optional[int] = None
    path_nodes: Optional[List] = None


class LandAvoidanceConfig:
    """Configuration for land avoidance system"""
    warning_zone_nm = 2.5
    caution_zone_nm = 1.5
    danger_zone_nm = 1.0
    critical_zone_nm = 0.5
    heading_granularity = 15  # degrees
    lookahead_steps = 5
    max_reverse_time = 300  # seconds


class LandAvoidance:
    """
    Handles land detection and avoidance using global-land-mask library.

    Features:
    - Fast offline land/water detection
    - Distance estimation to nearest land
    - Safe heading calculation to avoid land
    """

    def __init__(self, min_distance_from_land_nm: float = 0.81):
        """
        Initialize land avoidance system

        Args:
            min_distance_from_land_nm: Minimum safe distance from land (nautical miles)
                                      Default: 0.81 nm (~1.5 km)
        """
        self.min_distance_from_land_nm = min_distance_from_land_nm
        self.check_count = 0

    def is_on_land(self, lat: float, lon: float) -> bool:
        """
        Check if location is on land

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            True if on land, False if on water
        """
        self.check_count += 1
        return globe.is_land(lat, lon)

    def is_on_ocean(self, lat: float, lon: float) -> bool:
        """
        Check if location is on ocean/water

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            True if on water, False if on land
        """
        self.check_count += 1
        return globe.is_ocean(lat, lon)

    def estimate_distance_to_land(self, lat: float, lon: float) -> float:
        """
        Estimate approximate distance to nearest land

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            Distance to land in nautical miles (0 if on land, > 0 if on water)
        """
        # Check if current location is on land
        if self.is_on_land(lat, lon):
            return 0.0

        # We're on water - estimate distance by checking expanding circles
        # Check points at different radii in 8 directions
        min_distance_to_land = float('inf')

        # Check at increasing radii: 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0 nm
        for radius_nm in [0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0]:
            found_land = False
            radius_degrees = radius_nm / 60  # Convert nm to degrees

            # Check 8 directions (N, NE, E, SE, S, SW, W, NW)
            for bearing_deg in [0, 45, 90, 135, 180, 225, 270, 315]:
                bearing_rad = math.radians(bearing_deg)

                # Calculate test point
                test_lat = lat + radius_degrees * math.cos(bearing_rad)
                test_lon = lon + radius_degrees * math.sin(bearing_rad) / math.cos(math.radians(lat))

                # Check if this point is land
                if self.is_on_land(test_lat, test_lon):
                    # Found land at this distance
                    min_distance_to_land = min(min_distance_to_land, radius_nm)
                    found_land = True
                    break  # No need to check other directions at this radius

            # If we found land at this radius, no need to check further
            if found_land:
                break

        # If no land found in search, return a large distance
        if min_distance_to_land == float('inf'):
            return 10.0  # Assume far from land (> 10 nm)

        return min_distance_to_land

    def is_too_close_to_land(self, lat: float, lon: float) -> bool:
        """
        Check if location is too close to land (within min_distance_from_land_nm)

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            True if too close to land or on land
        """
        # First check if we're on land
        if self.is_on_land(lat, lon):
            return True

        # Check if any nearby points are on land (within min distance)
        test_distance_degrees = self.min_distance_from_land_nm / 60  # Convert to degrees

        # Check 8 directions
        for bearing_deg in [0, 45, 90, 135, 180, 225, 270, 315]:
            bearing_rad = math.radians(bearing_deg)
            test_lat = lat + test_distance_degrees * math.cos(bearing_rad)
            test_lon = lon + test_distance_degrees * math.sin(bearing_rad) / math.cos(math.radians(lat))

            if self.is_on_land(test_lat, test_lon):
                return True

        return False

    def get_proximity_zone(self, lat: float, lon: float) -> LandProximityZone:
        """
        Determine which proximity zone boat is in

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            LandProximityZone indicating distance from land
        """
        distance = self.estimate_distance_to_land(lat, lon)

        if distance < LandAvoidanceConfig.critical_zone_nm:
            return LandProximityZone.CRITICAL
        elif distance < LandAvoidanceConfig.danger_zone_nm:
            return LandProximityZone.DANGER
        elif distance < LandAvoidanceConfig.caution_zone_nm:
            return LandProximityZone.CAUTION
        elif distance < LandAvoidanceConfig.warning_zone_nm:
            return LandProximityZone.WARNING
        else:
            return LandProximityZone.SAFE

    def get_safe_heading_away_from_land(self, lat: float, lon: float) -> Optional[float]:
        """
        Calculate heading that points away from land

        Args:
            lat: Current latitude
            lon: Current longitude

        Returns:
            Heading away from land (degrees, 0-360) or None if not near land
        """
        if not self.is_too_close_to_land(lat, lon):
            return None

        # Find direction with most water (away from land)
        best_heading = None
        max_water_distance = 0

        # Check 8 main compass directions
        for heading_deg in [0, 45, 90, 135, 180, 225, 270, 315]:
            # Check point in this direction (0.1 nm away)
            test_distance_degrees = 0.1 / 60  # 0.1 nm in degrees
            bearing_rad = math.radians(heading_deg)
            test_lat = lat + test_distance_degrees * math.cos(bearing_rad)
            test_lon = lon + test_distance_degrees * math.sin(bearing_rad) / math.cos(math.radians(lat))

            # If this direction leads to water, estimate how far
            if self.is_on_ocean(test_lat, test_lon):
                distance = self.estimate_distance_to_land(test_lat, test_lon)
                if distance > max_water_distance:
                    max_water_distance = distance
                    best_heading = heading_deg

        return best_heading

    def _project_position(self, lat: float, lon: float, heading: float, distance_nm: float) -> tuple:
        """
        Project position by distance in given heading

        Args:
            lat: Current latitude
            lon: Current longitude
            heading: Heading in degrees (0-360)
            distance_nm: Distance to project in nautical miles

        Returns:
            Tuple of (new_lat, new_lon)
        """
        distance_degrees = distance_nm / 60  # Convert nm to degrees
        bearing_rad = math.radians(heading)

        new_lat = lat + distance_degrees * math.cos(bearing_rad)
        new_lon = lon + distance_degrees * math.sin(bearing_rad) / math.cos(math.radians(lat))

        return new_lat, new_lon

    def _calculate_alignment_bonus(self, current_heading: float, test_heading: float) -> float:
        """
        Calculate bonus for headings close to current (minimize turn)

        Args:
            current_heading: Current boat heading (0-360)
            test_heading: Test heading to evaluate (0-360)

        Returns:
            Bonus value in nautical miles (0 to 0.5)
        """
        # Calculate angular difference (handles wraparound)
        diff = abs((test_heading - current_heading + 180) % 360 - 180)
        # Max 0.5 nm bonus for same heading, 0 for opposite heading
        return (180 - diff) / 180 * 0.5

    def get_safe_heading_fine_grained(
        self, lat: float, lon: float,
        current_heading: float, granularity: int = None
    ) -> Optional[float]:
        """
        Find best escape heading with fine granularity

        Args:
            lat: Current latitude
            lon: Current longitude
            current_heading: Current boat heading (0-360)
            granularity: Heading increment in degrees (default from config)

        Returns:
            Best escape heading (0-360) or None if all directions blocked
        """
        if granularity is None:
            granularity = LandAvoidanceConfig.heading_granularity

        # Generate test headings (0, 15, 30, 45... for 15° granularity)
        test_headings = range(0, 360, granularity)
        heading_scores = []

        for heading in test_headings:
            # Project 0.5 nm in this direction (look further ahead)
            test_lat, test_lon = self._project_position(lat, lon, heading, 0.5)

            if self.is_on_ocean(test_lat, test_lon):
                # Score = distance to land + alignment bonus
                distance = self.estimate_distance_to_land(test_lat, test_lon)
                alignment_bonus = self._calculate_alignment_bonus(current_heading, heading)
                score = distance + alignment_bonus
                heading_scores.append((heading, score))

        if not heading_scores:
            # Fine-grained search found nothing - try coarse 8-direction search as fallback
            for heading_deg in [0, 45, 90, 135, 180, 225, 270, 315]:
                test_lat, test_lon = self._project_position(lat, lon, heading_deg, 0.3)
                if self.is_on_ocean(test_lat, test_lon):
                    distance = self.estimate_distance_to_land(test_lat, test_lon)
                    alignment_bonus = self._calculate_alignment_bonus(current_heading, heading_deg)
                    score = distance + alignment_bonus
                    heading_scores.append((heading_deg, score))

            if not heading_scores:
                return None  # Truly blocked

        # Return heading with best score
        return max(heading_scores, key=lambda x: x[1])[0]

    def lookahead_path_check(
        self, lat: float, lon: float, heading: float,
        speed: float, time_step: float, steps: int = None
    ) -> PathEvaluation:
        """
        Check if current heading leads to safe water multiple steps ahead

        Args:
            lat: Current latitude
            lon: Current longitude
            heading: Current heading (0-360)
            speed: Current speed in knots
            time_step: Time step in seconds
            steps: Number of steps to look ahead (default from config)

        Returns:
            PathEvaluation with safety status
        """
        if steps is None:
            steps = LandAvoidanceConfig.lookahead_steps

        current_lat, current_lon = lat, lon

        for step in range(steps):
            # Calculate next position
            distance_nm = (speed / 3600) * time_step
            next_lat, next_lon = self._project_position(
                current_lat, current_lon, heading, distance_nm
            )

            # Check proximity zone at future position
            zone = self.get_proximity_zone(next_lat, next_lon)

            # If future position hits danger, path is unsafe
            if zone in [LandProximityZone.DANGER, LandProximityZone.CRITICAL]:
                return PathEvaluation(
                    is_safe=False,
                    unsafe_at_step=step
                )

            current_lat, current_lon = next_lat, next_lon

        return PathEvaluation(is_safe=True)

    def detect_dead_end(
        self, lat: float, lon: float, heading: float,
        speed: float, time_step: float
    ) -> bool:
        """
        Detect if heading into a dead end

        Args:
            lat: Current latitude
            lon: Current longitude
            heading: Current heading (0-360)
            speed: Current speed in knots
            time_step: Time step in seconds

        Returns:
            True if heading into dead end
        """
        # Test multiple headings in forward hemisphere
        test_headings = [heading - 60, heading - 30, heading, heading + 30, heading + 60]
        blocked_count = 0

        for test_heading in test_headings:
            path_eval = self.lookahead_path_check(
                lat, lon, test_heading % 360, speed, time_step
            )
            if not path_eval.is_safe:
                blocked_count += 1

        # If 4 out of 5 forward paths blocked, it's a dead end
        return blocked_count >= 4

    def _normalize_angle(self, angle: float) -> float:
        """
        Normalize angle to -180 to +180 range

        Args:
            angle: Angle in degrees

        Returns:
            Normalized angle in range [-180, 180]
        """
        return (angle + 180) % 360 - 180

    def _blend_headings(self, heading1: float, weight1: float, heading2: float, weight2: float) -> float:
        """
        Blend two headings with given weights (proper circular mean for angles)

        Args:
            heading1: First heading (0-360)
            weight1: Weight for first heading (0-1)
            heading2: Second heading (0-360)
            weight2: Weight for second heading (0-1)

        Returns:
            Blended heading (0-360)
        """
        # Convert to radians
        h1_rad = math.radians(heading1)
        h2_rad = math.radians(heading2)

        # Convert to unit vectors
        x1 = math.cos(h1_rad)
        y1 = math.sin(h1_rad)
        x2 = math.cos(h2_rad)
        y2 = math.sin(h2_rad)

        # Weighted average of vectors
        x_avg = weight1 * x1 + weight2 * x2
        y_avg = weight1 * y1 + weight2 * y2

        # Convert back to angle
        blended_rad = math.atan2(y_avg, x_avg)
        blended_deg = math.degrees(blended_rad)

        # Normalize to 0-360
        return blended_deg % 360

    def calculate_course_correction(
        self, lat: float, lon: float, current_heading: float,
        zone: LandProximityZone, destination_bearing: float
    ) -> CourseCorrection:
        """
        Calculate zone-appropriate course correction

        Args:
            lat: Current latitude
            lon: Current longitude
            current_heading: Current boat heading (0-360)
            zone: Current proximity zone
            destination_bearing: Bearing to destination (0-360)

        Returns:
            CourseCorrection with recommended heading change and speed penalty
        """
        if zone == LandProximityZone.SAFE:
            return CourseCorrection(
                heading_change=0, speed_penalty=1.0,
                correction_type="none"
            )

        # Find safest heading
        safe_heading = self.get_safe_heading_fine_grained(lat, lon, current_heading)

        if safe_heading is None:
            return CourseCorrection(
                heading_change=0, speed_penalty=0.0,
                correction_type="stuck"
            )

        if zone == LandProximityZone.WARNING:
            # Gentle: 30% safety, 70% destination
            blended = self._blend_headings(safe_heading, 0.3, destination_bearing, 0.7)
            correction = self._normalize_angle(blended - current_heading)
            return CourseCorrection(
                heading_change=correction * 0.40,  # Apply 40% per update - faster response
                speed_penalty=0.95,
                correction_type="gentle",
                duration=30
            )

        elif zone == LandProximityZone.CAUTION:
            # Moderate: 60% safety, 40% destination
            blended = self._blend_headings(safe_heading, 0.6, destination_bearing, 0.4)
            correction = self._normalize_angle(blended - current_heading)
            return CourseCorrection(
                heading_change=correction * 0.60,  # Apply 60% per update - urgent response
                speed_penalty=0.85,
                correction_type="moderate",
                duration=40
            )

        elif zone == LandProximityZone.DANGER:
            # Emergency: immediate full turn (current behavior)
            correction = self._normalize_angle(safe_heading - current_heading)
            return CourseCorrection(
                heading_change=correction,
                speed_penalty=0.65,
                correction_type="emergency",
                duration=48
            )

        else:  # CRITICAL
            return CourseCorrection(
                heading_change=0, speed_penalty=0.0,
                correction_type="critical_reverse_needed"
            )

    def find_reverse_heading(
        self, lat: float, lon: float, current_heading: float
    ) -> Optional[float]:
        """
        Find viable reverse heading when stuck

        Args:
            lat: Current latitude
            lon: Current longitude
            current_heading: Current heading (0-360)

        Returns:
            Reverse heading (0-360) or None if all reverse directions blocked
        """
        # Try straight back (180°)
        reverse_heading = (current_heading + 180) % 360
        reverse_lat, reverse_lon = self._project_position(lat, lon, reverse_heading, 0.1)

        if self.is_on_ocean(reverse_lat, reverse_lon):
            zone = self.get_proximity_zone(reverse_lat, reverse_lon)
            if zone in [LandProximityZone.SAFE, LandProximityZone.WARNING]:
                return reverse_heading

        # Try backing at angles (±30°, ±60°)
        for angle_offset in [-30, 30, -60, 60]:
            test_heading = (current_heading + 180 + angle_offset) % 360
            test_lat, test_lon = self._project_position(lat, lon, test_heading, 0.1)

            if self.is_on_ocean(test_lat, test_lon):
                zone = self.get_proximity_zone(test_lat, test_lon)
                if zone in [LandProximityZone.SAFE, LandProximityZone.WARNING]:
                    return test_heading

        # All reverse directions blocked
        return None

    def get_stats(self) -> dict:
        """Get statistics about land checks performed"""
        return {
            "land_checks_performed": self.check_count,
            "min_safe_distance_nm": self.min_distance_from_land_nm
        }
