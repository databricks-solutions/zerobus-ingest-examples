"""
Sailboat Telemetry Data Generator

Generates realistic sailboat telemetry data including:
- GPS coordinates (latitude, longitude)
- Speed over ground (SOG)
- Course over ground (COG)
- Wind speed and direction
- Boat heading
- Heel angle
- Boat name and identifier
"""

import math
import random
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any

from boat import Boat
from land_avoidance import LandAvoidance, LandProximityZone
from navigation_utils import (
    calculate_distance,
    calculate_bearing,
    normalize_angle_difference,
    classify_destination_relative_to_wind,
    get_optimal_sailing_angles,
    SailingMode,
    PointOfSail
)

logger = logging.getLogger(__name__)


class SailboatTelemetryGenerator:
    """Generates realistic sailboat telemetry data"""

    # Shared land avoidance instance (all boats share to minimize duplicate checks)
    _land_avoidance = None

    @classmethod
    def get_land_avoidance(cls) -> LandAvoidance:
        """Get or create shared land avoidance instance (1 nm / 1.85 km safety margin)"""
        if cls._land_avoidance is None:
            cls._land_avoidance = LandAvoidance(min_distance_from_land_nm=1.0)  # 1 nm (~1.85 km)
        return cls._land_avoidance

    def __init__(self, boat: Boat,
                 start_lat: float = 37.8, start_lon: float = -122.4,
                 base_wind_direction: float = 0,
                 min_wind_speed: float = 8, max_wind_speed: float = 18,
                 wind_direction_variation: float = 10,
                 simulated_time_step_seconds: float = 1.0,
                 line_length_nm: float = 0.1,
                 marks: list = None,
                 mark_rounding_radius_nm: float = 0.05):
        """
        Initialize the telemetry generator

        Args:
            boat: Boat instance with identity and racing strategy
            start_lat: Starting latitude for race course (center of start line)
            start_lon: Starting longitude for race course (center of start line)
            base_wind_direction: Base wind direction in degrees (where wind comes FROM)
            min_wind_speed: Minimum wind speed for the race (knots)
            max_wind_speed: Maximum wind speed for the race (knots)
            wind_direction_variation: Wind direction variation in degrees (+/-)
            simulated_time_step_seconds: How many seconds of race time pass per update
            line_length_nm: Length of start/finish lines in nautical miles
            marks: List of marks - last mark is finish line [[lat1, lon1], [lat2, lon2], ...]
            mark_rounding_radius_nm: Radius of mark rounding zone in nautical miles
        """
        # Boat identity, type, and racing strategy
        self.boat = boat
        self.boat_id = boat.get_boat_id()
        self.boat_name = boat.get_boat_name()
        self.boat_type = boat.get_boat_type()
        self.modifiers = boat.get_modifiers()
        self.racing_strategy = boat.get_racing_strategy()
        self.crew_experience = boat.get_crew_experience()

        # Land avoidance
        self.land_avoidance = self.get_land_avoidance()
        self.land_distance_nm = 10.0  # Initialize with large distance
        self.approaching_land = False

        # Race course coordinates
        self.start_lat = start_lat
        self.start_lon = start_lon
        self.line_length_nm = line_length_nm

        # Mark configuration and tracking
        # Last mark is the finish line, all others must be rounded
        self.marks = marks if marks else []
        if not self.marks:
            # Default: simple race with finish line 0.1 degrees north of start
            self.marks = [[start_lat + 0.1, start_lon]]

        # Extract finish line position from last mark
        self.finish_lat = self.marks[-1][0]
        self.finish_lon = self.marks[-1][1]

        # Marks to round (all except the last one, which is the finish)
        self.marks_to_round = self.marks[:-1] if len(self.marks) > 1 else []

        self.mark_rounding_radius_nm = mark_rounding_radius_nm
        self.current_mark_index = 0  # Index of next mark to round
        self.marks_rounded = [False] * len(self.marks_to_round)

        # Time simulation parameter
        self.simulated_time_step_seconds = simulated_time_step_seconds

        # Calculate course bearing first (needed for line calculations)
        temp_lat = start_lat
        temp_lon = start_lon
        self.latitude = temp_lat
        self.longitude = temp_lon
        course_bearing = self._calculate_bearing_to_destination()

        # Calculate start line endpoints (perpendicular to course)
        # Start line is perpendicular to the course bearing
        perpendicular_bearing = (course_bearing + 90) % 360

        # Calculate line endpoints
        half_line_distance = line_length_nm / 2
        self.start_line_port, self.start_line_starboard = self._calculate_line_endpoints(
            start_lat, start_lon, perpendicular_bearing, half_line_distance
        )

        # Calculate finish line endpoints
        # Finish line is at the last mark, perpendicular to the approach
        # If there are marks to round, use bearing from last mark to finish
        # Otherwise, use the start-to-finish bearing
        if len(self.marks_to_round) > 0:
            # Calculate bearing from second-to-last mark to finish
            last_mark = self.marks_to_round[-1]
            temp_lat_for_finish = last_mark[0]
            temp_lon_for_finish = last_mark[1]
            self.latitude = temp_lat_for_finish
            self.longitude = temp_lon_for_finish
            finish_approach_bearing = self._calculate_bearing_to_destination()
            finish_perpendicular_bearing = (finish_approach_bearing + 90) % 360
        else:
            # No marks to round - use start to finish bearing
            finish_perpendicular_bearing = perpendicular_bearing

        self.finish_line_port, self.finish_line_starboard = self._calculate_line_endpoints(
            self.finish_lat, self.finish_lon, finish_perpendicular_bearing, half_line_distance
        )

        # Position boat randomly behind start line (within 0.15nm)
        # First, pick a random position along the start line
        random_position_along_line = random.uniform(0, 1)
        line_lat = self.start_line_port[0] + random_position_along_line * (self.start_line_starboard[0] - self.start_line_port[0])
        line_lon = self.start_line_port[1] + random_position_along_line * (self.start_line_starboard[1] - self.start_line_port[1])

        # Then, offset backward from start line by 0 to 0.15nm
        # "Behind" means opposite direction of course bearing
        distance_behind = random.uniform(0, 0.15)
        behind_bearing = (course_bearing + 180) % 360
        behind_bearing_rad = math.radians(behind_bearing)

        # Calculate offset (1 degree ≈ 60 nm)
        lat_offset = (distance_behind / 60) * math.cos(behind_bearing_rad)
        lon_offset = (distance_behind / 60) * math.sin(behind_bearing_rad) / math.cos(math.radians(line_lat))

        self.latitude = line_lat + lat_offset
        self.longitude = line_lon + lon_offset

        # Race status tracking
        self.has_started = True  # All boats start the race immediately
        self.has_finished = False
        self.has_dnf = False  # Did Not Finish - stuck at land
        self.dnf_reason = None
        self.race_start_time = None  # Will be set on first telemetry call  # Reason for DNF
        self.start_time = None
        self.finish_time = None
        self.previous_lat = self.latitude
        self.previous_lon = self.longitude

        # Land stuck tracking (for DNF detection)
        self.time_stuck_at_land = 0.0  # Seconds of race time stuck without safe heading
        self.max_stuck_time = 3600.0  # 1 hour of race time before DNF

        # Enhanced collision avoidance state
        self.current_proximity_zone = LandProximityZone.SAFE
        self.active_course_correction = None
        self.reverse_maneuver_active = False
        self.reverse_heading = None
        self.reverse_start_time = 0
        self.total_reverse_time = 0
        self.max_total_reverse_time = 300  # 5 minutes
        self.race_time_seconds = 0.0  # Race time tracking for reverse maneuvers

        # Calculate bearing to destination (current mark or finish)
        self.destination_bearing = self._calculate_bearing_to_destination()

        # Initialize wind conditions with base wind direction
        # Initial wind speed within configured range
        self.wind_speed = random.uniform(min_wind_speed, max_wind_speed)
        # Apply initial wind direction variation
        self.wind_direction = (base_wind_direction + random.uniform(-wind_direction_variation, wind_direction_variation)) % 360

        # Calculate initial heading for sailing toward destination
        # Determine if destination is upwind or downwind
        destination_classification = classify_destination_relative_to_wind(
            self.destination_bearing, self.wind_direction
        )

        if destination_classification.mode == SailingMode.DOWNWIND:
            # Downwind - choose best jibe angle (±160° from wind)
            starboard_jibe, port_jibe = get_optimal_sailing_angles(
                self.wind_direction, destination_classification.mode
            )

            starboard_angle_to_dest = normalize_angle_difference(self.destination_bearing, starboard_jibe)
            port_angle_to_dest = normalize_angle_difference(self.destination_bearing, port_jibe)

            # Choose jibe that points closer to destination
            if starboard_angle_to_dest < port_angle_to_dest:
                self.heading = starboard_jibe
            else:
                self.heading = port_jibe

            # Speed is moderate when running downwind, modified by boat type
            base_speed = random.uniform(5.0, 8.0)
            self.speed_knots = base_speed * self.modifiers.speed_multiplier * self.modifiers.downwind_efficiency
            # Heel angle is less when running downwind, affected by boat stability
            self.heel_angle = random.uniform(5, 15) * self.modifiers.stability

        elif destination_classification.mode == SailingMode.UPWIND:
            # Upwind - choose best tack (±45° from wind)
            starboard_tack, port_tack = get_optimal_sailing_angles(
                self.wind_direction, destination_classification.mode
            )

            starboard_angle_to_dest = normalize_angle_difference(self.destination_bearing, starboard_tack)
            port_angle_to_dest = normalize_angle_difference(self.destination_bearing, port_tack)

            # Choose tack that points closer to destination
            if starboard_angle_to_dest < port_angle_to_dest:
                self.heading = starboard_tack
            else:
                self.heading = port_tack

            # Speed is slower when sailing upwind, modified by boat type
            base_speed = random.uniform(4.0, 7.0)
            self.speed_knots = base_speed * self.modifiers.speed_multiplier * self.modifiers.upwind_efficiency
            # Heel angle is significant when beating upwind, affected by boat stability
            self.heel_angle = random.uniform(15, 25) * self.modifiers.stability

        else:
            # Reaching - sail directly toward destination (fastest point of sail)
            self.heading = self.destination_bearing
            # Speed is highest when reaching, modified by boat type
            base_speed = random.uniform(6.0, 9.0)
            self.speed_knots = base_speed * self.modifiers.speed_multiplier * self.modifiers.reaching_efficiency
            # Moderate heel angle when reaching, affected by boat stability
            self.heel_angle = random.uniform(10, 20) * self.modifiers.stability

        # Track distance traveled
        self.distance_traveled_nm = 0.0

        # Tacking management (time since last tack in seconds)
        self.time_since_last_tack = random.uniform(60, 300)  # Start with random time to stagger tacks

        # Variable penalty based on point of sail
        self.current_penalty_factor = 1.0     # Penalty multiplier (0.40-0.80 depending on maneuver)
        self.current_penalty_duration = 0     # Duration in seconds (35-65 depending on maneuver)
        self.last_maneuver_type = None        # Track type of last maneuver for debugging

        # Track distance to destination for VMG (Velocity Made Good) calculations
        self.distance_to_destination = self._calculate_distance_to_destination()
        self.distance_at_last_tack_check = self.distance_to_destination
        self.distance_samples = []  # Track recent distance samples for trend analysis
        self.vmg = 0.0  # VMG in knots (positive = closing, negative = opening)

        # Sail configuration (will be updated each telemetry cycle)
        self.current_sail_config = []

        # Weather shift detection and crew reaction tracking
        self.previous_wind_speed = self.wind_speed
        self.previous_wind_direction = self.wind_direction
        self.weather_shift_detected = False
        self.weather_shift_reaction_delay = 0.0  # Seconds remaining before crew reacts
        self.wind_shift_magnitude = 0.0  # For telemetry tracking

    def generate_telemetry(self, wind_speed: float, wind_direction: float,
                          current_race_timestamp: float) -> Dict[str, Any]:
        """
        Generate a single telemetry data point

        Args:
            wind_speed: Current wind speed in knots (from shared weather)
            wind_direction: Current wind direction in degrees (from shared weather)
            current_race_timestamp: Current race time as Unix timestamp (seconds since epoch)

        Returns:
            Dictionary containing sailboat telemetry data
        """
        # Calculate race time in seconds from start
        if self.race_start_time is None:
            # First telemetry call - record race start time
            self.race_start_time = current_race_timestamp
            self.race_time_seconds = 0.0
        else:
            # Calculate elapsed race time in seconds
            self.race_time_seconds = current_race_timestamp - self.race_start_time

        # Update wind conditions from shared weather
        self.wind_speed = wind_speed
        self.wind_direction = wind_direction

        # Store previous position for line crossing detection
        self.previous_lat = self.latitude
        self.previous_lon = self.longitude

        # Check sailing parameters (including land avoidance) BEFORE moving
        self._update_sailing_parameters()

        # Update position AFTER checking land and setting safe heading
        self._update_position()

        # Check for line crossings
        self._check_line_crossings()

        # Check for mark rounding
        self._check_mark_rounding()

        telemetry = {
            "boat_id": self.boat_id,
            "boat_name": self.boat_name,
            "boat_type": self.boat_type,
            "timestamp": int(current_race_timestamp * 1000000),  # Epoch microseconds (race time)

            # GPS data
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),

            # Speed and course
            "speed_over_ground_knots": round(self.speed_knots, 2),
            "course_over_ground_degrees": round(self.heading, 1),

            # Wind data
            "wind_speed_knots": round(self.wind_speed, 1),
            "wind_direction_degrees": round(self.wind_direction, 1),
            "apparent_wind_speed_knots": round(self._calculate_apparent_wind_speed(), 1),
            "apparent_wind_angle_degrees": round(self._calculate_apparent_wind_angle(), 1),

            # Boat attitude
            "heading_degrees": round(self.heading, 1),
            "heel_angle_degrees": round(self.heel_angle, 1),

            # Additional metadata
            "sail_configuration": self.current_sail_config,
            "autopilot_engaged": random.choice([True, False]),
            "water_temperature_celsius": round(random.uniform(12, 18), 1),
            "air_temperature_celsius": round(random.uniform(15, 25), 1),
            "battery_voltage": round(random.uniform(12.2, 14.4), 2),

            # Race statistics
            "distance_traveled_nm": round(self.distance_traveled_nm, 3),
            "distance_to_destination_nm": round(self.distance_to_destination, 3),
            "vmg_knots": round(self.vmg, 2),

            # Mark progress
            "current_mark_index": self.current_mark_index,
            "marks_rounded": sum(self.marks_rounded),
            "total_marks": len(self.marks_to_round),
            "distance_to_current_mark_nm": round(calculate_distance(
                self.latitude, self.longitude,
                self.marks_to_round[self.current_mark_index][0],
                self.marks_to_round[self.current_mark_index][1]
            ), 3) if self.current_mark_index < len(self.marks_to_round) else 0.0,

            # Race status
            "has_started": self.has_started,
            "has_finished": self.has_finished,
            "race_status": "dnf" if self.has_dnf else ("finished" if self.has_finished else ("racing" if self.has_started else "not_started"))
        }

        return telemetry

    def _update_position(self):
        """Update GPS position based on speed and heading"""
        # ═══════════════════════════════════════════════════════════════════════
        # REVERSE MANEUVER HANDLING
        # ═══════════════════════════════════════════════════════════════════════
        # Check if in reverse maneuver
        if self.reverse_maneuver_active:
            elapsed = self.race_time_seconds - self.reverse_start_time

            # Check timeout (5 minutes)
            if elapsed >= self.max_total_reverse_time:
                self.has_dnf = True
                self.dnf_reason = "Reverse maneuver failed after 5 minutes"
                self.reverse_maneuver_active = False
                logger.warning(f"{self.boat_name} ({self.boat_id}) DNF: {self.dnf_reason}")
                return

            # Check if backed into safe water
            zone = self.land_avoidance.get_proximity_zone(self.latitude, self.longitude)
            if zone in [LandProximityZone.SAFE, LandProximityZone.WARNING]:
                # Try to find forward heading
                safe_heading = self.land_avoidance.get_safe_heading_fine_grained(
                    self.latitude, self.longitude, self.heading
                )
                if safe_heading is not None:
                    # Resume forward navigation
                    self.heading = safe_heading
                    self.reverse_maneuver_active = False
                    self.total_reverse_time += elapsed
                    logger.info(f"{self.boat_name} completed reverse maneuver after {elapsed:.1f}s")
                    # Continue to normal position update below
                # else: Keep reversing - movement happens below with reverse heading/speed

        # Rough conversion: 1 knot = 1.852 km/h
        # Calculate distance traveled in this time step
        distance_nm = (self.speed_knots / 3600) * self.simulated_time_step_seconds  # nautical miles per time step

        # Convert heading to radians
        heading_rad = math.radians(self.heading)

        # Calculate where boat WILL BE after this move (predictive check)
        # 1 degree latitude ≈ 60 nautical miles
        lat_change = (distance_nm / 60) * math.cos(heading_rad)
        lon_change = (distance_nm / 60) * math.sin(heading_rad) / math.cos(math.radians(self.latitude))

        next_lat = self.latitude + lat_change
        next_lon = self.longitude + lon_change

        # Track cumulative distance traveled
        self.distance_traveled_nm += distance_nm

        # Update position (either with original or safe heading)
        self.latitude = next_lat
        self.longitude = next_lon

    def _update_sailing_parameters(self):
        """Simulate gradual changes in sailing parameters for upwind racing"""
        # ═══════════════════════════════════════════════════════════════════════
        # WEATHER SHIFT DETECTION AND CREW REACTION DELAY
        # ═══════════════════════════════════════════════════════════════════════
        # NOTE: This delay ONLY affects normal tactical tacking decisions
        # It does NOT affect emergency land avoidance (which bypasses all delays)
        # ═══════════════════════════════════════════════════════════════════════

        # Detect weather shifts and apply crew reaction delay
        direction_change = normalize_angle_difference(self.wind_direction, self.previous_wind_direction)
        speed_change = abs(self.wind_speed - self.previous_wind_speed)

        # Detect significant weather shift (direction > 5° or speed > 2 knots)
        if (direction_change > 5 or speed_change > 2) and not self.weather_shift_detected:
            self.weather_shift_detected = True
            self.wind_shift_magnitude = max(direction_change, speed_change)

            # Calculate reaction delay based on crew experience
            # Base delay: 60-180 seconds depending on crew experience
            # High experience (1.0) = 60 seconds
            # Low experience (0.5) = 180 seconds
            base_delay = 180 - (self.crew_experience * 120)

            # Larger shifts require more time to react, even for experienced crews
            shift_multiplier = 1.0 + (self.wind_shift_magnitude / 40.0)  # Up to 1.5x for 20° shift

            self.weather_shift_reaction_delay = base_delay * shift_multiplier

        # Decrement reaction delay if active
        if self.weather_shift_reaction_delay > 0:
            self.weather_shift_reaction_delay -= self.simulated_time_step_seconds
            if self.weather_shift_reaction_delay <= 0:
                # Reaction delay complete - crew has now adapted to new wind
                self.weather_shift_reaction_delay = 0
                self.weather_shift_detected = False
                self.previous_wind_speed = self.wind_speed
                self.previous_wind_direction = self.wind_direction

        # Increment time since last tack based on simulated time step
        self.time_since_last_tack += self.simulated_time_step_seconds

        # Update destination bearing and distance
        self.destination_bearing = self._calculate_bearing_to_destination()
        prev_distance = self.distance_to_destination
        self.distance_to_destination = self._calculate_distance_to_destination()

        # Calculate VMG (Velocity Made Good) - positive = closing, negative = opening
        # VMG is the rate of distance change toward destination
        self.vmg = (prev_distance - self.distance_to_destination) / (self.simulated_time_step_seconds / 3600.0)
        # vmg is now in nautical miles per hour (knots)
        # Positive VMG = getting closer, Negative VMG = moving away

        # Track distance samples for VMG analysis (keep last 60 samples)
        # This represents 60 * simulated_time_step_seconds of race time
        self.distance_samples.append(self.distance_to_destination)
        if len(self.distance_samples) > 60:
            self.distance_samples.pop(0)

        # ═══════════════════════════════════════════════════════════════════════
        # EMERGENCY 0: LAND AVOIDANCE CHECK (ABSOLUTE HIGHEST PRIORITY)
        # ═══════════════════════════════════════════════════════════════════════
        # Enhanced multi-zone collision avoidance system
        # This check overrides ALL other navigation logic including:
        # - Crew experience delays
        # - Weather shift reactions
        # - Racing strategy decisions
        # - Tacking thresholds
        # ALL boats, regardless of crew experience, react IMMEDIATELY to land threats
        # Progressive zones: SAFE > WARNING > CAUTION > DANGER > CRITICAL
        # If stuck for 1 hour of race time, boat is marked DNF (or 5 min if reverse fails)
        # ═══════════════════════════════════════════════════════════════════════

        # Determine current zone
        zone = self.land_avoidance.get_proximity_zone(self.latitude, self.longitude)
        self.current_proximity_zone = zone

        if zone != LandProximityZone.SAFE:
            self.approaching_land = True

            # Calculate appropriate correction
            correction = self.land_avoidance.calculate_course_correction(
                self.latitude, self.longitude, self.heading,
                zone, self.destination_bearing
            )

            self.active_course_correction = correction

            if correction.correction_type == "stuck" or correction.correction_type == "critical_reverse_needed":
                # Initiate reverse maneuver
                reverse_heading = self.land_avoidance.find_reverse_heading(
                    self.latitude, self.longitude, self.heading
                )
                if reverse_heading is not None:
                    self.reverse_maneuver_active = True
                    self.reverse_heading = reverse_heading
                    self.reverse_start_time = self.race_time_seconds
                    self.heading = reverse_heading
                    self.speed_knots = 0.5  # Slow reverse speed
                    logger.info(f"{self.boat_name} initiating reverse maneuver at race time {self.race_time_seconds:.1f}s")
                    # DON'T RETURN - let position update happen
                else:
                    # Can't reverse either - stuck
                    self.time_stuck_at_land += self.simulated_time_step_seconds
                    if self.time_stuck_at_land >= self.max_stuck_time and not self.has_dnf:
                        self.has_dnf = True
                        self.dnf_reason = "Stuck at land, no escape route"
                        logger.warning(f"{self.boat_name} ({self.boat_id}) DNF: {self.dnf_reason}")
                    return

            elif correction.correction_type != "none":
                # Apply course correction
                self.heading = (self.heading + correction.heading_change) % 360
                self.current_penalty_factor = correction.speed_penalty
                self.current_penalty_duration = correction.duration
                self.last_maneuver_type = f"land_avoidance_{correction.correction_type}"

                # Reset stuck timer if making progress
                if zone in [LandProximityZone.WARNING, LandProximityZone.CAUTION]:
                    self.time_stuck_at_land = 0.0

                # Update land distance for telemetry
                self.land_distance_nm = self.land_avoidance.estimate_distance_to_land(
                    self.latitude, self.longitude
                )

                # For DANGER/EMERGENCY, reset tack timer
                if zone == LandProximityZone.DANGER:
                    self.time_since_last_tack = 0  # Reset tack timer
                    # DON'T RETURN - let position update happen normally
        else:
            self.approaching_land = False
            self.active_course_correction = None
            # Reset stuck timer when safe
            self.time_stuck_at_land = 0.0

        # Update land distance periodically (every cycle)
        # Only estimate if we were recently close or every 10th check (to save CPU)
        if self.approaching_land or (self.time_since_last_tack % 600 < self.simulated_time_step_seconds):
            self.land_distance_nm = self.land_avoidance.estimate_distance_to_land(
                self.latitude, self.longitude
            )

        # Calculate angle between current heading and destination
        angle_to_dest = normalize_angle_difference(self.destination_bearing, self.heading)

        # Calculate current wind angle - CRITICAL for sailing physics
        current_wind_angle = (self.heading - self.wind_direction) % 360
        if current_wind_angle > 180:
            current_wind_angle = current_wind_angle - 360

        # Determine if destination is upwind or downwind
        destination_classification = classify_destination_relative_to_wind(
            self.destination_bearing, self.wind_direction
        )

        # Calculate optimal sailing angles based on whether destination is upwind or downwind
        if destination_classification.mode == SailingMode.DOWNWIND:
            # Downwind sailing - use broad reach/run angles (150-170° from wind)
            starboard_jibe_heading, port_jibe_heading = get_optimal_sailing_angles(
                self.wind_direction, destination_classification.mode
            )

            starboard_angle_to_dest = normalize_angle_difference(self.destination_bearing, starboard_jibe_heading)
            port_angle_to_dest = normalize_angle_difference(self.destination_bearing, port_jibe_heading)

            # Determine which jibe we're on (based on which side of wind)
            currently_on_starboard = current_wind_angle > 0

            # Better jibe is the one closer to destination
            on_better_tack = (currently_on_starboard and starboard_angle_to_dest < port_angle_to_dest) or \
                            (not currently_on_starboard and port_angle_to_dest < starboard_angle_to_dest)
        else:
            # Upwind or Reaching sailing - use close-hauled or reaching angles
            starboard_tack_heading, port_tack_heading = get_optimal_sailing_angles(
                self.wind_direction, destination_classification.mode
            )

            # Determine which tack we're currently on based on wind
            currently_on_starboard = current_wind_angle > 0

            # Calculate which tack gives better VMG (minimizes distance to destination)
            starboard_angle_to_dest = normalize_angle_difference(self.destination_bearing, starboard_tack_heading)
            port_angle_to_dest = normalize_angle_difference(self.destination_bearing, port_tack_heading)

            # Calculate VMG for each tack (smaller angle = better VMG toward destination)
            # The tack with smaller angle to destination will close distance faster
            on_better_tack = (currently_on_starboard and starboard_angle_to_dest < port_angle_to_dest) or \
                            (not currently_on_starboard and port_angle_to_dest < starboard_angle_to_dest)

        # VMG-based tacking logic to minimize total distance traveled
        should_tack = False

        # Calculate angle difference between tacks (how much better is the other tack?)
        tack_advantage = abs(starboard_angle_to_dest - port_angle_to_dest)

        # Get strategy-specific parameters from racing strategy
        min_tack_time = self.racing_strategy.get_min_tack_time()
        tack_adv_high, tack_adv_med, tack_adv_low = self.racing_strategy.get_tack_advantage_thresholds()
        vmg_strong, vmg_weak = self.racing_strategy.get_vmg_thresholds()
        prob_med, prob_low = self.racing_strategy.get_tack_probabilities()

        # ═══════════════════════════════════════════════════════════════════════
        # CREW EXPERIENCE EFFECT ON NORMAL TACTICAL TACKING
        # ═══════════════════════════════════════════════════════════════════════
        # NOTE: This ONLY affects normal tactical tacking decisions during racing
        # It does NOT affect:
        #   - Emergency land avoidance (bypasses all delays)
        #   - EMERGENCY 1/2 tacks below (negative VMG, in irons)
        # ═══════════════════════════════════════════════════════════════════════
        # Reduce tacking probability during weather shift reaction delay
        # Crew is still adapting to new wind conditions, so they're more conservative
        if self.weather_shift_reaction_delay > 0:
            reaction_factor = self.weather_shift_reaction_delay / 180.0  # 0.0-1.0
            prob_med *= (1.0 - 0.5 * reaction_factor)  # Reduce by up to 50%
            prob_low *= (1.0 - 0.7 * reaction_factor)  # Reduce by up to 70%
            tack_adv_high += 10 * reaction_factor  # Require 10° more advantage
            tack_adv_med += 5 * reaction_factor  # Require 5° more advantage

        # EMERGENCY 1: Negative VMG - moving away from destination!
        # This emergency check bypasses crew experience delays
        # This check is INDEPENDENT of which tack we think is better
        if self.vmg < vmg_strong and self.time_since_last_tack > 30:
            # Strongly negative VMG: actively moving away from destination
            # ALWAYS tack, regardless of theoretical tack advantage
            should_tack = True
        elif self.vmg < vmg_weak and self.time_since_last_tack > min_tack_time:
            # Any negative VMG after minimum tack time - tack
            should_tack = True

        # EMERGENCY 2: In irons (stalled)
        # This emergency check also bypasses crew experience delays
        if abs(current_wind_angle) < 35 and self.time_since_last_tack > 20:
            should_tack = True

        # ═══════════════════════════════════════════════════════════════════════
        # NORMAL TACTICAL TACKING DECISIONS (affected by crew experience)
        # ═══════════════════════════════════════════════════════════════════════
        # Everything below this line is subject to crew experience delays
        # The above EMERGENCY checks always execute immediately
        # ═══════════════════════════════════════════════════════════════════════

        # PRIORITY 1: Check if we're on the wrong tack (poor VMG)
        if not should_tack and not on_better_tack and self.time_since_last_tack > min_tack_time:
            # The other tack has better VMG toward destination
            if tack_advantage > tack_adv_high:
                # Significantly better VMG on other tack - tack deterministically
                should_tack = True
            elif tack_advantage > tack_adv_med:
                # Moderately better VMG - likely tack
                should_tack = random.random() < prob_med
            elif tack_advantage > tack_adv_low:
                # Slightly better VMG - consider tacking
                should_tack = random.random() < prob_low

        # PRIORITY 2: Check if we can lay the mark (don't overshoot)
        # Only applies if we DON'T have negative VMG
        if not should_tack:
            dest_to_wind_angle = normalize_angle_difference(self.destination_bearing, self.wind_direction)

            if angle_to_dest < 50 and dest_to_wind_angle > 50:
                # We can fetch the destination on this tack - maintain course
                # This is only relevant if we weren't already going to tack
                pass

        # Execute tack/jibe to the heading with better VMG (smaller angle to destination)
        if should_tack:
            # Determine point of sail and set appropriate penalty
            if destination_classification.mode == SailingMode.DOWNWIND:
                # JIBING - downwind maneuver (least costly)
                self.current_penalty_factor = 0.80  # 20% speed loss
                self.current_penalty_duration = 35  # 35 seconds
                self.last_maneuver_type = "jibe"

                # Execute jibe
                if starboard_angle_to_dest < port_angle_to_dest:
                    self.heading = starboard_jibe_heading
                else:
                    self.heading = port_jibe_heading
            elif destination_classification.mode == SailingMode.UPWIND:
                # TACKING - upwind maneuver (most costly)
                self.current_penalty_factor = 0.40  # 60% speed loss
                self.current_penalty_duration = 65  # 65 seconds
                self.last_maneuver_type = "tack"

                # Execute tack
                if starboard_angle_to_dest < port_angle_to_dest:
                    self.heading = starboard_tack_heading
                else:
                    self.heading = port_tack_heading
            else:
                # REACHING - beam reach maneuver (medium cost)
                self.current_penalty_factor = 0.65  # 35% speed loss
                self.current_penalty_duration = 48  # 48 seconds
                self.last_maneuver_type = "reach"

                # Execute reach tack (using beam angle ~100° from wind)
                starboard_reach_heading, port_reach_heading = get_optimal_sailing_angles(
                    self.wind_direction, SailingMode.REACHING
                )

                starboard_reach_to_dest = normalize_angle_difference(self.destination_bearing, starboard_reach_heading)
                port_reach_to_dest = normalize_angle_difference(self.destination_bearing, port_reach_heading)

                if starboard_reach_to_dest < port_reach_to_dest:
                    self.heading = starboard_reach_heading
                else:
                    self.heading = port_reach_heading

            # Reset tack timer and record distance
            self.time_since_last_tack = 0
            self.distance_at_last_tack_check = self.distance_to_destination

        # Gradually adjust heading based on destination direction
        # SKIP heading adjustments on the cycle we just tacked/jibed to avoid undoing it
        if not should_tack:
            # We already calculated destination_classification above

            if destination_classification.mode == SailingMode.DOWNWIND:
                # DOWNWIND SAILING - maintain broad reach/run angles
                # Can sail more directly toward destination when downwind
                if angle_to_dest < 30:
                    # Close enough to sail directly at destination
                    angle_diff = (self.destination_bearing - self.heading) % 360
                    if angle_diff > 180:
                        angle_diff = angle_diff - 360
                    self.heading += angle_diff * 0.1  # Turn toward destination
                else:
                    # Maintain broad reach angle (140-170° from wind)
                    if abs(current_wind_angle) < 140:
                        # Too close to wind for downwind - bear away
                        self.heading += random.uniform(2, 5) * (-1 if current_wind_angle >= 0 else 1)
                    elif abs(current_wind_angle) > 175:
                        # Dead run (directly downwind) - unstable, head up slightly
                        self.heading -= random.uniform(2, 5) * (-1 if current_wind_angle >= 0 else 1)
                    else:
                        # Fine tune - maintain good downwind angle
                        self.heading += random.uniform(-1, 1)

            elif destination_classification.mode == SailingMode.UPWIND:
                # UPWIND SAILING - maintain close-hauled angles
                # Boats must maintain 40-50° off wind to keep speed

                # Adjust heading to maintain proper wind angle
                if abs(current_wind_angle) < 38:
                    # Too close to wind (in irons), bear away IMMEDIATELY
                    self.heading += random.uniform(5, 10) * (1 if current_wind_angle >= 0 else -1)
                elif abs(current_wind_angle) > 55:
                    # Too far from wind (sailing too low), head up
                    self.heading -= random.uniform(2, 5) * (1 if current_wind_angle >= 0 else -1)
                else:
                    # Fine tune - maintain optimal upwind angle (40-50° off wind)
                    self.heading += random.uniform(-1, 1)

            else:
                # REACHING (destination is on the beam) - sail directly toward it
                # This is the fastest point of sail
                angle_diff = (self.destination_bearing - self.heading) % 360
                if angle_diff > 180:
                    angle_diff = angle_diff - 360
                self.heading += angle_diff * 0.1  # Turn toward destination

            self.heading = self.heading % 360
        else:
            # Just tacked/jibed - normalize heading but don't adjust
            self.heading = self.heading % 360

        # Speed varies based on wind angle and wind speed
        # Sailing physics: speed depends heavily on angle to wind

        # Generate sail configuration to determine sail-specific speed factors
        self.current_sail_config = self._generate_sail_configuration()
        has_spinnaker = any(sail in self.current_sail_config for sail in
                           ["spinnaker", "asymmetric_spinnaker", "gennaker", "code_zero"])

        # Calculate speed multiplier based on wind angle
        abs_wind_angle = abs(current_wind_angle)

        if abs_wind_angle < 35:
            # In irons - boat stalls (0-20% speed)
            speed_factor = 0.1 * (abs_wind_angle / 35)  # 0.0 at 0°, 0.1 at 35°
        elif abs_wind_angle < 45:
            # Close-hauled but not optimal (30-60% speed)
            speed_factor = 0.3 + 0.3 * ((abs_wind_angle - 35) / 10)
        elif abs_wind_angle < 65:
            # Optimal upwind angle (60-75% speed)
            speed_factor = 0.6 + 0.15 * ((abs_wind_angle - 45) / 20)
        elif abs_wind_angle < 90:
            # Close reach (75-90% speed) - fastest point of sail
            speed_factor = 0.75 + 0.15 * ((abs_wind_angle - 65) / 25)
        elif abs_wind_angle < 120:
            # Beam to broad reach (85-95% speed) - very fast
            speed_factor = 0.85 + 0.10 * ((abs_wind_angle - 90) / 30)
            # Bonus for spinnaker/gennaker on broad reach
            if has_spinnaker:
                speed_factor += 0.05
        elif abs_wind_angle < 150:
            # Broad reach (80-90% speed)
            speed_factor = 0.80 + 0.10 * ((abs_wind_angle - 120) / 30)
            # Significant bonus for spinnaker on broad reach
            if has_spinnaker:
                speed_factor += 0.10
        elif abs_wind_angle < 170:
            # Running (70-85% speed)
            speed_factor = 0.70 + 0.15 * ((abs_wind_angle - 150) / 20)
            # Large bonus for spinnaker when running
            if has_spinnaker:
                speed_factor += 0.15
        else:
            # Dead run (65-80% speed without spinnaker, 90-110% with spinnaker!)
            if has_spinnaker:
                # Spinnakers are VERY effective on dead runs
                # Can actually go faster than wind speed in some conditions
                speed_factor = 0.90 + 0.20 * ((180 - abs_wind_angle) / 10)
            else:
                # Without spinnaker - slower due to blanketing
                speed_factor = 0.65 + 0.15 * ((180 - abs_wind_angle) / 10)

        # Apply variable tack/jibe penalty based on point of sail and boat type
        is_in_tack_penalty = self.time_since_last_tack < self.current_penalty_duration
        if is_in_tack_penalty:
            # Base penalty modified by boat type (catamarans tack with less penalty)
            tack_penalty_factor = self.current_penalty_factor * self.modifiers.tacking_penalty_multiplier
            tack_penalty_factor = max(0.2, min(1.0, tack_penalty_factor))  # Keep in reasonable range
        else:
            tack_penalty_factor = 1.0  # No penalty

        # Apply wind condition modifiers (light vs heavy wind performance)
        if self.wind_speed < 8:
            wind_condition_multiplier = self.modifiers.light_wind_multiplier
        elif self.wind_speed > 15:
            wind_condition_multiplier = self.modifiers.heavy_wind_multiplier
        else:
            wind_condition_multiplier = 1.0  # Moderate wind

        # Calculate target speed based on wind speed, angle, tack penalty, and boat type
        target_speed = (self.wind_speed * speed_factor * tack_penalty_factor *
                       wind_condition_multiplier * random.uniform(0.9, 1.1))

        # Gradually adjust to target speed
        self.speed_knots += (target_speed - self.speed_knots) * 0.2
        # Apply boat-specific speed limits
        self.speed_knots = max(0.5, min(self.modifiers.max_speed_limit, self.speed_knots))

        # Wind conditions are now managed by shared Weather instance (not updated here)

        # Heel angle depends on wind angle and sail configuration
        # More wind = more heel, but also depends on wind angle
        apparent_wind_angle = abs(self._calculate_apparent_wind_angle())
        if apparent_wind_angle > 180:
            apparent_wind_angle = 360 - apparent_wind_angle

        if 30 <= apparent_wind_angle <= 60:
            # Close-hauled upwind - maximum heel, modified by boat stability
            self.heel_angle = (self.wind_speed / 1.5) * random.uniform(0.9, 1.1) * self.modifiers.stability
        elif apparent_wind_angle >= 150:
            # Running downwind - minimal heel, especially with spinnaker
            if has_spinnaker:
                # Spinnaker pulls boat forward, not sideways - very little heel
                self.heel_angle = (self.wind_speed / 5.0) * random.uniform(0.7, 1.0) * self.modifiers.stability
            else:
                # Still less heel than upwind
                self.heel_angle = (self.wind_speed / 3.5) * random.uniform(0.8, 1.0) * self.modifiers.stability
        else:
            # Reaching - moderate heel, modified by boat stability
            self.heel_angle = (self.wind_speed / 2.5) * random.uniform(0.8, 1.0) * self.modifiers.stability

        # Catamarans and more stable boats have lower heel angles
        max_heel = 35 * self.modifiers.stability
        self.heel_angle = max(2, min(max_heel, self.heel_angle))

    def _calculate_apparent_wind_speed(self) -> float:
        """Calculate apparent wind speed (simplified)"""
        # Simplified calculation - actual would use vector addition
        return self.wind_speed + (self.speed_knots * 0.5)

    def _calculate_apparent_wind_angle(self) -> float:
        """Calculate apparent wind angle relative to boat heading"""
        # Simplified calculation
        angle = (self.wind_direction - self.heading) % 360
        return angle

    def _get_current_destination(self) -> tuple:
        """Get current destination coordinates (next mark or finish)"""
        if self.current_mark_index < len(self.marks_to_round):
            # Next mark to round is the destination
            mark = self.marks_to_round[self.current_mark_index]
            return (mark[0], mark[1])
        else:
            # All marks rounded, head to finish line
            return (self.finish_lat, self.finish_lon)

    def _calculate_bearing_to_destination(self) -> float:
        """Calculate bearing from current position to current destination (mark or finish)"""
        dest_lat, dest_lon = self._get_current_destination()
        return calculate_bearing(self.latitude, self.longitude, dest_lat, dest_lon)

    def _calculate_distance_to_destination(self) -> float:
        """Calculate distance to current destination (mark or finish) in nautical miles"""
        dest_lat, dest_lon = self._get_current_destination()
        return calculate_distance(self.latitude, self.longitude, dest_lat, dest_lon)

    def _calculate_bearing_to_point(self, lat: float, lon: float) -> float:
        """Calculate bearing to a specific point"""
        return calculate_bearing(self.latitude, self.longitude, lat, lon)

    def _set_optimal_heading_for_destination(self):
        """
        Immediately set the boat's heading to the optimal angle for the current destination.
        Called after rounding a mark to ensure boat points toward new destination.
        """
        # Determine if destination is upwind or downwind
        destination_classification = classify_destination_relative_to_wind(
            self.destination_bearing, self.wind_direction
        )

        if destination_classification.mode == SailingMode.DOWNWIND:
            # Downwind sailing - use broad reach/run angles (160° from wind)
            starboard_jibe_heading, port_jibe_heading = get_optimal_sailing_angles(
                self.wind_direction, destination_classification.mode
            )

            # Calculate which jibe is closer to destination
            starboard_angle_to_dest = normalize_angle_difference(self.destination_bearing, starboard_jibe_heading)
            port_angle_to_dest = normalize_angle_difference(self.destination_bearing, port_jibe_heading)

            # Set heading to the jibe closer to destination
            if starboard_angle_to_dest < port_angle_to_dest:
                self.heading = starboard_jibe_heading
            else:
                self.heading = port_jibe_heading
        elif destination_classification.mode == SailingMode.UPWIND:
            # Upwind sailing - use close-hauled angles (45° from wind)
            starboard_tack_heading, port_tack_heading = get_optimal_sailing_angles(
                self.wind_direction, destination_classification.mode
            )

            # Calculate which tack is closer to destination
            starboard_angle_to_dest = normalize_angle_difference(self.destination_bearing, starboard_tack_heading)
            port_angle_to_dest = normalize_angle_difference(self.destination_bearing, port_tack_heading)

            # Set heading to the tack closer to destination
            if starboard_angle_to_dest < port_angle_to_dest:
                self.heading = starboard_tack_heading
            else:
                self.heading = port_tack_heading
        else:
            # Beam reach - can sail more directly toward destination
            # Use a broad angle (100° from wind) for beam reaching
            starboard_reach_heading, port_reach_heading = get_optimal_sailing_angles(
                self.wind_direction, destination_classification.mode  # Will be SailingMode.REACHING
            )

            starboard_angle_to_dest = normalize_angle_difference(self.destination_bearing, starboard_reach_heading)
            port_angle_to_dest = normalize_angle_difference(self.destination_bearing, port_reach_heading)

            if starboard_angle_to_dest < port_angle_to_dest:
                self.heading = starboard_reach_heading
            else:
                self.heading = port_reach_heading

    def _rounded_port_side(self, mark_lat: float, mark_lon: float) -> bool:
        """Verify boat rounded mark on port (left) side using cross product"""
        # Use cross product of vectors to determine which side mark was on
        # Vector 1: from previous position to current position (boat path)
        # Vector 2: from previous position to mark

        boat_dx = self.longitude - self.previous_lon
        boat_dy = self.latitude - self.previous_lat

        mark_dx = mark_lon - self.previous_lon
        mark_dy = mark_lat - self.previous_lat

        # Cross product (2D): positive = mark on port (left), negative = starboard (right)
        cross_product = boat_dx * mark_dy - boat_dy * mark_dx

        return cross_product > 0  # Port side rounding

    def _check_mark_rounding(self):
        """Check if boat has rounded the current mark - simplified to just check distance"""
        if self.current_mark_index >= len(self.marks_to_round):
            return  # No more marks to round

        mark = self.marks_to_round[self.current_mark_index]
        mark_lat, mark_lon = mark[0], mark[1]

        # Calculate distance to current mark
        distance_to_mark = calculate_distance(self.latitude, self.longitude, mark_lat, mark_lon)

        # Simple distance check - if within 0.1nm, mark is rounded
        if distance_to_mark <= self.mark_rounding_radius_nm:
            # Mark this mark as rounded
            self.marks_rounded[self.current_mark_index] = True
            self.current_mark_index += 1

            # Update destination to next mark or finish
            self.destination_bearing = self._calculate_bearing_to_destination()
            self.distance_to_destination = self._calculate_distance_to_destination()
            self.distance_at_last_tack_check = self.distance_to_destination

            # Immediately set optimal heading for new destination
            self._set_optimal_heading_for_destination()

            # Reset tack timer to allow future tacking decisions
            self.time_since_last_tack = 0

            # Apply penalty based on sailing angle at mark rounding
            destination_classification = classify_destination_relative_to_wind(
                self.destination_bearing, self.wind_direction
            )
            if destination_classification.mode == SailingMode.UPWIND:  # Upwind
                self.current_penalty_factor = 0.40
                self.current_penalty_duration = 65
                self.last_maneuver_type = "mark_tack"
            elif destination_classification.mode == SailingMode.DOWNWIND:  # Downwind
                self.current_penalty_factor = 0.80
                self.current_penalty_duration = 35
                self.last_maneuver_type = "mark_jibe"
            else:  # SailingMode.REACHING
                self.current_penalty_factor = 0.65
                self.current_penalty_duration = 48
                self.last_maneuver_type = "mark_reach"

    def _calculate_line_endpoints(self, center_lat: float, center_lon: float, bearing: float, half_distance_nm: float):
        """Calculate endpoints of a line perpendicular to a bearing"""
        # Convert bearing to radians
        bearing_rad = math.radians(bearing)

        # Calculate offset in degrees (1 degree ≈ 60 nm)
        lat_offset = (half_distance_nm / 60) * math.cos(bearing_rad)
        lon_offset = (half_distance_nm / 60) * math.sin(bearing_rad) / math.cos(math.radians(center_lat))

        # Port endpoint (left)
        port_lat = center_lat - lat_offset
        port_lon = center_lon - lon_offset

        # Starboard endpoint (right)
        starboard_lat = center_lat + lat_offset
        starboard_lon = center_lon + lon_offset

        return (port_lat, port_lon), (starboard_lat, starboard_lon)

    def _check_line_crossings(self):
        """Check if boat has crossed start or finish line"""
        # Check start line crossing
        if not self.has_started:
            if self._crossed_line(self.start_line_port, self.start_line_starboard):
                self.has_started = True
                self.start_time = time.time()

        # Check finish line crossing (only if already started AND all marks rounded)
        if self.has_started and not self.has_finished:
            # Only finish if all marks have been rounded
            all_marks_rounded = self.current_mark_index >= len(self.marks_to_round)
            if all_marks_rounded and self._crossed_line(self.finish_line_port, self.finish_line_starboard):
                self.has_finished = True
                self.finish_time = time.time()

    def _crossed_line(self, line_p1, line_p2) -> bool:
        """Check if boat's path crossed a line segment"""
        # Line segment: line_p1 to line_p2
        # Boat path: (previous_lat, previous_lon) to (current_lat, current_lon)

        # Use line intersection algorithm
        x1, y1 = self.previous_lon, self.previous_lat
        x2, y2 = self.longitude, self.latitude
        x3, y3 = line_p1[1], line_p1[0]
        x4, y4 = line_p2[1], line_p2[0]

        denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)

        if abs(denom) < 1e-10:
            # Lines are parallel
            return False

        t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom
        u = -((x1 - x2) * (y1 - y3) - (y1 - y2) * (x1 - x3)) / denom

        # Check if intersection point is on both line segments
        if 0 <= t <= 1 and 0 <= u <= 1:
            return True

        return False

    def _generate_sail_configuration(self) -> list:
        """Generate realistic sail configuration based on apparent wind angle and wind speed"""
        configuration = []

        # Calculate apparent wind angle (normalize to 0-180)
        apparent_wind_angle = abs(self._calculate_apparent_wind_angle())
        if apparent_wind_angle > 180:
            apparent_wind_angle = 360 - apparent_wind_angle

        # Choose mainsail based on wind speed
        if self.wind_speed > 25:
            # Heavy winds - use trysail or deeply reefed main
            configuration.append("trysail" if random.random() < 0.3 else "reefed_main")
        elif self.wind_speed > 18:
            # Moderate-heavy winds - reef the main
            configuration.append("reefed_main" if random.random() < 0.7 else "mainsail")
        else:
            # Light-moderate winds - full main
            rand = random.random()
            if rand < 0.90:
                configuration.append("mainsail")
            elif rand < 0.98:
                configuration.append("reefed_main")
            else:
                configuration.append("trysail")

        # Choose foresail based on apparent wind angle and wind speed
        if apparent_wind_angle < 50:
            # Close-hauled / beating (upwind)
            if self.wind_speed > 25:
                configuration.append("storm_jib")
            elif self.wind_speed > 18:
                configuration.append(random.choice(["working_jib", "jib", "staysail"]))
            else:
                configuration.append(random.choice(["jib", "genoa", "working_jib"]))

        elif apparent_wind_angle < 80:
            # Close reach
            if self.wind_speed > 20:
                configuration.append(random.choice(["jib", "working_jib"]))
            else:
                configuration.append(random.choice(["genoa", "jib"]))

        elif apparent_wind_angle < 120:
            # Beam reach
            if self.wind_speed > 20:
                configuration.append("jib")
            else:
                configuration.append(random.choice(["genoa", "code_zero"]))

        elif apparent_wind_angle < 160:
            # Broad reach
            if self.wind_speed > 18:
                configuration.append(random.choice(["code_zero", "gennaker"]))
            else:
                configuration.append(random.choice(["asymmetric_spinnaker", "gennaker", "code_zero"]))

        else:
            # Running (downwind)
            if self.wind_speed > 20:
                configuration.append(random.choice(["asymmetric_spinnaker", "gennaker"]))
            else:
                configuration.append(random.choice(["spinnaker", "asymmetric_spinnaker", "blooper"]))

        # Optional mizzen for ketch/yawl rigs (20% chance)
        if random.random() < 0.2:
            configuration.append("mizzen")

        return configuration if configuration else ["bare_poles"]


def generate_sample_data(num_samples: int = 10, interval_seconds: float = 1.0) -> list:
    """
    Generate multiple telemetry samples

    Args:
        num_samples: Number of samples to generate
        interval_seconds: Time interval between samples (race time)

    Returns:
        List of telemetry dictionaries
    """
    # Import here to avoid circular dependency
    from fleet import SailboatFleet
    from datetime import datetime, timezone

    # Create a fleet with one boat (includes shared weather)
    fleet = SailboatFleet(num_boats=1)
    samples = []

    # Start from current time
    start_time = datetime.now(timezone.utc).timestamp()

    for i in range(num_samples):
        current_race_timestamp = start_time + (i * interval_seconds)
        fleet_telemetry = fleet.generate_fleet_telemetry(current_race_timestamp)
        samples.append(fleet_telemetry[0])

        if i < num_samples - 1:  # Don't sleep after last sample
            if i % 1000:
                time.sleep(1)

    return samples


if __name__ == "__main__":
    # Demo: Generate and print 5 telemetry samples
    print("Generating sailboat telemetry data...\n")

    # Create a fleet (which includes shared weather)
    from fleet import SailboatFleet
    from datetime import datetime, timezone

    fleet = SailboatFleet(num_boats=1)
    boat = fleet.boats[0]
    boat.boat_id = "DEMO-001"
    boat.boat_name = "Victory"

    start_time = datetime.now(timezone.utc).timestamp()

    for i in range(5):
        current_race_timestamp = start_time + (i * 60)  # 60 seconds between samples
        fleet_telemetry = fleet.generate_fleet_telemetry(current_race_timestamp)
        telemetry = fleet_telemetry[0]
        print(f"Sample {i+1}:")
        for key, value in telemetry.items():
            print(f"  {key}: {value}")
        print()
        time.sleep(1)
