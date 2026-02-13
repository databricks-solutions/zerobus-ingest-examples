"""
Sailing Navigation Logic

Strategic sailing decisions: tacking, heading adjustments, VMG optimization.
Handles all tactical racing decisions based on wind, destination, and strategy.
"""

import random
from typing import Optional, NamedTuple
from navigation_utils import (
    SailingMode,
    DestinationClassification,
    classify_destination_relative_to_wind,
    get_optimal_sailing_angles,
    normalize_angle_difference
)
from racing_strategy import RacingStrategy


class TackingDecision(NamedTuple):
    """Result of tacking decision analysis"""
    should_tack: bool
    new_heading: Optional[float]
    maneuver_type: Optional[str]  # "tack", "jibe", "reach"
    penalty_factor: float  # Speed penalty multiplier
    penalty_duration: float  # Duration in seconds


class SailingNavigator:
    """
    Makes tactical sailing decisions based on destination and conditions.

    Handles:
    - Tacking/jibing decisions (VMG optimization)
    - Gradual heading adjustments
    - Weather shift detection and response
    - VMG calculations
    """

    def __init__(self, racing_strategy: RacingStrategy):
        """
        Initialize navigator with racing strategy.

        Args:
            racing_strategy: Strategy defining aggressive vs conservative sailing
        """
        self.racing_strategy = racing_strategy

        # Tacking state
        self.time_since_last_tack = 0.0
        self.distance_at_last_tack_check = 0.0
        self.distance_samples = []  # For VMG trend analysis

        # Weather shift response
        self.weather_shift_detected = False
        self.weather_shift_reaction_delay = 0.0
        self.previous_wind_direction = 0.0
        self.previous_wind_speed = 0.0

    def should_tack(
        self,
        current_heading: float,
        destination_bearing: float,
        wind_direction: float,
        angle_to_dest: float,
        vmg: float,
        distance_to_destination: float,
        current_wind_angle: float
    ) -> TackingDecision:
        """
        Determine if boat should tack/jibe based on VMG and strategy.

        Decision logic:
        1. Emergency tacking (negative VMG, in irons)
        2. Strategic tacking (better angle on other tack)
        3. Crew experience and weather shift effects

        Args:
            current_heading: Current boat heading (0-360°)
            destination_bearing: Bearing to destination (0-360°)
            wind_direction: True wind direction (0-360°)
            angle_to_dest: Angle from heading to destination
            vmg: Current Velocity Made Good (knots)
            distance_to_destination: Distance to destination (nm)
            current_wind_angle: Angle from heading to wind (-180 to 180)

        Returns:
            TackingDecision with should_tack flag and heading if tacking
        """
        # Classify destination relative to wind
        dest_class = classify_destination_relative_to_wind(destination_bearing, wind_direction)

        # Calculate optimal sailing angles
        if dest_class.mode == SailingMode.DOWNWIND:
            starboard_heading, port_heading = get_optimal_sailing_angles(
                wind_direction, dest_class.mode
            )
            maneuver_type = "jibe"
            penalty_factor = 0.80  # 20% speed loss
            penalty_duration = 35  # seconds
        elif dest_class.mode == SailingMode.UPWIND:
            starboard_heading, port_heading = get_optimal_sailing_angles(
                wind_direction, dest_class.mode
            )
            maneuver_type = "tack"
            penalty_factor = 0.40  # 60% speed loss
            penalty_duration = 65  # seconds
        else:  # REACHING
            starboard_heading, port_heading = get_optimal_sailing_angles(
                wind_direction, dest_class.mode
            )
            maneuver_type = "reach"
            penalty_factor = 0.65  # 35% speed loss
            penalty_duration = 48  # seconds

        # Calculate angles to destination for each tack
        starboard_angle = normalize_angle_difference(destination_bearing, starboard_heading)
        port_angle = normalize_angle_difference(destination_bearing, port_heading)

        # Determine current tack and which is better
        currently_on_starboard = current_wind_angle > 0
        on_better_tack = ((currently_on_starboard and starboard_angle < port_angle) or
                         (not currently_on_starboard and port_angle < starboard_angle))

        # Calculate tack advantage (how much better is the other tack?)
        tack_advantage = abs(starboard_angle - port_angle)

        # Get strategy-specific parameters
        min_tack_time = self.racing_strategy.get_min_tack_time()
        tack_adv_high, tack_adv_med, tack_adv_low = self.racing_strategy.get_tack_advantage_thresholds()
        vmg_strong, vmg_weak = self.racing_strategy.get_vmg_thresholds()
        prob_med, prob_low = self.racing_strategy.get_tack_probabilities()

        # Adjust thresholds during weather shift (crew more conservative)
        if self.weather_shift_reaction_delay > 0:
            reaction_factor = self.weather_shift_reaction_delay / 180.0
            prob_med *= (1.0 - 0.5 * reaction_factor)
            prob_low *= (1.0 - 0.7 * reaction_factor)
            tack_adv_high += 10 * reaction_factor
            tack_adv_med += 5 * reaction_factor

        should_tack = False

        # EMERGENCY 1: Negative VMG (moving away from destination)
        if vmg < vmg_weak and self.time_since_last_tack > 30:
            should_tack = True

        # EMERGENCY 2: In irons (too close to wind)
        elif abs(current_wind_angle) < 35 and self.time_since_last_tack > 20:
            should_tack = True

        # STRATEGIC: Not on better tack and enough time passed
        elif not on_better_tack and self.time_since_last_tack > min_tack_time:
            # Decision based on tack advantage and VMG
            if tack_advantage > tack_adv_high:
                # Large advantage - always tack
                should_tack = True
            elif tack_advantage > tack_adv_med:
                # Medium advantage - tack with medium probability
                should_tack = random.random() < prob_med
            elif tack_advantage > tack_adv_low and vmg < vmg_strong:
                # Small advantage but poor VMG - tack with low probability
                should_tack = random.random() < prob_low

        # Special case: Close to destination and can fetch it on current tack
        if dest_class.mode != SailingMode.DOWNWIND:
            dest_angle_from_wind = dest_class.angle_to_wind
            if angle_to_dest < 50 and dest_angle_from_wind > 50:
                should_tack = False  # Can reach destination on this tack

        # Determine new heading if tacking
        new_heading = None
        if should_tack:
            if currently_on_starboard:
                new_heading = port_heading
            else:
                new_heading = starboard_heading

        return TackingDecision(
            should_tack=should_tack,
            new_heading=new_heading,
            maneuver_type=maneuver_type if should_tack else None,
            penalty_factor=penalty_factor if should_tack else 1.0,
            penalty_duration=penalty_duration if should_tack else 0.0
        )

    def adjust_heading_gradually(
        self,
        current_heading: float,
        destination_bearing: float,
        wind_direction: float,
        angle_to_dest: float,
        sailing_mode: SailingMode
    ) -> float:
        """
        Make gradual heading adjustments toward optimal sailing angle.

        Prevents jerky movements, simulates realistic steering.

        Args:
            current_heading: Current boat heading (0-360°)
            destination_bearing: Bearing to destination (0-360°)
            wind_direction: True wind direction (0-360°)
            angle_to_dest: Angle from heading to destination
            sailing_mode: Current sailing mode (UPWIND/DOWNWIND/REACHING)

        Returns:
            Adjusted heading (0-360°)
        """
        # Calculate current wind angle
        current_wind_angle = (current_heading - wind_direction) % 360
        if current_wind_angle > 180:
            current_wind_angle = current_wind_angle - 360

        new_heading = current_heading

        if sailing_mode == SailingMode.DOWNWIND:
            # DOWNWIND SAILING - maintain broad reach angles
            if angle_to_dest < 30:
                # Close to destination - turn directly toward it
                angle_diff = (destination_bearing - current_heading) % 360
                if angle_diff > 180:
                    angle_diff = angle_diff - 360
                new_heading += angle_diff * 0.1  # Gradual turn
            else:
                # Maintain good downwind angle (140-170° from wind)
                if abs(current_wind_angle) < 140:
                    # Too close to wind - bear away
                    new_heading += random.uniform(2, 5) * (-1 if current_wind_angle >= 0 else 1)
                elif abs(current_wind_angle) > 175:
                    # Dead run (unstable) - head up slightly
                    new_heading -= random.uniform(2, 5) * (-1 if current_wind_angle >= 0 else 1)
                else:
                    # Fine tune
                    new_heading += random.uniform(-1, 1)

        elif sailing_mode == SailingMode.UPWIND:
            # UPWIND SAILING - maintain close-hauled angles
            if abs(current_wind_angle) < 38:
                # Too close to wind (in irons) - bear away IMMEDIATELY
                new_heading += random.uniform(5, 10) * (1 if current_wind_angle >= 0 else -1)
            elif abs(current_wind_angle) > 55:
                # Too far from wind - head up
                new_heading -= random.uniform(2, 5) * (1 if current_wind_angle >= 0 else -1)
            else:
                # Maintain optimal upwind angle (40-50°)
                new_heading += random.uniform(-1, 1)

        else:  # REACHING
            # REACHING - can sail more directly toward destination
            if angle_to_dest < 30:
                # Close - turn directly toward destination
                angle_diff = (destination_bearing - current_heading) % 360
                if angle_diff > 180:
                    angle_diff = angle_diff - 360
                new_heading += angle_diff * 0.1
            else:
                # Fine tune toward destination
                new_heading += random.uniform(-2, 2)

        # Normalize to 0-360
        return new_heading % 360

    def detect_weather_shift(
        self,
        current_wind_direction: float,
        current_wind_speed: float
    ) -> bool:
        """
        Detect significant weather changes and set reaction delay.

        Simulates crew noticing shift and adjusting sails/tactics.

        Args:
            current_wind_direction: Current wind direction (0-360°)
            current_wind_speed: Current wind speed (knots)

        Returns:
            True if significant shift detected
        """
        direction_change = normalize_angle_difference(
            current_wind_direction,
            self.previous_wind_direction
        )
        speed_change = abs(current_wind_speed - self.previous_wind_speed)

        # Significant shift: >5° direction or >2 knots speed
        if (direction_change > 5 or speed_change > 2) and not self.weather_shift_detected:
            self.weather_shift_detected = True
            # Crew reaction time: 30-180 seconds based on experience
            # (This would be passed from boat.crew_experience)
            self.weather_shift_reaction_delay = random.uniform(30, 180)
            return True

        # Update previous values
        self.previous_wind_direction = current_wind_direction
        self.previous_wind_speed = current_wind_speed

        return False

    def calculate_vmg(
        self,
        current_distance: float,
        time_delta: float
    ) -> float:
        """
        Calculate Velocity Made Good toward destination.

        VMG is the rate of closing distance to the destination:
        - Positive = getting closer
        - Negative = moving away
        - Zero = maintaining distance

        Args:
            current_distance: Current distance to destination (nm)
            time_delta: Time since last calculation (seconds)

        Returns:
            VMG in knots (negative if moving away from destination)
        """
        # Track distance samples for trend analysis
        self.distance_samples.append(current_distance)
        if len(self.distance_samples) > 5:
            self.distance_samples.pop(0)  # Keep last 5 samples

        # Calculate VMG from distance change
        if len(self.distance_samples) >= 2:
            distance_change = self.distance_samples[-2] - current_distance
            time_hours = time_delta / 3600.0
            vmg = distance_change / time_hours if time_hours > 0 else 0.0
        else:
            vmg = 0.0

        return vmg

    def update_tacking_timer(self, time_delta: float):
        """Update time since last tack"""
        self.time_since_last_tack += time_delta

    def reset_tacking_timer(self):
        """Reset tacking timer after executing a tack/jibe"""
        self.time_since_last_tack = 0.0
        self.distance_at_last_tack_check = 0.0

    def update_weather_shift_delay(self, time_delta: float):
        """Decrease weather shift reaction delay over time"""
        if self.weather_shift_reaction_delay > 0:
            self.weather_shift_reaction_delay = max(0, self.weather_shift_reaction_delay - time_delta)
            if self.weather_shift_reaction_delay == 0:
                self.weather_shift_detected = False
