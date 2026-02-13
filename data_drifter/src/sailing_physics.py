"""
Sailing Physics Calculations

Pure functions for calculating sailing performance based on wind conditions,
boat characteristics, and sailing angles. Implements realistic polar diagram
behavior and sail selection logic.

Uses navigation_utils.py for point of sail classifications and angle calculations.
"""

import random
from navigation_utils import PointOfSail, get_point_of_sail
from boat import BoatPerformanceModifiers


class SailingPhysics:
    """
    Sailing physics calculations based on real-world sailing dynamics.

    All methods are static - no state, pure calculations based on inputs.
    This makes testing easy and behavior predictable.
    """

    @staticmethod
    def calculate_speed(
        wind_speed: float,
        wind_angle: float,
        point_of_sail: PointOfSail,
        has_spinnaker: bool,
        modifiers: BoatPerformanceModifiers,
        time_since_last_tack: float,
        current_penalty_factor: float,
        current_penalty_duration: float,
        current_speed: float
    ) -> float:
        """
        Calculate boat speed based on wind conditions and sailing angle.

        Implements polar diagram logic - boat speed varies significantly
        with angle to wind based on point of sail.

        Args:
            wind_speed: True wind speed in knots
            wind_angle: Angle between boat heading and true wind (-180 to 180)
            point_of_sail: Current point of sail (from navigation_utils)
            has_spinnaker: Whether spinnaker/gennaker is deployed
            modifiers: Boat-specific performance characteristics
            time_since_last_tack: Seconds since last tack/jibe
            current_penalty_factor: Speed penalty multiplier (0.4-1.0)
            current_penalty_duration: How long penalty lasts (seconds)
            current_speed: Current boat speed for gradual adjustment

        Returns:
            New boat speed in knots
        """
        abs_wind_angle = abs(wind_angle)

        # Calculate base speed factor from polar diagram
        # Use point of sail for major categories, fine-tune with angle
        if point_of_sail == PointOfSail.IN_IRONS:
            # In irons - boat stalls (0-10% speed)
            speed_factor = 0.1 * (abs_wind_angle / 35)

        elif point_of_sail == PointOfSail.CLOSE_HAULED:
            # Close-hauled (35-50°): 30-75% speed
            if abs_wind_angle < 45:
                speed_factor = 0.3 + 0.3 * ((abs_wind_angle - 35) / 10)
            else:
                speed_factor = 0.6 + 0.15 * ((abs_wind_angle - 45) / 5)

        elif point_of_sail == PointOfSail.CLOSE_REACH:
            # Close reach (50-70°): 75-90% speed
            speed_factor = 0.75 + 0.15 * ((abs_wind_angle - 50) / 20)

        elif point_of_sail == PointOfSail.BEAM_REACH:
            # Beam reach (70-110°): 85-95% speed - fastest!
            speed_factor = 0.85 + 0.10 * ((abs_wind_angle - 70) / 40)
            if has_spinnaker and abs_wind_angle > 90:
                speed_factor += 0.05  # Bonus for spinnaker on broad angles

        elif point_of_sail == PointOfSail.BROAD_REACH:
            # Broad reach (110-150°): 80-90% speed
            speed_factor = 0.80 + 0.10 * ((abs_wind_angle - 110) / 40)
            if has_spinnaker:
                speed_factor += 0.10  # Significant spinnaker bonus

        else:  # PointOfSail.RUNNING
            # Running (>150°): 70-110% speed depending on spinnaker
            if has_spinnaker:
                # Spinnakers are VERY effective on dead runs
                # Can actually go faster than wind speed in some conditions
                speed_factor = 0.90 + 0.20 * ((180 - abs_wind_angle) / 30)
            else:
                # Without spinnaker - slower due to main blanketing jib
                speed_factor = 0.65 + 0.15 * ((180 - abs_wind_angle) / 30)

        # Apply tack/jibe penalty if recent maneuver
        is_in_tack_penalty = time_since_last_tack < current_penalty_duration
        if is_in_tack_penalty:
            # Base penalty modified by boat type (catamarans tack with less penalty)
            tack_penalty_factor = current_penalty_factor * modifiers.tacking_penalty_multiplier
            tack_penalty_factor = max(0.2, min(1.0, tack_penalty_factor))
        else:
            tack_penalty_factor = 1.0  # No penalty

        # Apply wind condition modifiers (light vs heavy wind performance)
        if wind_speed < 8:
            wind_condition_multiplier = modifiers.light_wind_multiplier
        elif wind_speed > 15:
            wind_condition_multiplier = modifiers.heavy_wind_multiplier
        else:
            wind_condition_multiplier = 1.0  # Moderate wind

        # Calculate target speed with random variation
        target_speed = (wind_speed * speed_factor * tack_penalty_factor *
                       wind_condition_multiplier * random.uniform(0.9, 1.1))

        # Gradually adjust to target speed (realistic acceleration/deceleration)
        new_speed = current_speed + (target_speed - current_speed) * 0.2

        # Apply boat-specific speed limits
        new_speed = max(0.5, min(modifiers.max_speed_limit, new_speed))

        return new_speed

    @staticmethod
    def calculate_heel_angle(
        wind_speed: float,
        point_of_sail: PointOfSail,
        has_spinnaker: bool,
        modifiers: BoatPerformanceModifiers
    ) -> float:
        """
        Calculate heel angle based on wind conditions and point of sail.

        Heel is the sideways tilt of the boat caused by wind pressure on sails.
        - Upwind (close-hauled): Maximum heel
        - Reaching: Moderate heel
        - Downwind: Minimal heel (especially with spinnaker)

        Args:
            wind_speed: True wind speed in knots
            point_of_sail: Current point of sail
            has_spinnaker: Whether spinnaker is deployed
            modifiers: Boat stability characteristics

        Returns:
            Heel angle in degrees
        """
        if point_of_sail in (PointOfSail.IN_IRONS, PointOfSail.CLOSE_HAULED):
            # Close-hauled upwind - maximum heel
            heel = (wind_speed / 1.5) * random.uniform(0.9, 1.1) * modifiers.stability

        elif point_of_sail == PointOfSail.RUNNING:
            # Running downwind - minimal heel
            if has_spinnaker:
                # Spinnaker pulls boat forward, not sideways
                heel = (wind_speed / 5.0) * random.uniform(0.7, 1.0) * modifiers.stability
            else:
                # Still less heel than upwind
                heel = (wind_speed / 3.5) * random.uniform(0.8, 1.0) * modifiers.stability

        else:  # CLOSE_REACH, BEAM_REACH, BROAD_REACH
            # Reaching - moderate heel
            heel = (wind_speed / 2.5) * random.uniform(0.8, 1.0) * modifiers.stability

        # Apply maximum heel limit (varies by boat type)
        max_heel = 35 * modifiers.stability
        return max(2, min(max_heel, heel))

    @staticmethod
    def calculate_apparent_wind_speed(
        true_wind_speed: float,
        boat_speed: float
    ) -> float:
        """
        Calculate apparent wind speed (simplified).

        Apparent wind is the wind experienced by the moving boat.
        This is a simplified calculation - full vector addition would be more accurate.

        Args:
            true_wind_speed: True wind speed in knots
            boat_speed: Boat speed in knots

        Returns:
            Apparent wind speed in knots
        """
        # Simplified: apparent wind increases as boat moves
        return true_wind_speed + (boat_speed * 0.5)

    @staticmethod
    def calculate_apparent_wind_angle(
        wind_direction: float,
        boat_heading: float
    ) -> float:
        """
        Calculate apparent wind angle relative to boat heading.

        Simplified calculation - doesn't account for boat speed effects.

        Args:
            wind_direction: True wind direction (0-360°, where wind comes FROM)
            boat_heading: Boat heading (0-360°)

        Returns:
            Apparent wind angle in degrees (0-360)
        """
        angle = (wind_direction - boat_heading) % 360
        return angle

    @staticmethod
    def select_sail_configuration(
        wind_speed: float,
        point_of_sail: PointOfSail
    ) -> list[str]:
        """
        Select optimal sail configuration based on conditions.

        Realistic sail choices based on wind strength and point of sail:
        - Light winds: Full sails (mainsail, genoa, spinnaker)
        - Heavy winds: Reefed/storm sails
        - Upwind: Working jibs
        - Downwind: Spinnakers/gennakers

        Args:
            wind_speed: Wind speed in knots
            point_of_sail: Current point of sail

        Returns:
            List of active sails (e.g., ["mainsail", "genoa"])
        """
        configuration = []

        # Choose mainsail based on wind speed
        if wind_speed > 25:
            # Heavy winds - use trysail or deeply reefed main
            configuration.append("trysail" if random.random() < 0.3 else "reefed_main")
        elif wind_speed > 18:
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

        # Choose foresail based on point of sail and wind speed
        if point_of_sail in (PointOfSail.IN_IRONS, PointOfSail.CLOSE_HAULED):
            # Upwind sails
            if wind_speed > 25:
                configuration.append("storm_jib")
            elif wind_speed > 18:
                configuration.append(random.choice(["working_jib", "jib", "staysail"]))
            else:
                configuration.append(random.choice(["jib", "genoa", "working_jib"]))

        elif point_of_sail == PointOfSail.CLOSE_REACH:
            # Close reach
            if wind_speed > 20:
                configuration.append(random.choice(["jib", "working_jib"]))
            else:
                configuration.append(random.choice(["genoa", "jib"]))

        elif point_of_sail == PointOfSail.BEAM_REACH:
            # Beam reach
            if wind_speed > 20:
                configuration.append("jib")
            else:
                configuration.append(random.choice(["genoa", "code_zero"]))

        elif point_of_sail == PointOfSail.BROAD_REACH:
            # Broad reach
            if wind_speed > 18:
                configuration.append(random.choice(["code_zero", "gennaker"]))
            else:
                configuration.append(random.choice(["asymmetric_spinnaker", "gennaker", "code_zero"]))

        else:  # PointOfSail.RUNNING
            # Running (downwind)
            if wind_speed > 20:
                configuration.append(random.choice(["asymmetric_spinnaker", "gennaker"]))
            else:
                configuration.append(random.choice(["spinnaker", "asymmetric_spinnaker", "blooper"]))

        # Optional mizzen for ketch/yawl rigs (20% chance)
        if random.random() < 0.2:
            configuration.append("mizzen")

        return configuration if configuration else ["bare_poles"]
