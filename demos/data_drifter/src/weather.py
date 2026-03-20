"""
Weather System for Sailboat Racing Simulation

Manages shared weather conditions that all boats experience simultaneously.
Implements realistic weather patterns with stable periods and gradual shifts.
"""

import random
from typing import Dict, Optional
from enum import Enum


class WeatherEventType(Enum):
    """Types of weather events that can occur"""
    STABLE = "stable"
    FRONTAL_PASSAGE = "frontal_passage"
    GRADUAL_SHIFT = "gradual_shift"
    SUDDEN_GUST = "gust"


class Weather:
    """Manages weather conditions that all boats experience"""

    def __init__(self, base_wind_direction: float, min_wind_speed: float = 8,
                 max_wind_speed: float = 18, wind_direction_variation: float = 10,
                 min_stability_seconds: float = 300, max_stability_seconds: float = 900,
                 wind_speed_change_rate: float = 0.5, wind_direction_change_rate: float = 2.0,
                 frontal_probability: float = 0.15, gradual_probability: float = 0.65,
                 gust_probability: float = 0.20, simulated_time_step: float = 60,
                 frontal_direction_change: tuple = (20, 40), frontal_speed_change: tuple = (5, 10),
                 gradual_direction_change: tuple = (5, 15), gradual_speed_change: tuple = (2, 5),
                 gust_speed_increase: tuple = (5, 8), gust_duration: float = 120):
        """
        Initialize weather system with realistic patterns

        Args:
            base_wind_direction: Base wind direction in degrees (where wind comes FROM)
            min_wind_speed: Minimum wind speed in knots
            max_wind_speed: Maximum wind speed in knots
            wind_direction_variation: Wind direction variation in degrees (+/-)
            min_stability_seconds: Minimum time weather stays stable (race time seconds)
            max_stability_seconds: Maximum time weather stays stable (race time seconds)
            wind_speed_change_rate: How fast wind speed changes during transitions (knots/update)
            wind_direction_change_rate: How fast direction changes during transitions (deg/update)
            frontal_probability: Probability of frontal passage event
            gradual_probability: Probability of gradual shift event
            gust_probability: Probability of sudden gust event
            simulated_time_step: Seconds of race time per update
            frontal_direction_change: (min, max) direction change for frontal passage
            frontal_speed_change: (min, max) speed change for frontal passage
            gradual_direction_change: (min, max) direction change for gradual shift
            gradual_speed_change: (min, max) speed change for gradual shift
            gust_speed_increase: (min, max) speed increase for gust
            gust_duration: Duration of gust in race time seconds
        """
        self.base_wind_direction = base_wind_direction
        self.min_wind_speed = min_wind_speed
        self.max_wind_speed = max_wind_speed
        self.wind_direction_variation = wind_direction_variation
        self.min_stability_seconds = min_stability_seconds
        self.max_stability_seconds = max_stability_seconds
        self.wind_speed_change_rate = wind_speed_change_rate
        self.wind_direction_change_rate = wind_direction_change_rate
        self.simulated_time_step = simulated_time_step

        # Event probabilities
        self.frontal_probability = frontal_probability
        self.gradual_probability = gradual_probability
        self.gust_probability = gust_probability

        # Event characteristics
        self.frontal_direction_change = frontal_direction_change
        self.frontal_speed_change = frontal_speed_change
        self.gradual_direction_change = gradual_direction_change
        self.gradual_speed_change = gradual_speed_change
        self.gust_speed_increase = gust_speed_increase
        self.gust_duration = gust_duration

        # Current conditions (initialize with some variation)
        self.wind_direction = (base_wind_direction + random.uniform(-wind_direction_variation/2, wind_direction_variation/2)) % 360
        self.wind_speed = random.uniform(min_wind_speed + 2, max_wind_speed - 2)

        # Weather state tracking
        self.time_in_current_state = 0.0
        self.next_stability_duration = random.uniform(min_stability_seconds, max_stability_seconds)
        self.current_event = WeatherEventType.STABLE

        # Transition state
        self.in_transition = False
        self.target_wind_speed: Optional[float] = None
        self.target_wind_direction: Optional[float] = None

        # Gust state
        self.in_gust = False
        self.gust_start_time = 0.0
        self.gust_base_speed = 0.0
        self.gust_peak_speed = 0.0

    def update(self):
        """Update weather conditions for the next time step"""
        self.time_in_current_state += self.simulated_time_step

        # Handle active gust
        if self.in_gust:
            self._update_gust()
            return

        # Handle transition to new weather state
        if self.in_transition:
            self._update_transition()
            return

        # Check if it's time for a new weather event
        if self.time_in_current_state >= self.next_stability_duration:
            self._trigger_weather_event()

    def _trigger_weather_event(self):
        """Trigger a new weather event"""
        # Choose event type based on probabilities
        rand = random.random()

        if rand < self.frontal_probability:
            self._start_frontal_passage()
        elif rand < self.frontal_probability + self.gradual_probability:
            self._start_gradual_shift()
        else:
            self._start_sudden_gust()

    def _start_frontal_passage(self):
        """Start a frontal passage event - large wind shift"""
        self.current_event = WeatherEventType.FRONTAL_PASSAGE
        self.in_transition = True

        # Large direction change
        direction_change = random.uniform(self.frontal_direction_change[0], self.frontal_direction_change[1])
        if random.random() < 0.5:
            direction_change = -direction_change

        # Large speed change
        speed_change = random.uniform(self.frontal_speed_change[0], self.frontal_speed_change[1])
        if random.random() < 0.5:
            speed_change = -speed_change

        # Calculate targets
        self.target_wind_direction = (self.wind_direction + direction_change) % 360
        self.target_wind_speed = max(self.min_wind_speed, min(self.max_wind_speed, self.wind_speed + speed_change))

        # Ensure target stays within allowed variation from base
        self._clamp_target_direction()

    def _start_gradual_shift(self):
        """Start a gradual shift event - slow, steady change"""
        self.current_event = WeatherEventType.GRADUAL_SHIFT
        self.in_transition = True

        # Small direction change
        direction_change = random.uniform(self.gradual_direction_change[0], self.gradual_direction_change[1])
        if random.random() < 0.5:
            direction_change = -direction_change

        # Small speed change
        speed_change = random.uniform(self.gradual_speed_change[0], self.gradual_speed_change[1])
        if random.random() < 0.5:
            speed_change = -speed_change

        # Calculate targets
        self.target_wind_direction = (self.wind_direction + direction_change) % 360
        self.target_wind_speed = max(self.min_wind_speed, min(self.max_wind_speed, self.wind_speed + speed_change))

        # Ensure target stays within allowed variation from base
        self._clamp_target_direction()

    def _start_sudden_gust(self):
        """Start a sudden gust event - temporary speed spike"""
        self.current_event = WeatherEventType.SUDDEN_GUST
        self.in_gust = True
        self.gust_start_time = 0.0

        # Store base speed and calculate peak
        self.gust_base_speed = self.wind_speed
        gust_increase = random.uniform(self.gust_speed_increase[0], self.gust_speed_increase[1])
        self.gust_peak_speed = min(self.max_wind_speed, self.wind_speed + gust_increase)

    def _update_transition(self):
        """Gradually transition to target conditions"""
        if self.target_wind_speed is None or self.target_wind_direction is None:
            self.in_transition = False
            return

        # Gradually adjust wind speed
        speed_diff = self.target_wind_speed - self.wind_speed
        if abs(speed_diff) <= self.wind_speed_change_rate:
            self.wind_speed = self.target_wind_speed
        else:
            self.wind_speed += self.wind_speed_change_rate if speed_diff > 0 else -self.wind_speed_change_rate

        # Gradually adjust wind direction (shortest path)
        direction_diff = self.target_wind_direction - self.wind_direction
        if direction_diff > 180:
            direction_diff -= 360
        elif direction_diff < -180:
            direction_diff += 360

        if abs(direction_diff) <= self.wind_direction_change_rate:
            self.wind_direction = self.target_wind_direction
        else:
            self.wind_direction += self.wind_direction_change_rate if direction_diff > 0 else -self.wind_direction_change_rate

        self.wind_direction = self.wind_direction % 360

        # Check if transition is complete
        if self.wind_speed == self.target_wind_speed and self.wind_direction == self.target_wind_direction:
            self.in_transition = False
            self.target_wind_speed = None
            self.target_wind_direction = None
            self.current_event = WeatherEventType.STABLE
            self.time_in_current_state = 0.0
            self.next_stability_duration = random.uniform(self.min_stability_seconds, self.max_stability_seconds)

    def _update_gust(self):
        """Update during a gust event"""
        self.gust_start_time += self.simulated_time_step

        # Calculate gust progression (0 to 1)
        gust_progress = self.gust_start_time / self.gust_duration

        if gust_progress >= 1.0:
            # Gust is over, return to base speed
            self.wind_speed = self.gust_base_speed
            self.in_gust = False
            self.current_event = WeatherEventType.STABLE
            self.time_in_current_state = 0.0
            self.next_stability_duration = random.uniform(self.min_stability_seconds, self.max_stability_seconds)
        else:
            # Gust follows a curve: ramps up, peaks, ramps down
            # Use sine wave for smooth transition
            import math
            gust_factor = math.sin(gust_progress * math.pi)
            self.wind_speed = self.gust_base_speed + (self.gust_peak_speed - self.gust_base_speed) * gust_factor

    def _clamp_target_direction(self):
        """Ensure target direction stays within allowed variation from base"""
        if self.target_wind_direction is None:
            return

        # Calculate offset from base
        offset = (self.target_wind_direction - self.base_wind_direction) % 360
        if offset > 180:
            offset -= 360

        # Clamp to allowed variation
        if offset > self.wind_direction_variation:
            offset = self.wind_direction_variation
        elif offset < -self.wind_direction_variation:
            offset = -self.wind_direction_variation

        self.target_wind_direction = (self.base_wind_direction + offset) % 360

    def get_conditions(self) -> Dict[str, float]:
        """Get current weather conditions"""
        return {
            "wind_speed": self.wind_speed,
            "wind_direction": self.wind_direction
        }

    def get_detailed_conditions(self) -> Dict:
        """Get detailed weather conditions including event state"""
        return {
            "wind_speed": self.wind_speed,
            "wind_direction": self.wind_direction,
            "event_type": self.current_event.value,
            "in_transition": self.in_transition,
            "time_in_state": self.time_in_current_state,
            "next_change_in": max(0, self.next_stability_duration - self.time_in_current_state) if not self.in_transition else 0
        }
