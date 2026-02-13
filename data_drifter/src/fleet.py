"""
Sailboat Fleet Management

Manages multiple sailboats and coordinates shared weather conditions.
"""

from typing import List, Dict, Any

from boat import Boat
from weather import Weather
from sailboat_telemetry import SailboatTelemetryGenerator


class SailboatFleet:
    """Manages multiple sailboats and generates telemetry for all of them"""

    def __init__(self, num_boats: int = 1,
                 start_lat: float = 37.8, start_lon: float = -122.4,
                 base_wind_direction: float = 0,
                 min_wind_speed: float = 8, max_wind_speed: float = 18,
                 wind_direction_variation: float = 10,
                 simulated_time_step_seconds: float = 1.0,
                 line_length_nm: float = 0.1,
                 marks: list = None,
                 mark_rounding_radius_nm: float = 0.05,
                 min_weather_stability_seconds: float = 300,
                 max_weather_stability_seconds: float = 900,
                 wind_speed_change_rate: float = 0.5,
                 wind_direction_change_rate: float = 2.0,
                 frontal_probability: float = 0.15,
                 gradual_probability: float = 0.65,
                 gust_probability: float = 0.20,
                 frontal_direction_change: tuple = (20, 40),
                 frontal_speed_change: tuple = (5, 10),
                 gradual_direction_change: tuple = (5, 15),
                 gradual_speed_change: tuple = (2, 5),
                 gust_speed_increase: tuple = (5, 8),
                 gust_duration: float = 120):
        """
        Initialize a fleet of sailboats

        Args:
            num_boats: Number of boats in the fleet
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
            min_weather_stability_seconds: Minimum time weather stays stable (race time seconds)
            max_weather_stability_seconds: Maximum time weather stays stable (race time seconds)
            wind_speed_change_rate: How fast wind speed changes during transitions (knots/update)
            wind_direction_change_rate: How fast direction changes during transitions (deg/update)
            frontal_probability: Probability of frontal passage event
            gradual_probability: Probability of gradual shift event
            gust_probability: Probability of sudden gust event
            frontal_direction_change: (min, max) direction change for frontal passage
            frontal_speed_change: (min, max) speed change for frontal passage
            gradual_direction_change: (min, max) direction change for gradual shift
            gradual_speed_change: (min, max) speed change for gradual shift
            gust_speed_increase: (min, max) speed increase for gust
            gust_duration: Duration of gust in race time seconds
        """
        # Store marks configuration
        self.marks = marks if marks else []
        self.mark_rounding_radius_nm = mark_rounding_radius_nm

        # Create shared weather system for all boats with all configuration parameters
        self.weather = Weather(
            base_wind_direction=base_wind_direction,
            min_wind_speed=min_wind_speed,
            max_wind_speed=max_wind_speed,
            wind_direction_variation=wind_direction_variation,
            min_stability_seconds=min_weather_stability_seconds,
            max_stability_seconds=max_weather_stability_seconds,
            wind_speed_change_rate=wind_speed_change_rate,
            wind_direction_change_rate=wind_direction_change_rate,
            frontal_probability=frontal_probability,
            gradual_probability=gradual_probability,
            gust_probability=gust_probability,
            simulated_time_step=simulated_time_step_seconds,
            frontal_direction_change=frontal_direction_change,
            frontal_speed_change=frontal_speed_change,
            gradual_direction_change=gradual_direction_change,
            gradual_speed_change=gradual_speed_change,
            gust_speed_increase=gust_speed_increase,
            gust_duration=gust_duration
        )

        # Create boats with random racing strategies
        self.boats = []
        for i in range(num_boats):
            # Create a boat with a random strategy
            boat = Boat()

            # Create telemetry generator for this boat
            telemetry_generator = SailboatTelemetryGenerator(
                boat=boat,
                start_lat=start_lat,
                start_lon=start_lon,
                base_wind_direction=base_wind_direction,
                min_wind_speed=min_wind_speed,
                max_wind_speed=max_wind_speed,
                wind_direction_variation=wind_direction_variation,
                simulated_time_step_seconds=simulated_time_step_seconds,
                line_length_nm=line_length_nm,
                marks=self.marks,
                mark_rounding_radius_nm=self.mark_rounding_radius_nm
            )
            self.boats.append(telemetry_generator)

    def generate_fleet_telemetry(self, current_race_timestamp: float) -> List[Dict[str, Any]]:
        """
        Generate telemetry for all boats in the fleet

        Args:
            current_race_timestamp: Current race time as Unix timestamp (seconds since epoch)

        Returns:
            List of telemetry dictionaries, one for each boat
        """
        # Update shared weather conditions
        self.weather.update()
        conditions = self.weather.get_conditions()

        # Generate telemetry for all boats with same weather
        telemetry_data = []
        for boat in self.boats:
            # Skip boats that have finished the race or DNF'd
            if boat.has_finished or boat.has_dnf:
                continue

            telemetry = boat.generate_telemetry(
                wind_speed=conditions["wind_speed"],
                wind_direction=conditions["wind_direction"],
                current_race_timestamp=current_race_timestamp
            )
            telemetry_data.append(telemetry)
        return telemetry_data

    def get_boat_count(self) -> int:
        """Get the number of boats in the fleet"""
        return len(self.boats)
