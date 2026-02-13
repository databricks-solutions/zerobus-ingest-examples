"""
Boat Type Definitions

Defines different types of sailboats with realistic performance characteristics.
"""

from dataclasses import dataclass
from typing import Dict
import random


@dataclass
class BoatTypeModifiers:
    """
    Performance modifiers for a boat type

    Attributes:
        speed_multiplier: Overall speed multiplier (1.0 = base, >1.0 = faster, <1.0 = slower)
        upwind_efficiency: Multiplier for upwind performance (0.8-1.2)
        reaching_efficiency: Multiplier for reaching performance (0.8-1.2)
        downwind_efficiency: Multiplier for downwind performance (0.8-1.2)
        light_wind_multiplier: Performance in light wind <8kt (0.8-1.3)
        heavy_wind_multiplier: Performance in heavy wind >15kt (0.8-1.2)
        tacking_penalty_multiplier: Multiplier for speed loss when tacking (0.8-1.4)
        stability: Heel angle reduction factor (0.7-1.0, lower = more stable)
        max_speed_limit: Maximum speed cap in knots
        crew_experience_min: Minimum crew experience level (0.0-1.0)
        crew_experience_max: Maximum crew experience level (0.0-1.0)
    """
    speed_multiplier: float = 1.0
    upwind_efficiency: float = 1.0
    reaching_efficiency: float = 1.0
    downwind_efficiency: float = 1.0
    light_wind_multiplier: float = 1.0
    heavy_wind_multiplier: float = 1.0
    tacking_penalty_multiplier: float = 1.0
    stability: float = 1.0
    max_speed_limit: float = 12.0
    crew_experience_min: float = 0.5
    crew_experience_max: float = 0.9


class BoatType:
    """Defines different types of sailboats with realistic characteristics"""

    # Boat type definitions
    RACING_MONOHULL = "Racing Monohull"
    CRUISING_MONOHULL = "Cruising Monohull"
    PERFORMANCE_CATAMARAN = "Performance Catamaran"
    CRUISING_CATAMARAN = "Cruising Catamaran"

    # All available boat types
    ALL_TYPES = [
        RACING_MONOHULL,
        CRUISING_MONOHULL,
        PERFORMANCE_CATAMARAN,
        CRUISING_CATAMARAN
    ]

    # Performance characteristics for each boat type
    MODIFIERS: Dict[str, BoatTypeModifiers] = {
        RACING_MONOHULL: BoatTypeModifiers(
            speed_multiplier=1.05,         # Slightly faster overall
            upwind_efficiency=1.15,        # Excellent upwind performance
            reaching_efficiency=1.05,      # Good reaching
            downwind_efficiency=0.95,      # Decent downwind
            light_wind_multiplier=0.90,    # Struggles in light wind
            heavy_wind_multiplier=1.10,    # Excels in heavy wind
            tacking_penalty_multiplier=1.2,  # Moderate tacking penalty
            stability=1.0,                 # Normal heel
            max_speed_limit=14.0           # Can reach higher speeds
        ),

        CRUISING_MONOHULL: BoatTypeModifiers(
            speed_multiplier=0.85,         # Slower overall (heavier displacement)
            upwind_efficiency=0.95,        # Decent upwind
            reaching_efficiency=0.90,      # Moderate reaching
            downwind_efficiency=0.90,      # Moderate downwind
            light_wind_multiplier=0.85,    # Poor in light wind
            heavy_wind_multiplier=0.95,    # Handles heavy wind steadily
            tacking_penalty_multiplier=1.4,  # Significant tacking penalty (heavy)
            stability=0.75,                # Very stable (less heel)
            max_speed_limit=10.0           # Lower speed limit
        ),

        PERFORMANCE_CATAMARAN: BoatTypeModifiers(
            speed_multiplier=1.25,         # Much faster overall
            upwind_efficiency=0.85,        # Poor upwind (can't point as high)
            reaching_efficiency=1.25,      # Excellent reaching
            downwind_efficiency=1.30,      # Exceptional downwind speed
            light_wind_multiplier=1.15,    # Good in light wind
            heavy_wind_multiplier=1.20,    # Very fast in heavy wind
            tacking_penalty_multiplier=0.8,  # Minimal tacking penalty
            stability=0.70,                # Very stable platform
            max_speed_limit=18.0           # High top speed
        ),

        CRUISING_CATAMARAN: BoatTypeModifiers(
            speed_multiplier=1.00,         # Average speed
            upwind_efficiency=0.80,        # Weak upwind
            reaching_efficiency=1.10,      # Good reaching
            downwind_efficiency=1.15,      # Good downwind
            light_wind_multiplier=1.25,    # Excellent in light wind (light displacement)
            heavy_wind_multiplier=0.95,    # Moderate in heavy wind
            tacking_penalty_multiplier=0.9,  # Low tacking penalty
            stability=0.65,                # Extremely stable
            max_speed_limit=14.0           # Moderate top speed
        ),
    }

    @classmethod
    def get_modifiers(cls, boat_type: str) -> BoatTypeModifiers:
        """
        Get performance modifiers for a boat type

        Args:
            boat_type: Type of boat (must be one of the defined types)

        Returns:
            BoatTypeModifiers for the specified boat type
        """
        return cls.MODIFIERS.get(boat_type, BoatTypeModifiers())

    @classmethod
    def random_boat_type(cls) -> str:
        """Select a random boat type"""
        return random.choice(cls.ALL_TYPES)

    @classmethod
    def get_boat_type_description(cls, boat_type: str) -> str:
        """Get a description of the boat type characteristics"""
        descriptions = {
            cls.RACING_MONOHULL: "Fast racing yacht - excellent upwind, great in heavy air",
            cls.CRUISING_MONOHULL: "Traditional cruiser - stable and steady, slower overall",
            cls.PERFORMANCE_CATAMARAN: "High-performance cat - blazing downwind speed, weaker upwind",
            cls.CRUISING_CATAMARAN: "Cruising catamaran - stable platform, good light wind performance"
        }
        return descriptions.get(boat_type, "Unknown boat type")
