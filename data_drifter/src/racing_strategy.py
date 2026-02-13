"""
Racing Strategy Configuration

Defines different racing strategies that boats can use for tactical decisions.
"""

import random
from typing import Dict, Any


class RacingStrategy:
    """
    Represents a racing strategy with specific tacking and VMG parameters.

    10 distinct racing strategies with varied tactical philosophies:
    - Safe & Steady: Extremely conservative, minimal tacking
    - Distance Minimizer: Constantly optimizes angle, frequent tacking
    - Balanced: Middle ground between speed and distance
    - Pure VMG: Maximizes velocity made good toward destination
    - Wind Hunter: Responds quickly to wind shifts
    - Risk Taker: Gambles on marginal decisions, high variance
    - Mark Specialist: Optimized for courses with frequent marks
    - Steady Eddie: Ultimate patience and consistency
    - Tactical Racer: Strategic thinker, balances short/long term
    - Speed Demon: Prioritizes boat speed over optimal course
    """

    # Available strategy types
    CONSERVATIVE = "safe_and_steady"
    AGGRESSIVE = "distance_minimizer"
    BALANCED = "balanced"
    VMG_OPTIMIZER = "pure_vmg"
    WIND_HUNTER = "wind_hunter"
    RISK_TAKER = "risk_taker"
    MARK_SPECIALIST = "mark_specialist"
    STEADY_EDDIE = "steady_eddie"
    TACTICAL_RACER = "tactical_racer"
    SPEED_DEMON = "speed_demon"

    ALL_STRATEGIES = [
        CONSERVATIVE, AGGRESSIVE, BALANCED, VMG_OPTIMIZER,
        WIND_HUNTER, RISK_TAKER, MARK_SPECIALIST,
        STEADY_EDDIE, TACTICAL_RACER, SPEED_DEMON
    ]

    def __init__(self, strategy_type: str):
        """
        Initialize a racing strategy

        Args:
            strategy_type: One of CONSERVATIVE, AGGRESSIVE, BALANCED, or VMG_OPTIMIZER
        """
        if strategy_type not in self.ALL_STRATEGIES:
            raise ValueError(f"Invalid strategy type: {strategy_type}. Must be one of {self.ALL_STRATEGIES}")

        self.strategy_type = strategy_type
        self.params = self._get_strategy_params(strategy_type)

    def _get_strategy_params(self, strategy_type: str) -> Dict[str, Any]:
        """Get parameters for a specific strategy type"""
        strategies = {
            self.CONSERVATIVE: {
                "description": "Minimizes tacks to maintain speed, very patient",
                "min_tack_time": 180,  # Much longer between tacks
                "tack_advantage_high": 60,  # Needs massive advantage to tack
                "tack_advantage_medium": 40,
                "tack_advantage_low": 20,
                "vmg_threshold_strong": -1.5,  # Very tolerant of negative VMG
                "vmg_threshold_weak": -0.5,
                "tack_probability_medium": 0.05,  # Rarely tacks
                "tack_probability_low": 0.01,
            },
            self.AGGRESSIVE: {
                "description": "Constantly optimizes angle, tacks frequently",
                "min_tack_time": 30,  # Tacks very frequently
                "tack_advantage_high": 15,  # Tacks on tiny advantages
                "tack_advantage_medium": 7,
                "tack_advantage_low": 2,
                "vmg_threshold_strong": -0.2,  # Very sensitive to negative VMG
                "vmg_threshold_weak": -0.02,
                "tack_probability_medium": 0.5,  # 50% chance on medium advantage
                "tack_probability_low": 0.2,
            },
            self.BALANCED: {
                "description": "Balanced approach between speed and course",
                "min_tack_time": 90,  # Now true middle ground
                "tack_advantage_high": 35,
                "tack_advantage_medium": 20,
                "tack_advantage_low": 8,
                "vmg_threshold_strong": -0.6,
                "vmg_threshold_weak": -0.15,
                "tack_probability_medium": 0.15,
                "tack_probability_low": 0.03,
            },
            self.VMG_OPTIMIZER: {
                "description": "Maximizes velocity toward destination",
                "min_tack_time": 40,  # Very responsive
                "tack_advantage_high": 18,  # More aggressive than before
                "tack_advantage_medium": 9,
                "tack_advantage_low": 3,
                "vmg_threshold_strong": -0.1,  # Extremely sensitive to negative VMG
                "vmg_threshold_weak": -0.01,
                "tack_probability_medium": 0.4,
                "tack_probability_low": 0.15,
            },
            self.WIND_HUNTER: {
                "description": "Responds quickly to wind shifts",
                "min_tack_time": 50,
                "tack_advantage_high": 22,
                "tack_advantage_medium": 11,
                "tack_advantage_low": 4,
                "vmg_threshold_strong": -0.4,
                "vmg_threshold_weak": -0.08,
                "tack_probability_medium": 0.3,
                "tack_probability_low": 0.12,
            },
            self.RISK_TAKER: {
                "description": "Gambles on marginal decisions, high variance",
                "min_tack_time": 60,
                "tack_advantage_high": 25,
                "tack_advantage_medium": 12,
                "tack_advantage_low": 3,  # Low threshold
                "vmg_threshold_strong": -0.3,
                "vmg_threshold_weak": -0.05,
                "tack_probability_medium": 0.6,  # 60% on medium - very random!
                "tack_probability_low": 0.4,  # 40% even on low advantages
            },
            self.MARK_SPECIALIST: {
                "description": "Optimized for courses with frequent marks",
                "min_tack_time": 70,
                "tack_advantage_high": 28,
                "tack_advantage_medium": 14,
                "tack_advantage_low": 6,
                "vmg_threshold_strong": -0.5,
                "vmg_threshold_weak": -0.12,
                "tack_probability_medium": 0.18,
                "tack_probability_low": 0.04,
            },
            self.STEADY_EDDIE: {
                "description": "Extremely stable, never hasty, ultimate consistency",
                "min_tack_time": 240,  # 4 minutes!
                "tack_advantage_high": 70,  # Massive threshold
                "tack_advantage_medium": 50,
                "tack_advantage_low": 30,
                "vmg_threshold_strong": -2.0,  # Accepts terrible VMG
                "vmg_threshold_weak": -0.8,
                "tack_probability_medium": 0.02,  # Almost never random tacks
                "tack_probability_low": 0.001,
            },
            self.TACTICAL_RACER: {
                "description": "Strategic thinker, balances short/long term",
                "min_tack_time": 80,
                "tack_advantage_high": 32,
                "tack_advantage_medium": 16,
                "tack_advantage_low": 7,
                "vmg_threshold_strong": -0.45,
                "vmg_threshold_weak": -0.1,
                "tack_probability_medium": 0.22,
                "tack_probability_low": 0.06,
            },
            self.SPEED_DEMON: {
                "description": "Prioritizes boat speed over optimal course",
                "min_tack_time": 150,  # Long delays to keep speed up
                "tack_advantage_high": 50,
                "tack_advantage_medium": 30,
                "tack_advantage_low": 15,
                "vmg_threshold_strong": -0.8,  # Tolerates bad VMG to keep speed
                "vmg_threshold_weak": -0.25,
                "tack_probability_medium": 0.08,
                "tack_probability_low": 0.015,
            },
        }

        return strategies[strategy_type]

    @classmethod
    def random_strategy(cls) -> 'RacingStrategy':
        """Create a random racing strategy"""
        strategy_type = random.choice(cls.ALL_STRATEGIES)
        return cls(strategy_type)

    def get_min_tack_time(self) -> float:
        """Get minimum time between tacks (seconds)"""
        return self.params["min_tack_time"]

    def get_tack_advantage_thresholds(self) -> tuple:
        """Get tack advantage thresholds (high, medium, low) in degrees"""
        return (
            self.params["tack_advantage_high"],
            self.params["tack_advantage_medium"],
            self.params["tack_advantage_low"]
        )

    def get_vmg_thresholds(self) -> tuple:
        """Get VMG thresholds (strong, weak) in knots"""
        return (
            self.params["vmg_threshold_strong"],
            self.params["vmg_threshold_weak"]
        )

    def get_tack_probabilities(self) -> tuple:
        """Get tacking probabilities (medium, low) as 0-1 values"""
        return (
            self.params["tack_probability_medium"],
            self.params["tack_probability_low"]
        )

    def get_description(self) -> str:
        """Get human-readable description of this strategy"""
        return self.params["description"]

    def __str__(self) -> str:
        return f"{self.strategy_type} ({self.get_description()})"

    def __repr__(self) -> str:
        return f"RacingStrategy('{self.strategy_type}')"
