"""
Boat Representation

Represents a sailboat with its identity, type, and racing strategy.
"""

import random
from faker import Faker
from racing_strategy import RacingStrategy
from boat_type import BoatType, BoatTypeModifiers

fake = Faker()


class Boat:
    """
    Represents a sailboat with unique identity, type, and racing strategy.

    Each boat has:
    - Unique identifier (boat_id)
    - Name (boat_name)
    - Boat type (monohull/catamaran with variants)
    - Racing strategy that determines tactical decisions
    """

    # Fun boat name themes
    BOAT_NAME_SUFFIXES = [
        "DataLake Navigator", "Delta Voyager", "Lakehouse Explorer",
        "Spark Sailor", "Unity Cruiser", "Medallion Runner",
        "Bronze Wave", "Silver Stream", "Gold Horizon",
        "Photon Racer", "Catalog Seeker", "Schema Surfer",
        "Pipeline Pioneer", "Warehouse Wanderer", "Metastore Mariner",
        "Cluster Clipper", "Notebook Navigator", "Query Quest",
        "Table Tracker", "Zerobus Zephyr", "ETL Endeavor",
        "Analytics Adventurer", "Insights Islander", "Workflow Windrunner",
        "Streaming Schooner", "Partition Pathfinder", "Optimize Odyssey",
        "Compute Captain", "ML Model Merchant", "Data Drifter",
        "Serverless Seafarer", "Auto Loader Legend", "DLT Dreadnought",
        "Vector Vessel", "Feature Frigate", "Inference Interceptor",
        "Batch Buccaneer", "Real-time Raider", "CDC Corsair",
        "ACID Armada", "Columnar Catamaran", "Parquet Privateer",
        "DBFS Dinghy", "Mount Mermaid", "Secret Sloop", 
        "Token Trawler", "IAM Islander", "RBAC Runner",
        "Governance Galleon"
    ]

    def __init__(self, boat_id: str = None, boat_name: str = None,
                 boat_type: str = None, racing_strategy: RacingStrategy = None):
        """
        Initialize a boat

        Args:
            boat_id: Unique identifier (auto-generated if not provided)
            boat_name: Boat name (auto-generated if not provided)
            boat_type: Type of boat (auto-generated if not provided)
            racing_strategy: RacingStrategy instance (random if not provided)
        """
        self.boat_type = boat_type or BoatType.random_boat_type()
        self.boat_id = boat_id or self._generate_boat_id()
        self.boat_name = boat_name or self._generate_boat_name()
        self.racing_strategy = racing_strategy or RacingStrategy.random_strategy()
        self.modifiers = BoatType.get_modifiers(self.boat_type)

        # Assign crew experience within boat type's range
        # Higher crew experience = faster reaction to weather shifts
        self.crew_experience = random.uniform(
            self.modifiers.crew_experience_min,
            self.modifiers.crew_experience_max
        )

    @staticmethod
    def _generate_boat_id() -> str:
        """Generate a unique boat ID"""
        return f"BOAT-{random.randint(1000, 9999)}"

    @staticmethod
    def _generate_boat_name() -> str:
        """Generate a boat name"""
        first_name = fake.first_name()
        suffix = random.choice(Boat.BOAT_NAME_SUFFIXES)
        return f"{first_name}'s {suffix}"

    def get_boat_id(self) -> str:
        """Get boat ID"""
        return self.boat_id

    def get_boat_name(self) -> str:
        """Get boat name"""
        return self.boat_name

    def get_boat_type(self) -> str:
        """Get boat type"""
        return self.boat_type

    def get_racing_strategy(self) -> RacingStrategy:
        """Get racing strategy"""
        return self.racing_strategy

    def get_modifiers(self) -> BoatTypeModifiers:
        """Get boat type performance modifiers"""
        return self.modifiers

    def get_crew_experience(self) -> float:
        """Get crew experience level (0.0-1.0)"""
        return self.crew_experience

    def __str__(self) -> str:
        return f"{self.boat_name} ({self.boat_id}) - {self.boat_type} - {self.racing_strategy.strategy_type}"

    def __repr__(self) -> str:
        return f"Boat(boat_id='{self.boat_id}', boat_name='{self.boat_name}', type='{self.boat_type}', strategy='{self.racing_strategy.strategy_type}')"
