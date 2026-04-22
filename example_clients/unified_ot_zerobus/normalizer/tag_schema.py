"""Normalized tag schema definition for unified industrial data representation."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class TagQuality(str, Enum):
    """Tag quality codes."""
    GOOD = "good"
    BAD = "bad"
    UNCERTAIN = "uncertain"


class TagDataType(str, Enum):
    """Tag data types."""
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    STRING = "string"
    TIMESTAMP = "timestamp"


@dataclass
class NormalizedTag:
    """
    Unified tag structure for all protocols.

    This schema is protocol-independent and provides a consistent
    structure regardless of whether data comes from OPC-UA, Modbus, or MQTT.
    """

    # Core Identification
    tag_id: str
    tag_path: str

    # Value & Quality
    value: Any
    quality: TagQuality
    timestamp: datetime

    # Metadata
    data_type: TagDataType
    engineering_units: Optional[str] = None

    # Source Info
    source_protocol: str = ""
    source_identifier: str = ""
    source_address: str = ""

    # Context (optional)
    site_id: Optional[str] = None
    line_id: Optional[str] = None
    equipment_id: Optional[str] = None
    signal_type: Optional[str] = None

    # Additional
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to JSON-serializable dictionary.

        Returns:
            Dictionary representation with all fields.
        """
        result = asdict(self)

        # Convert enums to strings
        result['quality'] = self.quality.value
        result['data_type'] = self.data_type.value

        # Convert timestamp to ISO format string
        if isinstance(self.timestamp, datetime):
            result['timestamp'] = self.timestamp.isoformat()

        # Remove None values to reduce payload size
        result = {k: v for k, v in result.items() if v is not None}

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> NormalizedTag:
        """
        Create NormalizedTag from dictionary.

        Args:
            data: Dictionary with tag data

        Returns:
            NormalizedTag instance
        """
        # Convert string enums back to enum types
        if 'quality' in data and isinstance(data['quality'], str):
            data['quality'] = TagQuality(data['quality'])

        if 'data_type' in data and isinstance(data['data_type'], str):
            data['data_type'] = TagDataType(data['data_type'])

        # Convert timestamp string to datetime
        if 'timestamp' in data and isinstance(data['timestamp'], str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])

        return cls(**data)

    def __repr__(self) -> str:
        """Human-readable representation."""
        return (
            f"NormalizedTag(tag_path='{self.tag_path}', "
            f"value={self.value}, quality={self.quality.value}, "
            f"timestamp={self.timestamp.isoformat()})"
        )
