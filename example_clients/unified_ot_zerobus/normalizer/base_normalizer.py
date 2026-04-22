"""Base normalizer for all protocols."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Optional

from unified_connector.normalizer.tag_schema import NormalizedTag, TagQuality, TagDataType
from unified_connector.normalizer.quality_mapper import QualityMapper
from unified_connector.normalizer.path_builder import TagPathBuilder

logger = logging.getLogger(__name__)


class BaseNormalizer(ABC):
    """
    Abstract base class for protocol-specific normalizers.

    Subclasses must implement:
    - _get_protocol_name(): Return protocol name
    - normalize(): Convert raw data to NormalizedTag
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize normalizer with configuration.

        Args:
            config: Normalization configuration
        """
        self.config = config
        self.quality_mapper = QualityMapper()
        self.path_builder = TagPathBuilder(config.get("path_building", {}))

    @abstractmethod
    def _get_protocol_name(self) -> str:
        """
        Get protocol name.

        Returns:
            Protocol name (e.g., "opcua", "modbus", "mqtt")
        """
        pass

    @abstractmethod
    def normalize(self, raw_data: dict[str, Any]) -> NormalizedTag:
        """
        Convert raw protocol data to normalized tag.

        Args:
            raw_data: Raw data from protocol client

        Returns:
            NormalizedTag instance
        """
        pass

    def _extract_value(self, raw_data: dict[str, Any]) -> Any:
        """
        Extract value from raw data.

        Can be overridden by subclasses for protocol-specific extraction.

        Args:
            raw_data: Raw protocol data

        Returns:
            Extracted value
        """
        return raw_data.get("value")

    def _extract_timestamp(self, raw_data: dict[str, Any]) -> datetime:
        """
        Extract timestamp from raw data.

        Can be overridden by subclasses for protocol-specific extraction.

        Args:
            raw_data: Raw protocol data

        Returns:
            Timestamp as datetime object (UTC)
        """
        timestamp = raw_data.get("timestamp")

        if timestamp is None:
            # Use current time if no timestamp provided
            return datetime.now(timezone.utc)

        # Handle various timestamp formats
        if isinstance(timestamp, datetime):
            # Ensure timezone aware
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            return timestamp

        if isinstance(timestamp, (int, float)):
            # Unix timestamp (seconds or milliseconds)
            if timestamp > 1e10:
                # Likely milliseconds
                timestamp = timestamp / 1000.0
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)

        if isinstance(timestamp, str):
            try:
                # Try ISO format
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except Exception:
                logger.warning(f"Could not parse timestamp '{timestamp}', using current time")
                return datetime.now(timezone.utc)

        logger.warning(f"Unknown timestamp format: {type(timestamp)}, using current time")
        return datetime.now(timezone.utc)

    def _determine_data_type(self, value: Any) -> TagDataType:
        """
        Infer data type from Python type.

        Args:
            value: Value to check

        Returns:
            TagDataType enum
        """
        if isinstance(value, bool):
            return TagDataType.BOOL
        elif isinstance(value, int):
            return TagDataType.INT
        elif isinstance(value, float):
            return TagDataType.FLOAT
        elif isinstance(value, datetime):
            return TagDataType.TIMESTAMP
        elif isinstance(value, str):
            return TagDataType.STRING
        else:
            # Default to string for unknown types
            return TagDataType.STRING

    def _build_metadata(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """
        Build metadata dictionary.

        Args:
            raw_data: Raw protocol data

        Returns:
            Metadata dictionary
        """
        metadata = {}

        # Add protocol-specific metadata
        protocol_metadata = self._extract_protocol_metadata(raw_data)
        metadata.update(protocol_metadata)

        # Add any extra fields from raw_data
        excluded_keys = {
            'value', 'timestamp', 'quality', 'status_code',
            'source_identifier', 'source_address'
        }

        for key, val in raw_data.items():
            if key not in excluded_keys and key not in metadata:
                # Convert to string if not JSON-serializable
                try:
                    import json
                    json.dumps(val)
                    metadata[key] = val
                except (TypeError, ValueError):
                    metadata[key] = str(val)

        return metadata

    def _extract_protocol_metadata(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """
        Extract protocol-specific metadata.

        Can be overridden by subclasses.

        Args:
            raw_data: Raw protocol data

        Returns:
            Protocol-specific metadata dictionary
        """
        return {}

    def _extract_context(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """
        Extract context information for path building.

        Can be overridden by subclasses.

        Args:
            raw_data: Raw protocol data

        Returns:
            Context dictionary with site_id, line_id, equipment_id, signal_type
        """
        context = {}

        # Extract from config if provided
        config = raw_data.get("config", {})
        for key in ["site_id", "line_id", "equipment_id", "signal_type"]:
            if key in config:
                context[key] = config[key]

        return context

    def _build_tag_path(
        self,
        source_identifier: str,
        context: dict[str, Any]
    ) -> str:
        """
        Build hierarchical tag path.

        Args:
            source_identifier: Protocol-specific identifier
            context: Context for path building

        Returns:
            Normalized tag path
        """
        protocol = self._get_protocol_name()
        return self.path_builder.build_path(protocol, source_identifier, context)

    def _generate_tag_id(self, tag_path: str) -> str:
        """
        Generate unique tag ID.

        Args:
            tag_path: Normalized tag path

        Returns:
            Unique tag ID
        """
        protocol = self._get_protocol_name()
        return self.path_builder.generate_tag_id(tag_path, protocol)
