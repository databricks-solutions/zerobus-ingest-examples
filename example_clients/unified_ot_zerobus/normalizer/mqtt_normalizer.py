"""MQTT specific normalizer implementation."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from unified_connector.normalizer.base_normalizer import BaseNormalizer
from unified_connector.normalizer.tag_schema import NormalizedTag, TagQuality

logger = logging.getLogger(__name__)


class MQTTNormalizer(BaseNormalizer):
    """
    Normalizer for MQTT protocol data.

    Expected input format:
    {
        "topic": "factory/line3/machine1/temperature",
        "payload": "75.3",  # or JSON string
        "qos": 1,
        "retained": false,
        "timestamp": "2025-01-18T14:23:45.123Z",
        "broker_address": "mqtt.factory:1883",
        "config": {
            "data_type": "float",
            "engineering_units": "celsius"
        }
    }
    """

    def _get_protocol_name(self) -> str:
        return "mqtt"

    def normalize(self, raw_data: dict[str, Any]) -> NormalizedTag:
        """
        Convert MQTT data to normalized tag.

        Args:
            raw_data: Raw MQTT data

        Returns:
            NormalizedTag instance
        """
        try:
            # Extract topic
            topic = raw_data.get("topic", "")
            source_identifier = topic

            # Extract and parse payload
            payload = raw_data.get("payload", "")
            config = raw_data.get("config", {})

            value, payload_timestamp = self._parse_payload(payload, config)

            # Extract timestamp (prefer payload timestamp if available)
            if payload_timestamp:
                timestamp = payload_timestamp
            else:
                timestamp = self._extract_timestamp(raw_data)

            # Calculate message age for quality determination
            now = datetime.now(timezone.utc)
            age_seconds = (now - timestamp).total_seconds()

            # Map quality
            retained = raw_data.get("retained", False)
            age_threshold = config.get("age_threshold", 300.0)  # 5 minutes default

            quality = self.quality_mapper.map_mqtt_quality(retained, age_seconds, age_threshold)

            # Determine data type
            tag_data_type = self._determine_data_type(value)

            # Extract engineering units
            engineering_units = config.get("engineering_units")

            # Extract context from topic and config
            context = self._extract_context_from_topic(topic)
            config_context = self._extract_context(raw_data)
            context.update(config_context)  # Config takes precedence

            # Build tag path
            tag_path = self._build_tag_path(source_identifier, context)

            # Generate tag ID
            tag_id = self._generate_tag_id(tag_path)

            # Source address
            source_address = raw_data.get("broker_address", "")

            # Build metadata
            metadata = self._build_metadata(raw_data)
            metadata["message_age_seconds"] = age_seconds

            return NormalizedTag(
                tag_id=tag_id,
                tag_path=tag_path,
                value=value,
                quality=quality,
                timestamp=timestamp,
                data_type=tag_data_type,
                engineering_units=engineering_units,
                source_protocol=self._get_protocol_name(),
                source_identifier=source_identifier,
                source_address=source_address,
                site_id=context.get("site_id"),
                line_id=context.get("line_id"),
                equipment_id=context.get("equipment_id"),
                signal_type=context.get("signal_type"),
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Error normalizing MQTT data: {e}", exc_info=True)
            raise

    def _parse_payload(
        self,
        payload: Any,
        config: dict[str, Any]
    ) -> tuple[Any, datetime | None]:
        """
        Parse MQTT payload.

        Tries to parse as JSON first, falls back to string conversion.
        If JSON has "value" field, extracts it.
        If JSON has "timestamp" field, extracts it.

        Args:
            payload: Raw payload (bytes, string, or already parsed)
            config: Configuration with data_type hint

        Returns:
            Tuple of (parsed_value, timestamp_if_found)
        """
        timestamp = None

        # Convert bytes to string if needed
        if isinstance(payload, bytes):
            try:
                payload = payload.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning("Could not decode payload as UTF-8")
                return payload.hex(), None

        # Try to parse as JSON
        parsed_json = None
        if isinstance(payload, str):
            try:
                parsed_json = json.loads(payload)
            except json.JSONDecodeError:
                # Not JSON, will use as string
                pass

        # Extract value and timestamp from JSON if available
        if isinstance(parsed_json, dict):
            # Try to extract timestamp
            if "timestamp" in parsed_json:
                try:
                    timestamp_str = parsed_json["timestamp"]
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except Exception as e:
                    logger.debug(f"Could not parse timestamp from JSON: {e}")

            # Extract value
            if "value" in parsed_json:
                value = parsed_json["value"]
            else:
                # Use entire JSON object as value
                value = parsed_json
        elif parsed_json is not None:
            # JSON but not a dict (list, number, string, etc.)
            value = parsed_json
        else:
            # Not JSON, use as string
            value = payload

        # Convert to configured data type if specified
        data_type = config.get("data_type", "").lower()
        if data_type and isinstance(value, str):
            value = self._convert_to_data_type(value, data_type)

        return value, timestamp

    def _convert_to_data_type(self, value: str, data_type: str) -> Any:
        """
        Convert string value to specified data type.

        Args:
            value: String value to convert
            data_type: Target data type

        Returns:
            Converted value (or original string if conversion fails)
        """
        try:
            if data_type == "float":
                return float(value)
            elif data_type == "int":
                return int(value)
            elif data_type == "bool":
                return value.lower() in ('true', '1', 'yes', 'on')
            else:
                return value
        except (ValueError, AttributeError) as e:
            logger.warning(f"Could not convert '{value}' to {data_type}: {e}")
            return value

    def _extract_context_from_topic(self, topic: str) -> dict[str, Any]:
        """
        Extract context from MQTT topic.

        Common patterns:
        - "factory/line3/machine1/temperature"
        - "site/area/equipment/signal"

        Args:
            topic: MQTT topic string

        Returns:
            Context dictionary
        """
        context = {}

        if not topic:
            return context

        # Split topic by /
        parts = topic.split("/")

        if len(parts) >= 4:
            # Full hierarchy: site/line/equipment/signal
            context["site_id"] = parts[0].lower()
            context["line_id"] = parts[1].lower()
            context["equipment_id"] = parts[2].lower()
            context["signal_type"] = parts[3].lower()

        elif len(parts) == 3:
            # Partial: line/equipment/signal
            context["line_id"] = parts[0].lower()
            context["equipment_id"] = parts[1].lower()
            context["signal_type"] = parts[2].lower()

        elif len(parts) == 2:
            # Minimal: equipment/signal
            context["equipment_id"] = parts[0].lower()
            context["signal_type"] = parts[1].lower()

        elif len(parts) == 1:
            # Just signal name
            context["signal_type"] = parts[0].lower()

        return context

    def _extract_protocol_metadata(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """
        Extract MQTT specific metadata.

        Args:
            raw_data: Raw MQTT data

        Returns:
            Protocol-specific metadata
        """
        metadata = {}

        # Add MQTT specific fields
        if "topic" in raw_data:
            metadata["mqtt_topic"] = raw_data["topic"]

        if "qos" in raw_data:
            metadata["mqtt_qos"] = raw_data["qos"]

        if "retained" in raw_data:
            metadata["mqtt_retained"] = raw_data["retained"]

        if "broker_address" in raw_data:
            metadata["mqtt_broker"] = raw_data["broker_address"]

        return metadata
