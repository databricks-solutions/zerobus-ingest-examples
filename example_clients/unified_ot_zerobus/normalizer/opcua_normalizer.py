"""OPC-UA specific normalizer implementation."""
from __future__ import annotations

import logging
import re
from typing import Any

from unified_connector.normalizer.base_normalizer import BaseNormalizer
from unified_connector.normalizer.tag_schema import NormalizedTag, TagQuality

logger = logging.getLogger(__name__)


class OPCUANormalizer(BaseNormalizer):
    """
    Normalizer for OPC-UA protocol data.

    Expected input format:
    {
        "node_id": "ns=2;s=Line3.Machine1.Temperature",
        "value": {
            "value": 75.3,
            "source_timestamp": "2025-01-18T14:23:45.123Z",
            "server_timestamp": "2025-01-18T14:23:45.125Z",
            "status_code": 0
        },
        "browse_path": "Objects/Line3/Machine1/Temperature",
        "engineering_units": "celsius",
        "server_url": "opc.tcp://plc1:4840",
        "config": {
            "site_id": "columbus",
            "line_id": "line3",
            "equipment_id": "machine1"
        }
    }
    """

    def _get_protocol_name(self) -> str:
        return "opcua"

    def normalize(self, raw_data: dict[str, Any]) -> NormalizedTag:
        """
        Convert OPC-UA data to normalized tag.

        Args:
            raw_data: Raw OPC-UA data

        Returns:
            NormalizedTag instance
        """
        try:
            # Extract node ID
            node_id = raw_data.get("node_id", "")
            source_identifier = node_id

            # Extract value (OPC-UA typically has nested value structure)
            value_data = raw_data.get("value", {})
            if isinstance(value_data, dict):
                value = value_data.get("value")
                status_code = value_data.get("status_code", 0)

                # Prefer source_timestamp over server_timestamp
                timestamp_str = value_data.get("source_timestamp") or value_data.get("server_timestamp")
                if timestamp_str:
                    raw_data["timestamp"] = timestamp_str
            else:
                # Value is not nested
                value = value_data
                status_code = raw_data.get("status_code", 0)

            # Extract timestamp
            timestamp = self._extract_timestamp(raw_data)

            # Map quality
            quality = self.quality_mapper.map_opcua_quality(status_code)

            # Determine data type
            data_type = self._determine_data_type(value)

            # Extract engineering units
            engineering_units = raw_data.get("engineering_units")

            # Extract context from browse path and config
            context = self._extract_context_from_browse_path(raw_data)
            config_context = self._extract_context(raw_data)
            context.update(config_context)  # Config takes precedence

            # Build tag path
            tag_path = self._build_tag_path(source_identifier, context)

            # Generate tag ID
            tag_id = self._generate_tag_id(tag_path)

            # Source address
            source_address = raw_data.get("server_url", "")

            # Build metadata
            metadata = self._build_metadata(raw_data)

            return NormalizedTag(
                tag_id=tag_id,
                tag_path=tag_path,
                value=value,
                quality=quality,
                timestamp=timestamp,
                data_type=data_type,
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
            logger.error(f"Error normalizing OPC-UA data: {e}", exc_info=True)
            raise

    def _extract_context_from_browse_path(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """
        Extract context from OPC-UA browse path.

        Parse browse paths like:
        - "Objects/Columbus/Line3/Machine1/Temperature"
        - "Root/Site/Area/Equipment/Signal"

        Args:
            raw_data: Raw OPC-UA data

        Returns:
            Context dictionary
        """
        context = {}

        browse_path = raw_data.get("browse_path", "")
        if not browse_path:
            # Try to extract from node ID
            node_id = raw_data.get("node_id", "")
            if ";s=" in node_id:
                # Extract string identifier: ns=2;s=Line3.Machine1.Temperature
                browse_path = node_id.split(";s=")[1]

        if browse_path:
            # Split by / or .
            parts = re.split(r'[/.]', browse_path)

            # Filter out common root names
            parts = [p for p in parts if p and p.lower() not in ('objects', 'root', 'server')]

            if len(parts) >= 1:
                # Last part is likely the signal name
                context["signal_type"] = parts[-1].lower()

            if len(parts) >= 2:
                # Second to last is likely equipment
                context["equipment_id"] = parts[-2].lower()

            if len(parts) >= 3:
                # Third to last might be line/area
                context["line_id"] = parts[-3].lower()

            if len(parts) >= 4:
                # Fourth to last might be site
                context["site_id"] = parts[-4].lower()

        return context

    def _extract_protocol_metadata(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """
        Extract OPC-UA specific metadata.

        Args:
            raw_data: Raw OPC-UA data

        Returns:
            Protocol-specific metadata
        """
        metadata = {}

        # Add OPC-UA specific fields
        if "node_id" in raw_data:
            metadata["opcua_node_id"] = raw_data["node_id"]

        if "browse_path" in raw_data:
            metadata["opcua_browse_path"] = raw_data["browse_path"]

        if "display_name" in raw_data:
            metadata["opcua_display_name"] = raw_data["display_name"]

        # Extract status code from value if nested
        value_data = raw_data.get("value", {})
        if isinstance(value_data, dict) and "status_code" in value_data:
            metadata["opcua_status_code"] = value_data["status_code"]
        elif "status_code" in raw_data:
            metadata["opcua_status_code"] = raw_data["status_code"]

        return metadata
