"""Tag path generation from protocol-specific identifiers."""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)


class TagPathBuilder:
    """Builds hierarchical tag paths from protocol-specific identifiers."""

    def __init__(self, config: dict[str, Any]):
        """
        Initialize path builder with configuration.

        Args:
            config: Configuration dict with templates and patterns
        """
        self.config = config

        # Get templates
        self.default_template = config.get("default_template", "{site_id}/{line_id}/{equipment_id}/{signal_type}")
        self.templates = {
            "opcua": config.get("opcua_template", self.default_template),
            "modbus": config.get("modbus_template", self.default_template),
            "mqtt": config.get("mqtt_template", self.default_template),
        }

        # Get extraction patterns
        self.patterns = {
            "opcua": config.get("opcua_patterns", {}),
            "modbus": config.get("modbus_patterns", {}),
            "mqtt": config.get("mqtt_patterns", {}),
        }

        # Prevent log spam when templates don't match incoming identifiers.
        # We warn once per missing component per protocol/template.
        self._missing_component_warned: set[str] = set()

    def build_path(
        self,
        protocol: str,
        source_identifier: str,
        context: dict[str, Any]
    ) -> str:
        """
        Build hierarchical tag path.

        Args:
            protocol: Protocol name ("opcua", "modbus", "mqtt")
            source_identifier: Protocol-specific identifier
            context: Additional context for path building

        Returns:
            Normalized tag path (e.g., "columbus/line3/press1/hydraulic_pressure")
        """
        protocol_lower = protocol.lower()

        try:
            # Extract components from source identifier
            extracted = self._extract_components(protocol_lower, source_identifier)

            # Merge with provided context (context takes precedence)
            components = {**extracted, **context}

            # Get template for this protocol
            template = self.templates.get(protocol_lower, self.default_template)

            # Try to fill template
            try:
                path = template.format(**components)
            except KeyError as e:
                missing = str(e).strip("'")
                warn_key = f"{protocol_lower}:{missing}:{template}"
                if warn_key not in self._missing_component_warned:
                    self._missing_component_warned.add(warn_key)
                    logger.warning(f"Missing component {e} for template, using fallback path")
                path = self._build_fallback_path(protocol_lower, source_identifier, components)

            # Normalize the path
            path = self._normalize_path(path)

            return path

        except Exception as e:
            logger.error(f"Error building path for {protocol}:{source_identifier}: {e}")
            return self._build_fallback_path(protocol_lower, source_identifier, context)

    def _extract_components(self, protocol: str, source_identifier: str) -> dict[str, str]:
        """
        Extract path components from source identifier using regex patterns.

        Args:
            protocol: Protocol name
            source_identifier: Protocol-specific identifier

        Returns:
            Dictionary of extracted components
        """
        components = {}
        patterns = self.patterns.get(protocol, {})

        if not patterns:
            return components

        # Try each pattern
        for component_name, pattern in patterns.items():
            try:
                match = re.search(pattern, source_identifier, re.IGNORECASE)
                if match:
                    # Use first capturing group or full match
                    value = match.group(1) if match.groups() else match.group(0)
                    components[component_name] = self._sanitize_identifier(value)
            except Exception as e:
                logger.debug(f"Pattern '{pattern}' failed for {source_identifier}: {e}")

        # Protocol-specific extraction
        if protocol == "mqtt":
            # MQTT topics are already hierarchical
            parts = source_identifier.split("/")
            if len(parts) >= 4:
                components.setdefault("site_id", self._sanitize_identifier(parts[0]))
                components.setdefault("line_id", self._sanitize_identifier(parts[1]))
                components.setdefault("equipment_id", self._sanitize_identifier(parts[2]))
                components.setdefault("signal_type", self._sanitize_identifier(parts[3]))
            elif len(parts) >= 2:
                components.setdefault("signal_type", self._sanitize_identifier(parts[-1]))

        elif protocol == "opcua":
            # Try to extract from node ID or browse path
            # Pattern: ns=X;s=Path.To.Node or Objects/Path/To/Node
            if ";" in source_identifier:
                # Node ID format: ns=2;s=Line3.Machine1.Temperature
                parts = source_identifier.split(";s=")
                if len(parts) > 1:
                    node_path = parts[1]
                    path_parts = re.split(r'[./]', node_path)
                    if len(path_parts) >= 2:
                        components.setdefault("signal_type", self._sanitize_identifier(path_parts[-1]))
                        components.setdefault("equipment_id", self._sanitize_identifier(path_parts[-2]))
                        if len(path_parts) >= 3:
                            components.setdefault("line_id", self._sanitize_identifier(path_parts[-3]))

        elif protocol == "modbus":
            # Modbus has no inherent hierarchy, rely on config
            # Extract register address as signal identifier
            if "register_address" in source_identifier:
                components.setdefault("signal_type", f"register_{source_identifier}")

        return components

    def _sanitize_identifier(self, identifier: str) -> str:
        """
        Remove special characters and normalize identifier.

        Args:
            identifier: Raw identifier string

        Returns:
            Sanitized identifier (lowercase, underscores)
        """
        # Replace special chars with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', identifier)

        # Remove duplicate underscores
        sanitized = re.sub(r'_+', '_', sanitized)

        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')

        # Lowercase
        sanitized = sanitized.lower()

        return sanitized

    def _build_fallback_path(
        self,
        protocol: str,
        source_identifier: str,
        components: dict[str, str]
    ) -> str:
        """
        Build fallback path when template filling fails.

        Args:
            protocol: Protocol name
            source_identifier: Source identifier
            components: Available components

        Returns:
            Fallback path
        """
        # Use available components or defaults
        site = components.get("site_id", "unknown_site")
        signal = components.get("signal_type", self._sanitize_identifier(source_identifier))

        path = f"{site}/{protocol}/{signal}"
        return self._normalize_path(path)

    def _normalize_path(self, path: str) -> str:
        """
        Normalize path string.

        Args:
            path: Raw path

        Returns:
            Normalized path
        """
        # Remove duplicate slashes
        path = re.sub(r'/+', '/', path)

        # Remove leading/trailing slashes
        path = path.strip('/')

        # Lowercase
        path = path.lower()

        return path

    def generate_tag_id(self, tag_path: str, protocol: str) -> str:
        """
        Generate unique tag ID from path and protocol.

        Uses SHA256 hash to ensure consistency and uniqueness.

        Args:
            tag_path: Normalized tag path
            protocol: Protocol name

        Returns:
            16-character tag ID
        """
        # Create unique string combining protocol and path
        unique_str = f"{protocol}:{tag_path}"

        # Hash it
        hash_obj = hashlib.sha256(unique_str.encode('utf-8'))
        hash_hex = hash_obj.hexdigest()

        # Return first 16 characters
        return hash_hex[:16]
