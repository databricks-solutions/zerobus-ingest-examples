"""Normalization manager for loading config and creating normalizers."""
from __future__ import annotations

import logging
import yaml
from pathlib import Path
from typing import Any, Optional

from unified_connector.normalizer.opcua_normalizer import OPCUANormalizer
from unified_connector.normalizer.modbus_normalizer import ModbusNormalizer
from unified_connector.normalizer.mqtt_normalizer import MQTTNormalizer
from unified_connector.normalizer.base_normalizer import BaseNormalizer

logger = logging.getLogger(__name__)


class NormalizationManager:
    """
    Manages normalization configuration and normalizer instances.

    Handles loading normalization config and creating protocol-specific normalizers.
    """

    def __init__(self, config_path: str | Path | None = None):
        """
        Initialize normalization manager.

        Args:
            config_path: Path to normalization_config.yaml (optional)
        """
        self.config: dict[str, Any] = {}
        self.normalizers: dict[str, BaseNormalizer] = {}
        self.enabled = False

        # Load configuration
        if config_path:
            self.load_config(config_path)
        else:
            # Try default locations
            default_paths = [
                Path("config/normalization_config.yaml"),
                Path("unified_connector/config/normalization_config.yaml"),
                Path("/app/config/normalization_config.yaml"),
                Path("/app/unified_connector/config/normalization_config.yaml"),
            ]

            for path in default_paths:
                if path.exists():
                    self.load_config(path)
                    break

    def load_config(self, config_path: str | Path):
        """
        Load normalization configuration from YAML file.

        Args:
            config_path: Path to normalization_config.yaml
        """
        config_path = Path(config_path)

        if not config_path.exists():
            logger.warning(f"Normalization config not found: {config_path}")
            return

        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f) or {}

            logger.info(f"Loaded normalization config from {config_path}")

            # Check if normalization is enabled
            self.enabled = self.config.get("features", {}).get("normalization_enabled", False)

            # Also check mode setting
            mode = self.config.get("mode", "raw")
            if mode == "normalized":
                self.enabled = True

            logger.info(f"Normalization {'enabled' if self.enabled else 'disabled'}")

        except Exception as e:
            logger.error(f"Error loading normalization config: {e}")

    def is_enabled(self) -> bool:
        """
        Check if normalization is enabled.

        Returns:
            True if normalization is enabled
        """
        return self.enabled

    def set_enabled(self, enabled: bool):
        """
        Enable or disable normalization.

        Args:
            enabled: True to enable, False to disable
        """
        self.enabled = enabled
        if "features" not in self.config:
            self.config["features"] = {}
        self.config["features"]["normalization_enabled"] = enabled
        logger.info(f"Normalization {'enabled' if enabled else 'disabled'}")

    def get_normalizer(self, protocol: str) -> Optional[BaseNormalizer]:
        """
        Get normalizer for a protocol.

        Creates normalizer on first request and caches it.

        Args:
            protocol: Protocol name ("opcua", "modbus", "mqtt")

        Returns:
            Normalizer instance or None if normalization disabled
        """
        if not self.enabled:
            return None

        protocol_lower = protocol.lower()

        # Return cached normalizer if available
        if protocol_lower in self.normalizers:
            return self.normalizers[protocol_lower]

        # Create new normalizer
        try:
            normalizer = self._create_normalizer(protocol_lower)
            if normalizer:
                self.normalizers[protocol_lower] = normalizer
                logger.info(f"Created {protocol} normalizer")
            return normalizer

        except Exception as e:
            logger.error(f"Error creating {protocol} normalizer: {e}")
            return None

    def _create_normalizer(self, protocol: str) -> Optional[BaseNormalizer]:
        """
        Create normalizer instance for a protocol.

        Args:
            protocol: Protocol name

        Returns:
            Normalizer instance or None
        """
        if protocol == "opcua":
            return OPCUANormalizer(self.config)
        elif protocol == "modbus":
            return ModbusNormalizer(self.config)
        elif protocol == "mqtt":
            return MQTTNormalizer(self.config)
        else:
            logger.warning(f"Unknown protocol: {protocol}")
            return None

    def get_output_table(self, normalized: bool = True) -> str:
        """
        Get target table name for output.

        Args:
            normalized: True for normalized table, False for raw table

        Returns:
            Table name
        """
        output_config = self.config.get("output", {})

        if normalized:
            return output_config.get("normalized_table", "main.iot_bronze.normalized_tags")
        else:
            return output_config.get("raw_table", "main.iot_bronze.raw_protocol_data")

    def should_write_both(self) -> bool:
        """
        Check if both raw and normalized data should be written.

        Returns:
            True if dual-write mode is enabled
        """
        return self.config.get("output", {}).get("write_both", False)

    def get_batch_size(self) -> int:
        """Get batch size for writing."""
        return self.config.get("output", {}).get("batch_size", 100)

    def get_flush_interval(self) -> float:
        """Get flush interval in seconds."""
        return float(self.config.get("output", {}).get("flush_interval_seconds", 5.0))


# Global singleton instance
_normalization_manager: Optional[NormalizationManager] = None


def get_normalization_manager(config_path: str | Path | None = None) -> NormalizationManager:
    """
    Get global normalization manager instance.

    Args:
        config_path: Path to normalization config (only used on first call)

    Returns:
        NormalizationManager instance
    """
    global _normalization_manager

    if _normalization_manager is None:
        _normalization_manager = NormalizationManager(config_path)

    return _normalization_manager
