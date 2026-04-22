"""Quality code mapping for different protocols."""
from __future__ import annotations

import logging
from typing import Any

from unified_connector.normalizer.tag_schema import TagQuality

logger = logging.getLogger(__name__)


class QualityMapper:
    """Maps protocol-specific quality codes to unified TagQuality enum."""

    # OPC-UA quality ranges (32-bit status code)
    OPCUA_GOOD_MAX = 0x3FFFFFFF
    OPCUA_UNCERTAIN_MAX = 0x7FFFFFFF
    # Anything >= 0x80000000 is BAD

    @staticmethod
    def map_opcua_quality(status_code: int) -> TagQuality:
        """
        Map OPC-UA status code to TagQuality.

        OPC-UA uses 32-bit status codes:
        - 0x00000000 - 0x3FFFFFFF: Good
        - 0x40000000 - 0x7FFFFFFF: Uncertain
        - 0x80000000 - 0xFFFFFFFF: Bad

        Args:
            status_code: OPC-UA status code

        Returns:
            TagQuality enum
        """
        if status_code <= QualityMapper.OPCUA_GOOD_MAX:
            return TagQuality.GOOD
        elif status_code <= QualityMapper.OPCUA_UNCERTAIN_MAX:
            return TagQuality.UNCERTAIN
        else:
            return TagQuality.BAD

    @staticmethod
    def map_modbus_quality(
        success: bool,
        exception_code: int | None = None,
        timeout: bool = False
    ) -> TagQuality:
        """
        Map Modbus transaction status to TagQuality.

        Modbus has no native quality codes, so we infer quality from:
        - Transaction success/failure
        - Exception codes
        - Timeout status

        Args:
            success: Whether transaction succeeded
            exception_code: Modbus exception code (1-8) if error occurred
            timeout: Whether transaction timed out

        Returns:
            TagQuality enum
        """
        if success and not exception_code and not timeout:
            return TagQuality.GOOD
        else:
            # Any error condition = BAD quality
            return TagQuality.BAD

    @staticmethod
    def map_mqtt_quality(
        retained: bool = False,
        age_seconds: float = 0.0,
        age_threshold: float = 300.0
    ) -> TagQuality:
        """
        Map MQTT message characteristics to TagQuality.

        MQTT has no quality codes, so we use heuristics:
        - Retained messages might be stale → UNCERTAIN
        - Messages older than threshold → UNCERTAIN
        - Otherwise → GOOD

        Args:
            retained: Whether message was retained by broker
            age_seconds: How old the message is (seconds)
            age_threshold: Maximum age for GOOD quality (default 5 minutes)

        Returns:
            TagQuality enum
        """
        if retained:
            # Retained messages are potentially stale
            return TagQuality.UNCERTAIN

        if age_seconds > age_threshold:
            # Message too old
            return TagQuality.UNCERTAIN

        return TagQuality.GOOD

    @staticmethod
    def map_quality(protocol: str, quality_data: dict[str, Any]) -> TagQuality:
        """
        Dispatcher method that routes to protocol-specific mapper.

        Args:
            protocol: Protocol name ("opcua", "modbus", "mqtt")
            quality_data: Protocol-specific quality information

        Returns:
            TagQuality enum

        Examples:
            >>> # OPC-UA
            >>> map_quality("opcua", {"status_code": 0})
            TagQuality.GOOD

            >>> # Modbus
            >>> map_quality("modbus", {"success": True, "exception_code": None})
            TagQuality.GOOD

            >>> # MQTT
            >>> map_quality("mqtt", {"retained": False, "age_seconds": 10})
            TagQuality.GOOD
        """
        protocol_lower = protocol.lower()

        try:
            if protocol_lower == "opcua":
                status_code = quality_data.get("status_code", 0)
                return QualityMapper.map_opcua_quality(status_code)

            elif protocol_lower == "modbus":
                success = quality_data.get("success", False)
                exception_code = quality_data.get("exception_code")
                timeout = quality_data.get("timeout", False)
                return QualityMapper.map_modbus_quality(success, exception_code, timeout)

            elif protocol_lower == "mqtt":
                retained = quality_data.get("retained", False)
                age_seconds = quality_data.get("age_seconds", 0.0)
                age_threshold = quality_data.get("age_threshold", 300.0)
                return QualityMapper.map_mqtt_quality(retained, age_seconds, age_threshold)

            else:
                logger.warning(f"Unknown protocol '{protocol}', defaulting to UNCERTAIN quality")
                return TagQuality.UNCERTAIN

        except Exception as e:
            logger.error(f"Error mapping quality for protocol {protocol}: {e}")
            return TagQuality.UNCERTAIN
