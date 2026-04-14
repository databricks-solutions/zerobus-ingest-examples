"""Modbus specific normalizer implementation."""
from __future__ import annotations

import logging
import struct
from typing import Any

from unified_connector.normalizer.base_normalizer import BaseNormalizer
from unified_connector.normalizer.tag_schema import NormalizedTag, TagQuality

logger = logging.getLogger(__name__)


class ModbusNormalizer(BaseNormalizer):
    """
    Normalizer for Modbus protocol data.

    Expected input format:
    {
        "device_address": "192.168.1.100",
        "register_address": 40001,
        "register_count": 2,
        "raw_registers": [3125, 0],
        "timestamp": "2025-01-18T14:23:45.123Z",
        "success": true,
        "exception_code": null,
        "timeout": false,
        "config": {
            "data_type": "float32",
            "scale_factor": 0.1,
            "byte_order": "big",
            "word_order": "big",
            "engineering_units": "bar",
            "tag_name": "hydraulic_pressure",
            "site_id": "columbus",
            "line_id": "line3",
            "equipment_id": "press1"
        }
    }
    """

    def _get_protocol_name(self) -> str:
        return "modbus"

    def normalize(self, raw_data: dict[str, Any]) -> NormalizedTag:
        """
        Convert Modbus data to normalized tag.

        Args:
            raw_data: Raw Modbus data

        Returns:
            NormalizedTag instance
        """
        try:
            # Extract config
            config = raw_data.get("config", {})

            # Extract device and register info
            device_address = raw_data.get("device_address", "")
            register_address = raw_data.get("register_address", 0)
            source_identifier = f"{device_address}:{register_address}"

            # Extract raw registers
            raw_registers = raw_data.get("raw_registers", [])

            # Transform registers based on data type
            data_type = config.get("data_type", "int16")
            byte_order = config.get("byte_order", "big")
            word_order = config.get("word_order", "big")

            value = self._transform_registers(
                raw_registers,
                data_type,
                byte_order,
                word_order
            )

            # Apply scale factor if configured
            scale_factor = config.get("scale_factor")
            if scale_factor is not None and isinstance(value, (int, float)):
                value = value * scale_factor

            # Extract timestamp
            timestamp = self._extract_timestamp(raw_data)

            # Map quality
            success = raw_data.get("success", False)
            exception_code = raw_data.get("exception_code")
            timeout = raw_data.get("timeout", False)

            quality = self.quality_mapper.map_modbus_quality(success, exception_code, timeout)

            # Determine data type
            tag_data_type = self._determine_data_type(value)

            # Extract engineering units
            engineering_units = config.get("engineering_units")

            # Extract context from config
            context = self._extract_context(raw_data)

            # Use configured tag_name if available
            if "tag_name" in config:
                context["signal_type"] = config["tag_name"]
            else:
                context["signal_type"] = f"register_{register_address}"

            # Build tag path
            tag_path = self._build_tag_path(source_identifier, context)

            # Generate tag ID
            tag_id = self._generate_tag_id(tag_path)

            # Build metadata
            metadata = self._build_metadata(raw_data)

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
                source_address=device_address,
                site_id=context.get("site_id"),
                line_id=context.get("line_id"),
                equipment_id=context.get("equipment_id"),
                signal_type=context.get("signal_type"),
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Error normalizing Modbus data: {e}", exc_info=True)
            raise

    def _transform_registers(
        self,
        registers: list[int],
        data_type: str,
        byte_order: str,
        word_order: str
    ) -> Any:
        """
        Transform raw Modbus registers to actual value.

        Args:
            registers: List of 16-bit register values
            data_type: Data type (int16, int32, uint32, float32, etc.)
            byte_order: Byte order (big, little)
            word_order: Word order for multi-register (big, little)

        Returns:
            Transformed value
        """
        if not registers:
            return None

        try:
            data_type = data_type.lower()

            if data_type == "int16":
                # Single 16-bit signed integer
                value = registers[0]
                # Convert unsigned to signed
                if value > 32767:
                    value -= 65536
                return value

            elif data_type == "uint16":
                # Single 16-bit unsigned integer
                return registers[0]

            elif data_type == "int32":
                # 32-bit signed integer from 2 registers
                if len(registers) < 2:
                    return None
                return self._registers_to_int32(registers[:2], byte_order, word_order)

            elif data_type == "uint32":
                # 32-bit unsigned integer from 2 registers
                if len(registers) < 2:
                    return None
                return self._registers_to_uint32(registers[:2], byte_order, word_order)

            elif data_type == "float32":
                # 32-bit float from 2 registers
                if len(registers) < 2:
                    return None
                return self._registers_to_float32(registers[:2], byte_order, word_order)

            elif data_type == "bool":
                # Boolean from first register
                return bool(registers[0])

            else:
                logger.warning(f"Unknown data type '{data_type}', returning raw registers")
                return registers

        except Exception as e:
            logger.error(f"Error transforming registers: {e}")
            return registers

    def _registers_to_int32(
        self,
        registers: list[int],
        byte_order: str,
        word_order: str
    ) -> int:
        """Convert 2 registers to 32-bit signed integer."""
        # Apply word order
        if word_order.lower() == "little":
            registers = [registers[1], registers[0]]

        # Pack as bytes
        if byte_order.lower() == "little":
            byte_data = struct.pack('<HH', registers[0], registers[1])
            return struct.unpack('<i', byte_data)[0]
        else:
            byte_data = struct.pack('>HH', registers[0], registers[1])
            return struct.unpack('>i', byte_data)[0]

    def _registers_to_uint32(
        self,
        registers: list[int],
        byte_order: str,
        word_order: str
    ) -> int:
        """Convert 2 registers to 32-bit unsigned integer."""
        # Apply word order
        if word_order.lower() == "little":
            registers = [registers[1], registers[0]]

        # Pack as bytes
        if byte_order.lower() == "little":
            byte_data = struct.pack('<HH', registers[0], registers[1])
            return struct.unpack('<I', byte_data)[0]
        else:
            byte_data = struct.pack('>HH', registers[0], registers[1])
            return struct.unpack('>I', byte_data)[0]

    def _registers_to_float32(
        self,
        registers: list[int],
        byte_order: str,
        word_order: str
    ) -> float:
        """Convert 2 registers to 32-bit float."""
        # Apply word order
        if word_order.lower() == "little":
            registers = [registers[1], registers[0]]

        # Pack as bytes
        if byte_order.lower() == "little":
            byte_data = struct.pack('<HH', registers[0], registers[1])
            return struct.unpack('<f', byte_data)[0]
        else:
            byte_data = struct.pack('>HH', registers[0], registers[1])
            return struct.unpack('>f', byte_data)[0]

    def _extract_protocol_metadata(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """
        Extract Modbus specific metadata.

        Args:
            raw_data: Raw Modbus data

        Returns:
            Protocol-specific metadata
        """
        metadata = {}

        # Add Modbus specific fields
        if "register_address" in raw_data:
            metadata["modbus_register_address"] = raw_data["register_address"]

        if "device_address" in raw_data:
            metadata["modbus_device_address"] = raw_data["device_address"]

        if "function_code" in raw_data:
            metadata["modbus_function_code"] = raw_data["function_code"]

        if "exception_code" in raw_data and raw_data["exception_code"] is not None:
            metadata["modbus_exception_code"] = raw_data["exception_code"]

        if "unit_id" in raw_data:
            metadata["modbus_unit_id"] = raw_data["unit_id"]

        return metadata
