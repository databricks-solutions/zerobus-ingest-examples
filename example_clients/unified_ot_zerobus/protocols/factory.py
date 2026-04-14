"""Protocol client factory."""
from __future__ import annotations

from typing import Any, Callable

from unified_connector.protocols.base import ProtocolClient, ProtocolRecord, ProtocolType
from unified_connector.protocols.mqtt_client import MQTTClient
from unified_connector.protocols.modbus_client import ModbusClient
from unified_connector.protocols.opcua_client import OPCUAClient


def create_protocol_client(
    protocol_type: str | ProtocolType,
    source_name: str,
    endpoint: str,
    config: dict[str, Any],
    on_record: Callable[[ProtocolRecord], None],
    on_stats: Callable[[dict[str, Any]], None] | None = None,
) -> ProtocolClient:
    """Create a protocol client based on the protocol type.

    Args:
        protocol_type: Protocol type (opcua, mqtt, modbus)
        source_name: Unique name for this source
        endpoint: Connection endpoint/URL
        config: Protocol-specific configuration
        on_record: Callback for received records
        on_stats: Optional callback for stats updates

    Returns:
        Protocol client instance

    Raises:
        ValueError: If protocol type is not supported
    """
    if isinstance(protocol_type, str):
        protocol_type = protocol_type.lower()

    if protocol_type in (ProtocolType.OPCUA, "opcua", "opc-ua", "opc_ua"):
        return OPCUAClient(source_name, endpoint, config, on_record, on_stats)
    elif protocol_type in (ProtocolType.MQTT, "mqtt"):
        return MQTTClient(source_name, endpoint, config, on_record, on_stats)
    elif protocol_type in (ProtocolType.MODBUS, "modbus"):
        return ModbusClient(source_name, endpoint, config, on_record, on_stats)
    else:
        raise ValueError(f"Unsupported protocol type: {protocol_type}")


def detect_protocol_type(endpoint: str) -> ProtocolType:
    """Auto-detect protocol type from endpoint URL.

    Args:
        endpoint: Connection endpoint/URL

    Returns:
        Detected protocol type

    Raises:
        ValueError: If protocol cannot be detected
    """
    endpoint_lower = endpoint.lower().strip()

    if endpoint_lower.startswith("opc.tcp://"):
        return ProtocolType.OPCUA
    elif endpoint_lower.startswith("mqtt://") or endpoint_lower.startswith("mqtts://"):
        return ProtocolType.MQTT
    elif endpoint_lower.startswith("modbus://") or endpoint_lower.startswith("modbusrtu://"):
        return ProtocolType.MODBUS
    else:
        # Try to infer from endpoint format
        if ":" in endpoint and not endpoint.startswith("/"):
            # Could be TCP (Modbus or MQTT)
            # Default to Modbus TCP for now
            return ProtocolType.MODBUS
        elif endpoint.startswith("/dev/") or endpoint.startswith("COM"):
            # Serial port, likely Modbus RTU
            return ProtocolType.MODBUS
        else:
            raise ValueError(f"Cannot detect protocol type from endpoint: {endpoint}")
