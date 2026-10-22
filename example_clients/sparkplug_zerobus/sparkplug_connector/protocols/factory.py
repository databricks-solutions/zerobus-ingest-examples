"""Protocol client factory."""
from __future__ import annotations

from typing import Any, Callable

from sparkplug_connector.protocols.base import ProtocolClient, ProtocolType
from sparkplug_connector.protocols.sparkplug_client import SparkplugClient


def create_protocol_client(
    protocol_type: str | ProtocolType,
    source_name: str,
    endpoint: str,
    config: dict[str, Any],
    on_record: Callable[[Any], None],
    on_stats: Callable[[dict[str, Any]], None] | None = None,
) -> ProtocolClient:
    """The client of a source's protocol (``sparkplug``; ``sparkplug_b`` too, any case).

    Raises:
        ValueError: If protocol type is not supported
    """
    if isinstance(protocol_type, str):
        protocol_type = protocol_type.lower()

    if protocol_type in (ProtocolType.SPARKPLUG, "sparkplug", "sparkplug_b"):
        return SparkplugClient(source_name, endpoint, config, on_record, on_stats)
    raise ValueError(f"Unsupported protocol type: {protocol_type}")
