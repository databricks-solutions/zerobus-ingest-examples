"""The source client interface the bridge drives (connect, subscribe, stop, disconnect)."""
from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable


class ProtocolType(str, Enum):
    """Supported protocol types."""
    SPARKPLUG = "sparkplug"   # Sparkplug B over MQTT: sparkplug_raw rows (protocols/sparkplug_client.py)


class ProtocolClient(ABC):
    """Abstract base class for source clients."""

    def __init__(
        self,
        source_name: str,
        endpoint: str,
        config: dict[str, Any],
        on_record: Callable[[Any], None],
        on_stats: Callable[[dict[str, Any]], None] | None = None,
    ):
        self.source_name = source_name
        self.endpoint = endpoint
        self.config = config
        self.on_record = on_record
        self.on_stats = on_stats

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the protocol endpoint."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the protocol endpoint."""

    @abstractmethod
    async def subscribe(self) -> None:
        """Subscribe to data updates from the protocol endpoint."""

    @property
    @abstractmethod
    def protocol_type(self) -> ProtocolType:
        """Return the protocol type."""

    async def stop(self) -> None:
        """Called by the bridge before disconnect() when the source stops, and after a connection error before
        it connects the same client again; nothing by default."""

    async def close(self) -> None:
        """Called by the bridge once the source has stopped for good (not between reconnects); nothing by default."""

    def _emit_stats(self, delta: dict[str, Any]) -> None:
        """Emit stats update."""
        if self.on_stats:
            self.on_stats(delta)
