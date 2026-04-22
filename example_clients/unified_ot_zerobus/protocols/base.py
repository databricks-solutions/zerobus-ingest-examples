"""Protocol abstraction layer for various industrial protocols."""
from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable


class ProtocolType(str, Enum):
    """Supported protocol types."""
    OPCUA = "opcua"
    MQTT = "mqtt"
    MODBUS = "modbus"


@dataclass
class ProtocolRecord:
    """Generic record for any protocol data."""
    event_time_ms: int
    source_name: str
    endpoint: str
    protocol_type: ProtocolType

    # Generic fields for all protocols
    topic_or_path: str  # MQTT topic, OPC UA browse path, or Modbus register address
    value: Any
    value_type: str
    value_num: float | None = None

    # Protocol-specific metadata (stored as JSON-compatible dict)
    metadata: dict[str, Any] | None = None

    # Quality/status information
    status_code: int = 0
    status: str = "Good"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "event_time": int(self.event_time_ms) * 1000,  # Convert ms to microseconds for UC TIMESTAMP
            "ingest_time": int(time.time() * 1_000_000),  # Microseconds for UC TIMESTAMP
            "source_name": self.source_name,
            "endpoint": self.endpoint,
            "protocol_type": self.protocol_type.value,
            "topic_or_path": self.topic_or_path,
            "value": self.value,
            "value_type": self.value_type,
            "value_num": self.value_num,
            "metadata": self.metadata or {},
            "status_code": self.status_code,
            "status": self.status,
        }


@dataclass
class ConnectionStatus:
    """Connection status for a protocol client."""
    connected: bool = False
    last_connect_time_ms: int | None = None
    last_disconnect_time_ms: int | None = None
    reconnect_attempts: int = 0
    last_error: str | None = None


@dataclass
class ProtocolTestResult:
    """Test connection result."""
    ok: bool
    endpoint: str
    protocol_type: ProtocolType
    duration_ms: int | None = None
    server_info: dict[str, Any] | None = None
    error: str | None = None


class ProtocolClient(ABC):
    """Abstract base class for protocol clients."""

    def __init__(
        self,
        source_name: str,
        endpoint: str,
        config: dict[str, Any],
        on_record: Callable[[ProtocolRecord], None],
        on_stats: Callable[[dict[str, Any]], None] | None = None,
    ):
        self.source_name = source_name
        self.endpoint = endpoint
        self.config = config
        self.on_record = on_record
        self.on_stats = on_stats
        self._status = ConnectionStatus()
        self._stop_evt = asyncio.Event()
        self._last_data_time: float | None = None  # Track last data received time

        # Reconnection settings
        self.reconnect_enabled = bool(config.get("reconnect_enabled", True))
        self.reconnect_delay_ms = int(config.get("reconnect_delay_ms", 5000))
        self.reconnect_max_attempts = int(config.get("reconnect_max_attempts", 0))  # 0 = infinite
        self.reconnect_backoff_multiplier = float(config.get("reconnect_backoff_multiplier", 2.0))
        self.reconnect_max_delay_ms = int(config.get("reconnect_max_delay_ms", 60000))

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the protocol endpoint."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the protocol endpoint."""
        pass

    @abstractmethod
    async def subscribe(self) -> None:
        """Subscribe to data updates from the protocol endpoint."""
        pass

    @abstractmethod
    async def test_connection(self) -> ProtocolTestResult:
        """Test connectivity without subscribing."""
        pass

    @property
    @abstractmethod
    def protocol_type(self) -> ProtocolType:
        """Return the protocol type."""
        pass

    def get_status(self) -> ConnectionStatus:
        """Get current connection status."""
        return self._status

    def request_stop(self) -> None:
        """Request the client to stop."""
        self._stop_evt.set()

    def _emit_stats(self, delta: dict[str, Any]) -> None:
        """Emit stats update."""
        if self.on_stats:
            self.on_stats(delta)

    async def run_with_reconnect(self) -> None:
        """Main loop with automatic reconnection logic."""
        reconnect_delay = self.reconnect_delay_ms / 1000.0

        while not self._stop_evt.is_set():
            try:
                # Attempt connection
                await self.connect()
                self._status.connected = True
                self._status.last_connect_time_ms = int(time.time() * 1000)
                self._status.reconnect_attempts = 0
                self._status.last_error = None

                self._emit_stats({
                    "connected": True,
                    "last_connect_time_ms": self._status.last_connect_time_ms,
                })

                # Start subscription
                await self.subscribe()

            except asyncio.CancelledError:
                break
            except Exception as e:
                error_msg = f"{type(e).__name__}: {e}"
                self._status.connected = False
                self._status.last_disconnect_time_ms = int(time.time() * 1000)
                self._status.last_error = error_msg
                self._status.reconnect_attempts += 1

                self._emit_stats({
                    "connected": False,
                    "last_error": error_msg,
                    "reconnect_attempts": self._status.reconnect_attempts,
                })

                # Check if reconnection is enabled and within max attempts
                if not self.reconnect_enabled:
                    raise

                if self.reconnect_max_attempts > 0 and self._status.reconnect_attempts >= self.reconnect_max_attempts:
                    raise RuntimeError(f"Max reconnection attempts ({self.reconnect_max_attempts}) exceeded")

                # Exponential backoff
                current_delay = min(
                    reconnect_delay * (self.reconnect_backoff_multiplier ** (self._status.reconnect_attempts - 1)),
                    self.reconnect_max_delay_ms / 1000.0
                )

                # Wait before reconnecting
                try:
                    await asyncio.wait_for(
                        self._stop_evt.wait(),
                        timeout=current_delay
                    )
                    # Stop was requested during wait
                    break
                except asyncio.TimeoutError:
                    # Continue to next reconnection attempt
                    pass
            finally:
                # Always try to disconnect cleanly
                try:
                    await self.disconnect()
                except Exception:
                    pass
