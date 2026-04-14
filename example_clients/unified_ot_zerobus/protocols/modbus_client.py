"""Modbus protocol client implementation."""
from __future__ import annotations

import asyncio
import time
from typing import Any, Callable

try:
    from pymodbus.client import AsyncModbusTcpClient, AsyncModbusSerialClient
    from pymodbus.exceptions import ModbusException
except ImportError:
    AsyncModbusTcpClient = None  # type: ignore
    AsyncModbusSerialClient = None  # type: ignore
    ModbusException = Exception  # type: ignore

from unified_connector.protocols.base import (
    ProtocolClient,
    ProtocolRecord,
    ProtocolTestResult,
    ProtocolType,
)


class ModbusClient(ProtocolClient):
    """Modbus TCP/RTU protocol client with polling and backpressure handling."""

    def __init__(
        self,
        source_name: str,
        endpoint: str,
        config: dict[str, Any],
        on_record: Callable[[ProtocolRecord], None],
        on_stats: Callable[[dict[str, Any]], None] | None = None,
    ):
        if AsyncModbusTcpClient is None:
            raise ImportError("pymodbus package required for Modbus support. Install with: pip install pymodbus")

        super().__init__(source_name, endpoint, config, on_record, on_stats)

        # Parse Modbus endpoint
        self._parse_endpoint()

        # Modbus-specific config
        self.unit_id = int(config.get("unit_id", 1))  # Slave address
        self.poll_interval_ms = int(config.get("poll_interval_ms", 1000))
        self.timeout = float(config.get("timeout", 3.0))

        # Register configuration
        # Format: [{"type": "holding", "address": 0, "count": 10, "name": "temperature"}]
        self.registers = config.get("registers", [])
        if not self.registers:
            # Default: read first 10 holding registers
            self.registers = [{"type": "holding", "address": 0, "count": 10}]

        self._client: Any = None
        self._polls_completed = 0

        # Normalization support
        self._normalization_enabled = config.get("normalization_enabled", False)
        self._normalizer = None
        if self._normalization_enabled:
            try:
                from unified_connector.normalizer import get_normalization_manager
                self._norm_manager = get_normalization_manager()
                if self._norm_manager.is_enabled():
                    self._normalizer = self._norm_manager.get_normalizer("modbus")
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"Could not initialize normalizer: {e}")

    def _parse_endpoint(self) -> None:
        """Parse Modbus endpoint."""
        endpoint = self.endpoint.strip()

        # Determine protocol
        if endpoint.startswith("modbus://"):
            self.transport = "tcp"
            endpoint = endpoint[9:]
        elif endpoint.startswith("modbusrtu://"):
            self.transport = "rtu"
            endpoint = endpoint[12:]
        elif ":" in endpoint and not endpoint.startswith("/"):
            # Looks like host:port, assume TCP
            self.transport = "tcp"
        else:
            # Assume serial port
            self.transport = "rtu"

        if self.transport == "tcp":
            # Parse host:port
            if ":" in endpoint:
                self.host, port_str = endpoint.rsplit(":", 1)
                try:
                    self.port = int(port_str)
                except ValueError:
                    self.port = 502
                    self.host = endpoint
            else:
                self.host = endpoint
                self.port = 502
        else:
            # Serial port
            self.port_name = endpoint
            self.baudrate = int(self.config.get("baudrate", 9600))
            self.parity = self.config.get("parity", "N")
            self.stopbits = int(self.config.get("stopbits", 1))
            self.bytesize = int(self.config.get("bytesize", 8))

    @property
    def protocol_type(self) -> ProtocolType:
        return ProtocolType.MODBUS

    async def connect(self) -> None:
        """Establish Modbus connection."""
        if self._client is not None:
            return

        if self.transport == "tcp":
            self._client = AsyncModbusTcpClient(
                host=self.host,
                port=self.port,
                timeout=self.timeout,
            )
        else:
            self._client = AsyncModbusSerialClient(
                port=self.port_name,
                baudrate=self.baudrate,
                parity=self.parity,
                stopbits=self.stopbits,
                bytesize=self.bytesize,
                timeout=self.timeout,
            )

        await self._client.connect()

        if not self._client.connected:
            raise RuntimeError(f"Failed to connect to Modbus {self.transport} at {self.endpoint}")

    async def disconnect(self) -> None:
        """Disconnect from Modbus device."""
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            finally:
                self._client = None

    async def subscribe(self) -> None:
        """Poll Modbus registers at configured interval."""
        if self._client is None:
            raise RuntimeError("Not connected")

        poll_interval_s = self.poll_interval_ms / 1000.0

        while not self._stop_evt.is_set():
            try:
                # Poll all configured registers
                for reg_config in self.registers:
                    if self._stop_evt.is_set():
                        break

                    records = await self._poll_register(reg_config)
                    for record in records:
                        # Update last data time
                        self._last_data_time = time.time()

                        # Check if normalization is enabled
                        if self._normalizer:
                            # Create raw data for normalizer
                            raw_data = self._create_raw_data_from_record(record, reg_config)
                            try:
                                normalized = self._normalizer.normalize(raw_data)
                                self.on_record(normalized.to_dict())
                            except Exception as norm_error:
                                # Normalization failed, fall back to raw mode
                                self._emit_stats({
                                    "normalization_error": f"{type(norm_error).__name__}: {norm_error}",
                                    "address": record.metadata.get("address"),
                                })
                                # Send raw record as fallback
                                self.on_record(record)
                        else:
                            # Raw mode (existing behavior)
                            self.on_record(record)

                self._polls_completed += 1

                if self._polls_completed % 10 == 0:
                    self._emit_stats({
                        "polls_completed": self._polls_completed,
                        "last_poll_time_ms": int(time.time() * 1000),
                    })

                # Wait for next poll interval
                try:
                    await asyncio.wait_for(
                        self._stop_evt.wait(),
                        timeout=poll_interval_s
                    )
                    # Stop was requested
                    break
                except asyncio.TimeoutError:
                    # Continue to next poll
                    pass

            except Exception as e:
                self._emit_stats({
                    "poll_error": f"{type(e).__name__}: {e}",
                })
                # Don't crash on poll errors, continue polling
                await asyncio.sleep(poll_interval_s)

    async def _poll_register(self, reg_config: dict[str, Any]) -> list[ProtocolRecord]:
        """Poll a single register configuration."""
        records: list[ProtocolRecord] = []

        reg_type = reg_config.get("type", "holding")
        address = int(reg_config.get("address", 0))
        count = int(reg_config.get("count", 1))
        name_prefix = reg_config.get("name", f"{reg_type}_reg")
        scale = float(reg_config.get("scale", 1.0))
        offset = float(reg_config.get("offset", 0.0))

        try:
            # Read registers based on type
            if reg_type == "holding":
                result = await self._client.read_holding_registers(address, count, slave=self.unit_id)
            elif reg_type == "input":
                result = await self._client.read_input_registers(address, count, slave=self.unit_id)
            elif reg_type == "coil":
                result = await self._client.read_coils(address, count, slave=self.unit_id)
            elif reg_type == "discrete":
                result = await self._client.read_discrete_inputs(address, count, slave=self.unit_id)
            else:
                self._emit_stats({"unknown_register_type": reg_type})
                return records

            if result.isError():
                self._emit_stats({
                    "register_read_error": str(result),
                    "address": address,
                    "type": reg_type,
                })
                return records

            # Extract values
            if reg_type in ["holding", "input"]:
                values = result.registers
            else:
                values = result.bits[:count]

            # Create records for each value
            event_time_ms = int(time.time() * 1000)
            for i, raw_value in enumerate(values):
                # Apply scaling and offset
                if isinstance(raw_value, bool):
                    value = int(raw_value)
                    value_num = float(value)
                    value_type = "bool"
                else:
                    value_num = (float(raw_value) * scale) + offset
                    value = value_num
                    value_type = "numeric"

                # Generate topic/path
                topic = f"{name_prefix}/{reg_type}/{address + i}"

                record = ProtocolRecord(
                    event_time_ms=event_time_ms,
                    source_name=self.source_name,
                    endpoint=self.endpoint,
                    protocol_type=self.protocol_type,
                    topic_or_path=topic,
                    value=value,
                    value_type=value_type,
                    value_num=value_num,
                    metadata={
                        "register_type": reg_type,
                        "address": address + i,
                        "unit_id": self.unit_id,
                        "raw_value": raw_value,
                        "scale": scale,
                        "offset": offset,
                    },
                    status_code=0,
                    status="Good",
                )
                records.append(record)

        except ModbusException as e:
            self._emit_stats({
                "modbus_exception": f"{type(e).__name__}: {e}",
                "address": address,
                "type": reg_type,
            })
        except Exception as e:
            self._emit_stats({
                "poll_error": f"{type(e).__name__}: {e}",
                "address": address,
                "type": reg_type,
            })

        return records

    def _create_raw_data_from_record(
        self,
        record: ProtocolRecord,
        reg_config: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Create raw data dict for normalizer from ProtocolRecord.

        Args:
            record: ProtocolRecord from poll
            reg_config: Register configuration dict

        Returns:
            Dictionary with normalized format expected by ModbusNormalizer
        """
        # Extract device address from endpoint
        device_address = self.host if hasattr(self, 'host') else self.endpoint

        # Get register address from metadata
        register_address = record.metadata.get("address", 0)

        # Determine if read was successful
        success = record.status_code == 0

        return {
            "device_address": device_address,
            "register_address": register_address,
            "register_count": reg_config.get("count", 1),
            "raw_registers": [record.metadata.get("raw_value", 0)],
            "timestamp": record.event_time_ms,
            "success": success,
            "exception_code": None if success else record.status_code,
            "timeout": False,
            "config": {
                "data_type": "int16",  # Default, could be configured
                "scale_factor": reg_config.get("scale", 1.0),
                "engineering_units": reg_config.get("units"),
                "tag_name": reg_config.get("name"),
                **self.config,  # Include all config for context
            }
        }

    async def test_connection(self) -> ProtocolTestResult:
        """Test Modbus connectivity."""
        start_time = time.time()
        error = None
        server_info: dict[str, Any] = {}

        try:
            await self.connect()

            # Try to read a single register to verify connection
            if self.transport == "tcp":
                server_info = {
                    "host": self.host,
                    "port": self.port,
                    "transport": "tcp",
                    "unit_id": self.unit_id,
                }
            else:
                server_info = {
                    "port": self.port_name,
                    "baudrate": self.baudrate,
                    "transport": "rtu",
                    "unit_id": self.unit_id,
                }

            # Test read
            result = await self._client.read_holding_registers(0, 1, slave=self.unit_id)
            if not result.isError():
                server_info["test_read_success"] = True
            else:
                server_info["test_read_warning"] = "Could not read test register (device may not support)"

            await self.disconnect()
            ok = True

        except Exception as e:
            ok = False
            error = f"{type(e).__name__}: {e}"
        finally:
            duration_ms = int((time.time() - start_time) * 1000)

        return ProtocolTestResult(
            ok=ok,
            endpoint=self.endpoint,
            protocol_type=self.protocol_type,
            duration_ms=duration_ms,
            server_info=server_info if ok else None,
            error=error,
        )
