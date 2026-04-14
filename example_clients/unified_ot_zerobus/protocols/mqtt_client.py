"""MQTT protocol client implementation with TLS/SSL support.

Security Features (NIS2 Article 21.2(h) compliant):
- TLS/SSL encryption (mqtts://)
- Certificate-based authentication (mutual TLS)
- CA certificate validation
- Username/password authentication
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable

try:
    import aiomqtt
except ImportError:
    aiomqtt = None  # type: ignore

from unified_connector.protocols.base import (
    ProtocolClient,
    ProtocolRecord,
    ProtocolTestResult,
    ProtocolType,
)
from unified_connector.protocols.mqtt_security import (
    MQTTSecurityConfig,
    MQTTSecurityManager,
    create_security_config_from_dict,
)


class MQTTClient(ProtocolClient):
    """MQTT protocol client with backpressure handling."""

    def __init__(
        self,
        source_name: str,
        endpoint: str,
        config: dict[str, Any],
        on_record: Callable[[ProtocolRecord], None],
        on_stats: Callable[[dict[str, Any]], None] | None = None,
    ):
        if aiomqtt is None:
            raise ImportError("aiomqtt package required for MQTT support. Install with: pip install aiomqtt")

        super().__init__(source_name, endpoint, config, on_record, on_stats)

        # Parse MQTT endpoint: mqtt://host:port or mqtts://host:port
        self._parse_endpoint()

        # MQTT-specific config
        self.topics = config.get("topics", ["#"])  # Default subscribe to all
        if isinstance(self.topics, str):
            self.topics = [self.topics]

        self.qos = int(config.get("qos", 1))
        self.client_id = config.get("client_id", f"dbx-{source_name}")
        self.username = config.get("username")
        self.password = config.get("password")
        self.clean_session = bool(config.get("clean_session", True))
        self.keepalive = int(config.get("keepalive", 60))

        # Message parsing
        self.payload_format = config.get("payload_format", "auto")  # auto, json, string, bytes
        self.value_field = config.get("value_field", "value")  # For JSON payloads

        # Security configuration (NIS2 compliance)
        self.security_config = create_security_config_from_dict(config)
        self.security_manager = MQTTSecurityManager(self.security_config)

        self._client: aiomqtt.Client | None = None
        self._messages_received = 0

        # Normalization support
        self._normalization_enabled = config.get("normalization_enabled", False)
        self._normalizer = None
        if self._normalization_enabled:
            try:
                from unified_connector.normalizer import get_normalization_manager
                self._norm_manager = get_normalization_manager()
                if self._norm_manager.is_enabled():
                    self._normalizer = self._norm_manager.get_normalizer("mqtt")
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"Could not initialize normalizer: {e}")

    def _parse_endpoint(self) -> None:
        """Parse MQTT endpoint URL."""
        endpoint = self.endpoint.strip()

        if endpoint.startswith("mqtts://"):
            self.use_tls = True
            endpoint = endpoint[8:]
        elif endpoint.startswith("mqtt://"):
            self.use_tls = False
            endpoint = endpoint[7:]
        else:
            # Assume mqtt:// if no scheme
            self.use_tls = False

        # Parse host:port
        if ":" in endpoint:
            self.host, port_str = endpoint.rsplit(":", 1)
            try:
                self.port = int(port_str)
            except ValueError:
                self.port = 8883 if self.use_tls else 1883
                self.host = endpoint
        else:
            self.host = endpoint
            self.port = 8883 if self.use_tls else 1883

    @property
    def protocol_type(self) -> ProtocolType:
        return ProtocolType.MQTT

    async def connect(self) -> None:
        """Establish MQTT connection with TLS/SSL support."""
        if self._client is not None:
            return

        # Validate security configuration
        if not self.security_manager.validate_configuration():
            raise ValueError("Invalid MQTT security configuration")

        # Log security status
        self.security_manager.log_security_status()

        # Get TLS parameters if TLS is enabled
        tls_context = None
        if self.security_config.use_tls:
            tls_context = self.security_manager.create_ssl_context()
            import logging
            logging.getLogger(__name__).info("✓ TLS/SSL enabled for MQTT connection")

        self._client = aiomqtt.Client(
            hostname=self.host,
            port=self.port,
            username=self.security_config.username or self.username,
            password=self.security_config.password or self.password,
            identifier=self.client_id,
            clean_session=self.clean_session,
            protocol=aiomqtt.ProtocolVersion.V311,
            tls_context=tls_context,
            keepalive=self.keepalive,
        )

        await self._client.__aenter__()

    async def disconnect(self) -> None:
        """Disconnect from MQTT broker."""
        if self._client is not None:
            try:
                await self._client.__aexit__(None, None, None)
            except Exception:
                pass
            finally:
                self._client = None

    async def subscribe(self) -> None:
        """Subscribe to MQTT topics and process messages."""
        if self._client is None:
            raise RuntimeError("Not connected")

        # Subscribe to all configured topics
        for topic in self.topics:
            await self._client.subscribe(topic, qos=self.qos)

        # Process messages
        async for message in self._client.messages:
            if self._stop_evt.is_set():
                break

            try:
                # Update last data time
                self._last_data_time = time.time()

                # Check if normalization is enabled
                if self._normalizer:
                    # Create raw data dict for normalizer
                    raw_data = self._create_raw_data(message)
                    try:
                        normalized = self._normalizer.normalize(raw_data)
                        # Send normalized data
                        self.on_record(normalized.to_dict())
                        self._messages_received += 1
                    except Exception as norm_error:
                        # Normalization failed, fall back to raw mode
                        self._emit_stats({
                            "normalization_error": f"{type(norm_error).__name__}: {norm_error}",
                            "topic": message.topic.value if hasattr(message.topic, 'value') else str(message.topic),
                        })
                        # Send raw data as fallback
                        record = self._parse_message(message)
                        if record:
                            self.on_record(record)
                            self._messages_received += 1
                else:
                    # Raw mode (existing behavior)
                    record = self._parse_message(message)
                    if record:
                        self.on_record(record)
                        self._messages_received += 1

                if self._messages_received % 100 == 0:
                    self._emit_stats({
                        "messages_received": self._messages_received,
                        "last_message_time_ms": int(time.time() * 1000),
                    })

            except Exception as e:
                # Log but don't crash on parse errors
                self._emit_stats({
                    "parse_error": f"{type(e).__name__}: {e}",
                    "topic": message.topic.value if hasattr(message.topic, 'value') else str(message.topic),
                })

    def _parse_message(self, message: Any) -> ProtocolRecord | None:
        """Parse MQTT message into a ProtocolRecord."""
        topic = message.topic.value if hasattr(message.topic, 'value') else str(message.topic)
        payload = message.payload

        # Parse payload based on format
        value = None
        value_type = "unknown"
        value_num = None
        metadata: dict[str, Any] = {
            "qos": message.qos,
            "retain": message.retain if hasattr(message, 'retain') else False,
        }

        try:
            if self.payload_format == "json" or (self.payload_format == "auto" and isinstance(payload, bytes)):
                # Try to parse as JSON
                try:
                    parsed = json.loads(payload)
                    if isinstance(parsed, dict):
                        value = parsed.get(self.value_field, parsed)
                        value_type = "json"
                        metadata["full_payload"] = parsed
                    else:
                        value = parsed
                        value_type = type(parsed).__name__
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Fall back to string/bytes
                    if self.payload_format == "json":
                        # User explicitly requested JSON, treat parse failure as error
                        return None
                    if isinstance(payload, bytes):
                        try:
                            value = payload.decode('utf-8')
                            value_type = "string"
                        except UnicodeDecodeError:
                            value = payload.hex()
                            value_type = "bytes"
                            metadata["bytes_length"] = len(payload)
                    else:
                        value = str(payload)
                        value_type = "string"
            elif isinstance(payload, bytes):
                try:
                    value = payload.decode('utf-8')
                    value_type = "string"
                except UnicodeDecodeError:
                    value = payload.hex()
                    value_type = "bytes"
                    metadata["bytes_length"] = len(payload)
            else:
                value = str(payload)
                value_type = "string"

            # Try to extract numeric value
            if isinstance(value, (int, float)):
                value_num = float(value)
            elif isinstance(value, str):
                try:
                    value_num = float(value)
                except (ValueError, TypeError):
                    pass

        except Exception as e:
            self._emit_stats({
                "value_parse_error": f"{type(e).__name__}: {e}",
                "topic": topic,
            })
            return None

        return ProtocolRecord(
            event_time_ms=int(time.time() * 1000),
            source_name=self.source_name,
            endpoint=self.endpoint,
            protocol_type=self.protocol_type,
            topic_or_path=topic,
            value=value,
            value_type=value_type,
            value_num=value_num,
            metadata=metadata,
            status_code=0,
            status="Good",
        )

    def _create_raw_data(self, message: Any) -> dict[str, Any]:
        """
        Create raw data dict for normalizer from MQTT message.

        Args:
            message: MQTT message object

        Returns:
            Dictionary with normalized format expected by MQTTNormalizer
        """
        topic = message.topic.value if hasattr(message.topic, 'value') else str(message.topic)
        payload = message.payload

        # Convert payload to string or keep as-is
        if isinstance(payload, bytes):
            try:
                payload = payload.decode('utf-8')
            except UnicodeDecodeError:
                payload = payload.hex()

        return {
            "topic": topic,
            "payload": payload,
            "qos": message.qos,
            "retained": message.retain if hasattr(message, 'retain') else False,
            "timestamp": int(time.time() * 1000),  # Current time in ms
            "broker_address": f"{self.host}:{self.port}",
            "config": self.config,  # Pass config for context extraction
        }

    async def test_connection(self) -> ProtocolTestResult:
        """Test MQTT connectivity."""
        start_time = time.time()
        error = None
        server_info: dict[str, Any] = {}

        try:
            await self.connect()
            server_info = {
                "host": self.host,
                "port": self.port,
                "tls": self.use_tls,
                "client_id": self.client_id,
            }
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
