"""Sparkplug B source: one ``sparkplug_raw`` row per Sparkplug message from an MQTT server (e.g. Chariot).

Connects to the MQTT server (``mqtt://host:port`` or ``mqtts://host:port``; credentials and TLS through
``mqtt_security.py``), subscribes (QoS 0 by default), keeps each payload as bytes and hands a ``RawRow`` (``sparkplug_connector.sparkplug.raw_row``) to the
bridge, which writes it unchanged.

With ``primary_host_id`` it is the edges' Sparkplug **primary host application** (``sparkplug/host.py``):
each connection carries a will (OFFLINE STATE, retained, QoS 1) stamped with the connect time T,
subscribes, then publishes ONLINE with the same T; a live OFFLINE for its own host id is answered with
ONLINE again. The bridge's ``HostSupervisor`` opens ``online_allowed`` only while Databricks accepts the
rows; ``connect()`` waits for it, and ``go_offline()`` publishes OFFLINE and disconnects.
Without ``primary_host_id`` it is a plain subscriber (not a primary host).
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from typing import Any, Callable

import aiomqtt

from sparkplug_connector.protocols.base import ProtocolClient, ProtocolType
from sparkplug_connector.protocols.mqtt_security import MQTTSecurityManager, create_security_config_from_dict
from sparkplug_connector.sparkplug.host import V2, V3, is_online_state, state_payload, state_topic
from sparkplug_connector.sparkplug.raw_row import Outcome, RawRow, Skip, convert

logger = logging.getLogger(__name__)

DEFAULT_TOPICS = ["spBv1.0/#"]
DISCONNECT_TIMEOUT_S = 5.0
STATE_PUBLISH_TIMEOUT_S = 5.0
CONNECT_TIMEOUT_S = 10.0


class SparkplugClient(ProtocolClient):
    """MQTT client that turns Sparkplug B messages into ``sparkplug_raw`` rows (optionally the primary host)."""

    def __init__(
        self,
        source_name: str,
        endpoint: str,
        config: dict[str, Any],
        on_record: Callable[[RawRow], None],
        on_stats: Callable[[dict[str, Any]], None] | None = None,
    ):
        super().__init__(source_name, endpoint, config, on_record, on_stats)
        self.host, self.port, self.use_tls = parse_endpoint(endpoint)
        topics = config.get("topics") or list(DEFAULT_TOPICS)
        self.topics: list[str] = [topics] if isinstance(topics, str) else list(topics)
        self.qos = int(config.get("qos", 0))          # Sparkplug publishes at QoS 0
        self.client_id = config.get("client_id", f"dbx-{source_name}")
        self.username = config.get("username")
        self.password = config.get("password")
        self.keepalive = int(config.get("keepalive", 60))
        # TLS (mqtts:// or security.use_tls) and credentials: protocols/mqtt_security.py
        self.security_config = create_security_config_from_dict({**config, "endpoint": endpoint})
        self.security_manager = MQTTSecurityManager(self.security_config)
        self._client: aiomqtt.Client | None = None
        self.counters: Counter[str] = Counter()
        self.subscribed = asyncio.Event()

        self.primary_host_id: str | None = config.get("primary_host_id") or None
        self.state_format: str = str(config.get("state_format", V3)).lower()
        if self.state_format not in (V3, V2):
            raise ValueError(f"state_format must be v3 or v2, not {self.state_format!r}")
        self.state_topic = state_topic(self.state_format, self.primary_host_id) if self.primary_host_id else None
        self.offline_after_seconds = float(config.get("offline_after_seconds", 30))
        self.online_after_seconds = float(config.get("online_after_seconds", 30))
        self.online = False                           # ONLINE published on the current connection
        self.session_timestamp: int | None = None     # T: the will's and ONLINE's timestamp
        self.online_allowed = asyncio.Event()         # the HostSupervisor opens it; always open for a plain subscriber
        if not self.primary_host_id:
            self.online_allowed.set()
        self._state_tasks: set[asyncio.Task] = set()
        self._state_lock = asyncio.Lock()             # STATE publishes in order: no ONLINE after an OFFLINE

    @property
    def protocol_type(self) -> ProtocolType:
        return ProtocolType.SPARKPLUG

    @property
    def is_connected(self) -> bool:
        return self._client is not None

    @property
    def is_online(self) -> bool:
        return self.online and self._client is not None

    async def connect(self) -> None:
        """Connect once allowed (primary host: while HostState says ONLINE), with a fresh will."""
        if self._client is not None:
            return
        await self.online_allowed.wait()
        if not self.security_manager.validate_configuration():
            raise ValueError("Invalid MQTT security configuration")
        self.security_manager.log_security_status()
        tls_context = self.security_manager.create_ssl_context() if self.security_config.use_tls else None
        t = int(time.time() * 1000)
        will = None
        if self.primary_host_id:
            will = aiomqtt.Will(self.state_topic, state_payload(self.state_format, False, t), qos=1, retain=True)
        client = aiomqtt.Client(
            hostname=self.host, port=self.port,
            username=self.security_config.username or self.username,
            password=self.security_config.password or self.password,
            identifier=self.client_id, clean_session=True, protocol=aiomqtt.ProtocolVersion.V311,
            tls_context=tls_context, keepalive=self.keepalive, will=will, timeout=CONNECT_TIMEOUT_S,
        )
        await client.__aenter__()
        self._client = client
        self.session_timestamp = t

    async def subscribe(self) -> None:
        if self._client is None:
            raise RuntimeError("Not connected")
        for topic in self.topics:
            await self._client.subscribe(topic, qos=self.qos)
        if self.primary_host_id:
            # own STATE before ONLINE; only if no filter covers it (spBv1.0/# covers v3), or every STATE comes twice
            if not any(_matches(f, self.state_topic) for f in self.topics):
                await self._client.subscribe(self.state_topic, qos=1)
            async with self._state_lock:
                # taken OFFLINE while subscribing (go_offline published OFFLINE, or this connection is gone)
                if not self.online_allowed.is_set() or self._client is None:
                    raise ConnectionError("taken OFFLINE while subscribing; no ONLINE")
                await self._client.publish(self.state_topic, state_payload(self.state_format, True, self.session_timestamp),
                                           qos=1, retain=True)
                self.online = True
            logger.info("[%s] ONLINE as primary host %s (STATE %s, timestamp %d), subscribed to %s",
                        self.source_name, self.primary_host_id, self.state_topic, self.session_timestamp, self.topics)
        else:
            logger.info("[%s] subscribed to %s", self.source_name, self.topics)
        self.subscribed.set()
        try:
            async for message in self._client.messages:
                topic, payload = message.topic.value, _payload_bytes(message.payload)
                if self.primary_host_id and topic == self.state_topic:
                    self.counters["messages_received"] += 1
                    self._on_own_state(payload, bool(getattr(message, "retain", False)))
                    continue
                self.handle_message(topic, payload, int(time.time() * 1000))
        finally:
            self.subscribed.clear()
            self.online = False

    def _on_own_state(self, payload: bytes, retained: bool) -> None:
        """A live OFFLINE for our host id while ONLINE: publish ONLINE again with T. The retained copy the
        broker delivers on subscribe is the previous session's state, about to be replaced: ignored."""
        if not self.online or retained or is_online_state(self.state_format, payload):
            return
        task = asyncio.get_running_loop().create_task(self._republish_online(self.session_timestamp))
        self._state_tasks.add(task)
        task.add_done_callback(self._state_tasks.discard)

    async def _republish_online(self, t: int) -> None:
        async with self._state_lock:
            client = self._client
            if not (self.online and client is not None and self.session_timestamp == t):
                return
            try:
                await asyncio.wait_for(client.publish(self.state_topic, state_payload(self.state_format, True, t), qos=1,
                                                      retain=True), STATE_PUBLISH_TIMEOUT_S)
                self.counters["state_republished"] += 1
                logger.warning("[%s] own STATE was set offline while ONLINE; re-published ONLINE (timestamp %d)",
                               self.source_name, t)
            except Exception as e:  # noqa: BLE001 - the connection may be going away
                logger.warning("[%s] re-publishing ONLINE failed: %s", self.source_name, e)

    async def go_offline(self) -> None:
        """Publish OFFLINE (now) and disconnect; ONLINE again only on a new connection (new will)."""
        async with self._state_lock:
            was_online, self.online = self.online, False
            client = self._client
            if client is not None and self.primary_host_id:
                try:
                    await asyncio.wait_for(client.publish(self.state_topic, state_payload(self.state_format, False,
                                                                                          int(time.time() * 1000)),
                                                          qos=1, retain=True), STATE_PUBLISH_TIMEOUT_S)
                except Exception as e:  # noqa: BLE001 - a broken connection: the broker publishes the will instead
                    logger.warning("[%s] publishing OFFLINE failed: %s", self.source_name, e)
        await self.disconnect()
        if was_online:
            logger.info("[%s] OFFLINE: published OFFLINE STATE and disconnected", self.source_name)

    def handle_message(self, topic: str, payload: bytes, received_at_ms: int) -> Outcome:
        """One MQTT message: a row for the bridge, or a counted skip."""
        self.counters["messages_received"] += 1
        outcome = convert(topic, payload, received_at_ms)
        if outcome.row is not None:
            try:
                self.on_record(outcome.row)
                self.counters["rows"] += 1
            except Exception as e:  # noqa: BLE001 - one row mustn't end the subscription (a primary host OFFLINE)
                self.counters["rows_failed"] += 1
                logger.error("[%s] row not queued (%s): %s: %s", self.source_name, topic, type(e).__name__, e)
        else:
            self.counters[f"skipped_{outcome.skip.value}"] += 1
            if outcome.skip in (Skip.MALFORMED_TOPIC, Skip.UNDECODABLE_PAYLOAD):
                logger.warning("[%s] no row (%s) for %s: %s", self.source_name, outcome.skip.value, topic, outcome.detail)
        if self.counters["messages_received"] % 100 == 0:
            self._emit_stats(self.stats())
        return outcome

    def stats(self) -> dict[str, int]:
        """Messages received, rows handed on, skipped messages by reason, ONLINE re-published."""
        return {
            "messages_received": self.counters["messages_received"],
            "rows": self.counters["rows"],
            "rows_failed": self.counters["rows_failed"],
            **{f"skipped_{s.value}": self.counters[f"skipped_{s.value}"] for s in Skip},
            "state_republished": self.counters["state_republished"],
        }

    async def disconnect(self) -> None:
        """The MQTT client's disconnect, bounded: on a half-open connection aiomqtt's exit can wait for good,
        and the bridge calls this before it reconnects (seen 2026-10-06 when the test broker restarted)."""
        self.online = False
        try:
            await asyncio.wait_for(self._close(), DISCONNECT_TIMEOUT_S)
        except asyncio.TimeoutError:
            logger.warning("[%s] MQTT disconnect took over %.0f s; dropping the connection", self.source_name, DISCONNECT_TIMEOUT_S)
            self._client = None

    async def _close(self) -> None:
        if self._client is not None:
            try:
                await self._client.__aexit__(None, None, None)
            except Exception:  # noqa: BLE001 - the connection is going away either way
                pass
            finally:
                self._client = None

    async def stop(self) -> None:
        """OFFLINE (primary host) and disconnect. The bridge calls this on shutdown and also to clean up after
        a connection error before it reconnects, so it must not leave a stop flag set (that ended every
        later subscription at its first message: found 2026-10-06 when Chariot was restarted). The bridge's
        own shutdown flag ends its reconnect loop."""
        await self.go_offline()


def parse_endpoint(endpoint: str) -> tuple[str, int, bool]:
    """(host, port, TLS) of ``mqtt://host:port`` or ``mqtts://host:port`` (no scheme: mqtt; default ports
    1883 and 8883)."""
    endpoint = endpoint.strip()
    use_tls = endpoint.startswith("mqtts://")
    if endpoint.startswith(("mqtts://", "mqtt://")):
        endpoint = endpoint.split("://", 1)[1]
    default_port = 8883 if use_tls else 1883
    if ":" in endpoint:
        host, port = endpoint.rsplit(":", 1)
        try:
            return host, int(port), use_tls
        except ValueError:
            return endpoint, default_port, use_tls
    return endpoint, default_port, use_tls


def _matches(topic_filter: str, topic: str) -> bool:
    """MQTT topic filter matching (``+`` one level, ``#`` the rest)."""
    f, t = topic_filter.split("/"), topic.split("/")
    for i, level in enumerate(f):
        if level == "#":
            return True
        if i >= len(t) or level not in ("+", t[i]):
            return False
    return len(f) == len(t)


def _payload_bytes(payload: Any) -> bytes:
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload)
    return b"" if payload is None else str(payload).encode("utf-8")
