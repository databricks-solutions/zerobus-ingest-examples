"""Sparkplug B source: one ``sparkplug_raw`` row per Sparkplug message from an MQTT server (e.g. Chariot).

Connects to the MQTT server (``mqtt://host:port`` or ``mqtts://host:port``; credentials and TLS through
``mqtt_security.py``), subscribes (QoS 0 by default), keeps each payload as bytes and hands a ``RawRow`` (``sparkplug_connector.sparkplug.raw_row``) to the
bridge, which writes it unchanged.

With ``primary_host_id`` it is the edges' Sparkplug **primary host application** (``sparkplug/host.py``):
each connection carries a will (OFFLINE STATE, retained, QoS 1) stamped with the connect time T,
subscribes (its topics, then its own STATE topic), then publishes ONLINE with the same T; a live OFFLINE for its
own host id is answered with ONLINE again. The bridge's ``HostSupervisor`` opens ``online_allowed`` only while Databricks accepts the
rows; ``connect()`` waits for it, and ``go_offline()`` publishes OFFLINE and disconnects.
Without ``primary_host_id`` it is a plain subscriber (not a primary host).

In both modes, rows and rebirth requests follow Eclipse Tahu's host decisions (``sparkplug/sequencer.py``):
``sequence_handling`` (``strict``: messages in ``seq`` order, an edge node's dropped after a gap until its rebirth;
``keep_all``: every message), ``sequence_reordering_timeout`` (ms) and ``block_auto_rebirth`` (only with
``keep_all``: strict needs the rebirths). A rebirth request is an NCMD on ``spBv1.0/<group>/NCMD/<edge node>`` (the
MQTT user needs publish rights there); if the server closes the connection right after one twice in a row (Chariot's
answer to a refused publish), the source sends no more until it restarts and writes every message (``keep_all``).
An operator can also ask an edge node for a rebirth (``request_rebirth``: the same NCMD, sent at once; not an automatic
request, so ``block_auto_rebirth`` doesn't stop it).
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
from sparkplug_connector.sparkplug.sequencer import (DEFAULT_REORDER_TIMEOUT_MS, KEEP_ALL, STRICT, TICK_S, Sequencer,
                                                     rebirth_payload, rebirth_topic)

logger = logging.getLogger(__name__)

DEFAULT_TOPICS = ["spBv1.0/#"]
DISCONNECT_TIMEOUT_S = 5.0
STATE_PUBLISH_TIMEOUT_S = 5.0
CONNECT_TIMEOUT_S = 10.0
REFUSED_WITHIN_S = 2.0          # a connection lost this soon after an NCMD counts as a refusal of it
REFUSALS_TO_STOP = 2            # in a row (an NCMD the connection outlives starts the count again)


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
        self._last_own_state: bytes | None = None     # the last STATE seen on our topic (this connection)
        self.session_timestamp: int | None = None     # T: the will's and ONLINE's timestamp
        self.online_allowed = asyncio.Event()         # the HostSupervisor opens it; always open for a plain subscriber
        if not self.primary_host_id:
            self.online_allowed.set()
        self._state_tasks: set[asyncio.Task] = set()
        self._state_lock = asyncio.Lock()             # STATE publishes in order: no ONLINE after an OFFLINE

        self.block_auto_rebirth = _flag(config.get("block_auto_rebirth", False))
        self.sequence_handling = str(config.get("sequence_handling") or STRICT).lower()
        timeout = config.get("sequence_reordering_timeout")
        timeout_ms = DEFAULT_REORDER_TIMEOUT_MS if timeout is None or str(timeout).strip() == "" else int(str(timeout).strip())
        self.sequencer = Sequencer(self.sequence_handling, timeout_ms, self.block_auto_rebirth, self._request_rebirth,
                                   source_name)
        if self.block_auto_rebirth and self.sequence_handling == STRICT:
            raise ValueError("block_auto_rebirth: true needs sequence_handling: keep_all (strict drops an edge node's "
                             "messages after a gap until its rebirth, which it then never asks for)")
        self._ticker: asyncio.Task | None = None      # the reorder timeouts, for the source's life (across reconnects)
        self._closing = False                         # our own disconnect: not a refused NCMD
        self._ncmd_at: float | None = None            # when the last NCMD went out on this connection
        self._refusals = 0                            # connections lost right after an NCMD, in a row
        self.rebirth_refused = False                  # the server refused NCMDs: none sent until the source restarts

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
        self._closing = False
        self._ncmd_at = None
        self._last_own_state = None

    async def subscribe(self) -> None:
        if self._client is None:
            raise RuntimeError("Not connected")
        for topic in self.topics:
            await self._client.subscribe(topic, qos=self.qos)
        if self.primary_host_id:
            # own STATE before ONLINE, even under spBv1.0/# (Sparkplug: the host subscribes to its own STATE topic;
            # the TCK checks for the filter itself); a server may then send each STATE once per filter: _on_own_state
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
        if self._ticker is None or self._ticker.done():
            self._ticker = asyncio.get_running_loop().create_task(self._tick())
        try:
            async for message in self._client.messages:
                topic, payload = message.topic.value, _payload_bytes(message.payload)
                if self.primary_host_id and topic == self.state_topic:
                    if self._on_own_state(payload, bool(getattr(message, "retain", False))):
                        self.counters["messages_received"] += 1       # a copy (once per filter) isn't counted
                    continue
                self.handle_message(topic, payload, int(time.time() * 1000))
        except aiomqtt.MqttError:
            self._connection_lost()
            raise
        finally:
            self.subscribed.clear()
            self.online = False

    def _on_own_state(self, payload: bytes, retained: bool) -> bool:
        """A live OFFLINE for our host id while ONLINE: publish ONLINE again with T. The retained copy the
        broker delivers on subscribe is the previous session's state, about to be replaced: ignored. So is a copy
        of the STATE just seen (a server may send one per matching filter: ``spBv1.0/#`` and the STATE topic), or
        ONLINE would go out twice. False for a copy."""
        copy, self._last_own_state = payload == self._last_own_state, payload
        if copy:
            return False
        if not self.online or retained or is_online_state(self.state_format, payload):
            return True
        task = asyncio.get_running_loop().create_task(self._republish_online(self.session_timestamp))
        self._state_tasks.add(task)
        task.add_done_callback(self._state_tasks.discard)
        return True

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
                self._last_own_state = None     # no ONLINE came back: the same OFFLINE again is answered again
                logger.warning("[%s] re-publishing ONLINE failed: %s", self.source_name, e)

    async def go_offline(self) -> None:
        """Publish OFFLINE (now) and disconnect; ONLINE again only on a new connection (new will)."""
        self._closing = True
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
        """One MQTT message: rows for the bridge (the sequencer's: this one, held ones it releases, or none), or a
        counted skip."""
        self.counters["messages_received"] += 1
        outcome = convert(topic, payload, received_at_ms)
        if outcome.row is not None:
            try:
                rows = self.sequencer.on_message(outcome, time.monotonic())
            except Exception as e:  # noqa: BLE001 - mustn't end the subscription either: the message keeps its row
                self.counters["sequence_errors"] += 1
                logger.error("[%s] sequencing failed (%s), row written as received: %s: %s", self.source_name, topic,
                             type(e).__name__, e, exc_info=self.counters["sequence_errors"] == 1)
                rows = [outcome.row]
            self._emit(rows)
        else:
            self.counters[f"skipped_{outcome.skip.value}"] += 1
            if outcome.skip in (Skip.MALFORMED_TOPIC, Skip.UNDECODABLE_PAYLOAD):
                logger.warning("[%s] no row (%s) for %s: %s", self.source_name, outcome.skip.value, topic, outcome.detail)
        if self.counters["messages_received"] % 100 == 0:
            self._emit_stats(self.stats())
        return outcome

    def _emit(self, rows: list[RawRow]) -> None:
        for row in rows:
            try:
                self.on_record(row)
                self.counters["rows"] += 1
            except Exception as e:  # noqa: BLE001 - one row mustn't end the subscription (a primary host OFFLINE)
                self.counters["rows_failed"] += 1
                logger.error("[%s] row not queued (%s): %s: %s", self.source_name, row.msg_topic, type(e).__name__, e)

    def stats(self) -> dict[str, Any]:
        """Messages received, rows handed on, skipped messages by reason, ONLINE re-published, the sequencer's
        counters and the rebirth requests sent."""
        return {
            "messages_received": self.counters["messages_received"],
            "rows": self.counters["rows"],
            "rows_failed": self.counters["rows_failed"],
            **{f"skipped_{s.value}": self.counters[f"skipped_{s.value}"] for s in Skip},
            "state_republished": self.counters["state_republished"],
            **self.sequencer.stats(),
            "sequence_errors": self.counters["sequence_errors"],
            "block_auto_rebirth": self.block_auto_rebirth,
            "rebirth_requests_sent": self.counters["rebirth_requests_sent"],
            "rebirth_requests_on_request": self.counters["rebirth_requests_on_request"],
            "rebirth_requests_failed": self.counters["rebirth_requests_failed"],
            "rebirth_refused": self.rebirth_refused,
            "rebirth_requests_not_sent_refused": self.counters["rebirth_requests_not_sent_refused"],
        }

    def edge_nodes(self) -> list[dict[str, Any]]:
        """The edge nodes and devices seen, online or offline since when (``Sequencer.edge_nodes``)."""
        return self.sequencer.edge_nodes()

    # ------------------------------------------------------------------ rebirth requests

    async def request_rebirth(self, group_id: str, edge_node_id: str) -> tuple[bool, str]:
        """An operator's rebirth request for an edge node: the NCMD, sent now (sent?, why not). ValueError: not a
        group id and an edge node id."""
        if not _valid_id(group_id) or not _valid_id(edge_node_id):
            raise ValueError("a group id and an edge node id, without '/', '+' or '#'")
        topic = rebirth_topic(group_id, edge_node_id)
        if self.rebirth_refused:
            return False, "the MQTT server refused rebirth requests (NCMD) from this source: none sent until it restarts"
        client = self._client
        if client is None or not self.subscribed.is_set():
            return False, "not connected"
        self.counters["rebirth_requests_on_request"] += 1
        self.sequencer.rebirth_requested(group_id, edge_node_id, time.monotonic())
        if not await self._send_rebirth(client, topic):
            return False, f"the NCMD on {topic} wasn't sent (see the log)"
        return True, f"NCMD sent on {topic}"

    def _request_rebirth(self, group_id: str, edge_node_id: str) -> None:
        """The sequencer asks an edge node for a rebirth: an NCMD (QoS 0, not retained)."""
        topic = rebirth_topic(group_id, edge_node_id)
        if self.rebirth_refused:
            self.counters["rebirth_requests_not_sent_refused"] += 1
            return
        client = self._client
        if client is None or not self.subscribed.is_set():
            self.counters["rebirth_requests_failed"] += 1
            logger.warning("[%s] rebirth request on %s not sent: not connected", self.source_name, topic)
            return
        task = asyncio.get_running_loop().create_task(self._send_rebirth(client, topic))
        self._state_tasks.add(task)
        task.add_done_callback(self._state_tasks.discard)

    async def _send_rebirth(self, client: aiomqtt.Client, topic: str) -> bool:
        self._ncmd_at = time.monotonic()
        try:
            await asyncio.wait_for(client.publish(topic, rebirth_payload(int(time.time() * 1000)), qos=0, retain=False),
                                   STATE_PUBLISH_TIMEOUT_S)
            self.counters["rebirth_requests_sent"] += 1
            logger.info("[%s] rebirth requested: NCMD on %s", self.source_name, topic)
            return True
        except Exception as e:  # noqa: BLE001 - the connection may be going away
            self.counters["rebirth_requests_failed"] += 1
            logger.warning("[%s] rebirth request on %s not sent: %s", self.source_name, topic, e)
            return False

    def _connection_lost(self) -> None:
        """The connection is lost. Right after an NCMD, twice in a row, the server refused them (Chariot closes the
        connection on a publish the ACL refuses; messages it had already sent may still arrive in between): no more
        NCMDs, or every gap would cost a reconnect, and ``keep_all``, since strict without rebirths would drop an
        edge node's messages until it rebirths by itself."""
        ncmd_at, self._ncmd_at = self._ncmd_at, None
        if self._closing or self.rebirth_refused or ncmd_at is None or time.monotonic() - ncmd_at > REFUSED_WITHIN_S:
            return
        self._refusals += 1
        user = self.security_config.username or self.username
        if self._refusals < REFUSALS_TO_STOP:
            logger.warning("[%s] the connection was lost right after a rebirth request (NCMD): if it happens again, "
                           "user %r may not publish on spBv1.0/<group>/NCMD/<edge node>", self.source_name, user)
            return
        self.rebirth_refused = True
        if self.sequence_handling != KEEP_ALL:
            self.sequence_handling = KEEP_ALL
            self._emit(self.sequencer.keep_all_from_now())
        logger.error("[%s] the MQTT server closed the connection right after a rebirth request (NCMD) %d times in a "
                     "row: user %r may not publish on spBv1.0/<group>/NCMD/<edge node>. No more rebirth requests from "
                     "this source until it restarts, and every message gets its row (keep_all): give the user publish "
                     "rights on spBv1.0/+/NCMD/+", self.source_name, self._refusals, user)

    async def _tick(self) -> None:
        while True:
            await asyncio.sleep(TICK_S)
            now = time.monotonic()
            if self._ncmd_at is not None and self._client is not None and now - self._ncmd_at > REFUSED_WITHIN_S:
                self._ncmd_at, self._refusals = None, 0       # the connection outlived the NCMD: not refused
            try:
                self.sequencer.tick(now)
            except Exception as e:  # noqa: BLE001 - the timer must go on
                logger.error("[%s] sequence timeout check failed: %s: %s", self.source_name, type(e).__name__, e)

    async def disconnect(self) -> None:
        """The MQTT client's disconnect, bounded: on a half-open connection aiomqtt's exit can wait for good,
        and the bridge calls this before it reconnects (seen 2026-10-06 when the test broker restarted)."""
        self._closing = True
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

    async def close(self) -> None:
        """The source is gone (after its last stop): the reorder timer ends and held messages are dropped."""
        if self._ticker is not None:
            self._ticker.cancel()
            await asyncio.gather(self._ticker, return_exceptions=True)
            self._ticker = None
        self.sequencer.close()


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


def _valid_id(value: Any) -> bool:
    """A Sparkplug group or edge node id for a topic: a non-empty string without ``/``, ``+`` or ``#``."""
    return isinstance(value, str) and value != "" and not any(c in value for c in "/+#")


def _flag(value: Any) -> bool:
    """A setting's true/false (YAML gives a bool, a placeholder a string): the rule of the bridge's switches and the
    web form (anything but false, 0, no, off, empty or none is true)."""
    return str(value).strip().lower() not in ("false", "0", "no", "off", "", "none")


def _payload_bytes(payload: Any) -> bytes:
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload)
    return b"" if payload is None else str(payload).encode("utf-8")
