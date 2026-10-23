"""The Sparkplug primary host application: STATE, and ONLINE only while Databricks accepts writes.

MQTT Transmission names one Primary Host ID per server set. While that host's STATE says ONLINE it
publishes; otherwise it disconnects and buffers tag data at the edge (store and forward), then rebirths
and sends the backlog (flagged historical) once the host is ONLINE again. So ONLINE must mean "the rows
reach Databricks": ``HostState`` decides from the confirmed writer's health and ``HostSupervisor`` makes
the MQTT side match.

Going OFFLINE publishes OFFLINE and disconnects: Sparkplug 3.0 doesn't allow going back ONLINE on the
same connection (the ONLINE timestamp must be the one in the prior CONNECT's will), so ONLINE again is a
new connection with a new will.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Protocol

from sparkplug_connector.core.confirmed_writer import WriterStatus

logger = logging.getLogger(__name__)

V3, V2 = "v3", "v2"


def state_topic(state_format: str, host_id: str) -> str:
    """v3: ``spBv1.0/STATE/<host>``; v2 (legacy): ``STATE/<host>``."""
    return f"spBv1.0/STATE/{host_id}" if state_format == V3 else f"STATE/{host_id}"


def state_payload(state_format: str, online: bool, timestamp_ms: int) -> bytes:
    """v3 ``{"online":b,"timestamp":t}``; v2 ``ONLINE``/``OFFLINE``."""
    if state_format == V3:
        return json.dumps({"online": online, "timestamp": timestamp_ms}, separators=(",", ":")).encode()
    return b"ONLINE" if online else b"OFFLINE"


def is_online_state(state_format: str, payload: bytes) -> bool:
    text = payload.decode("utf-8", "replace").strip()
    if state_format == V2:
        return text == "ONLINE"
    try:
        return json.loads(text).get("online") is True
    except (ValueError, AttributeError):
        return False


class HostState:
    """ONLINE or OFFLINE from the writer's health, with hysteresis.

    - Starts OFFLINE; goes ONLINE as soon as Zerobus is first healthy (a stream opened).
    - ONLINE -> OFFLINE when writes have failed for ``offline_after_s``, a batch has waited that long for
      its acknowledgement, or the queue is above its high-water mark (the edges then buffer instead).
    - OFFLINE -> ONLINE (after a failure) only once healthy for ``online_after_s`` without a break.
    - No writer (Zerobus stopped, or not started yet): OFFLINE at once, nothing is being written.
    """

    ONLINE, OFFLINE = "ONLINE", "OFFLINE"

    def __init__(self, offline_after_s: float = 30.0, online_after_s: float = 30.0):
        self._offline_after = offline_after_s
        self._online_after = online_after_s
        self.mode = self.OFFLINE
        self._ever_online = False
        self._healthy_since: float | None = None

    def evaluate(self, s: WriterStatus | None, now: float) -> str:
        if s is None:
            self._healthy_since = None
            self.mode = self.OFFLINE
            return self.mode
        failing_too_long = s.failing_since is not None and now - s.failing_since >= self._offline_after
        stuck = s.pending_since is not None and now - s.pending_since >= self._offline_after
        healthy = s.failing_since is None and not stuck and s.last_success is not None and not s.queue_high_water
        self._healthy_since = (self._healthy_since if self._healthy_since is not None else now) if healthy else None
        if self.mode == self.ONLINE and (failing_too_long or stuck or s.queue_high_water):
            self.mode = self.OFFLINE
        elif self.mode == self.OFFLINE and healthy and (not self._ever_online or now - self._healthy_since >= self._online_after):
            self.mode = self.ONLINE
            self._ever_online = True
        return self.mode


class Host(Protocol):
    """The MQTT side the supervisor drives (SparkplugClient)."""
    online_allowed: asyncio.Event

    @property
    def is_connected(self) -> bool: ...

    async def go_offline(self) -> None: ...


class HostSupervisor:
    """Once a second: asks HostState whether to be ONLINE and makes the source match. ONLINE opens the
    source's connect gate (the bridge's reconnect loop then connects: new will, subscribe, ONLINE);
    OFFLINE closes it, publishes OFFLINE and disconnects."""

    def __init__(self, host: Host, writer_status: Callable[[], WriterStatus | None], state: HostState,
                 name: str = "sparkplug", tick_s: float = 1.0, clock: Callable[[], float] = time.time):
        self._host = host
        self._writer_status = writer_status
        self.state = state
        self._name = name
        self._tick_s = tick_s
        self._clock = clock
        self._last_mode: str | None = None

    async def run(self) -> None:
        failed = False
        while True:
            try:
                await self.tick()
                failed = False
            except Exception as e:  # noqa: BLE001 - if this task ended, the gate would stay as it was for good
                logger.error("[%s] host state check failed: %s: %s", self._name, type(e).__name__, e, exc_info=not failed)
                failed = True
            await asyncio.sleep(self._tick_s)

    async def tick(self) -> str:
        status = self._writer_status()
        mode = self.state.evaluate(status, self._clock())
        if mode != self._last_mode:
            if status is None:
                logger.info("[%s] host state %s -> %s (no Zerobus writer)", self._name, self._last_mode or "start", mode)
            else:
                logger.info("[%s] host state %s -> %s (writer: failing_since=%s, pending_since=%s, queue_high_water=%s, "
                            "last_error=%s)", self._name, self._last_mode or "start", mode, status.failing_since,
                            status.pending_since, status.queue_high_water, status.last_error)
            self._last_mode = mode
        if mode == HostState.ONLINE:
            self._host.online_allowed.set()
        else:
            self._host.online_allowed.clear()
            if self._host.is_connected:
                await self._host.go_offline()
        return mode

    def snapshot(self) -> dict[str, Any]:
        return {"state": self.state.mode}
