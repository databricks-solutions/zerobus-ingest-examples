"""The bridge's lifecycle for the web page: stop, start and restart, one at a time.

A ``Bridge`` runs once: its ``stop()`` drains and closes everything for good (queued rows written up to
``zerobus.drain_timeout_seconds``, the write-ahead logs fsynced and closed). So a start after a stop builds a new
``Bridge`` from the configuration file, which also applies the settings saved since (a restart = stop + start).
The write-ahead logs reopen with the new bridge and replay what wasn't acknowledged.

Listeners (``add_listener``) are called with ``(bridge, config)`` when a new bridge replaces the stopped one: the
owners of a bridge reference (the web server, the connector's main) follow it.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, Optional

from sparkplug_connector.core.bridge import Bridge

logger = logging.getLogger(__name__)

STARTING, RUNNING, STOPPING, STOPPED = "starting", "running", "stopping", "stopped"


class BridgeLifecycle:
    def __init__(self, bridge: Bridge, load_config: Callable[[], Dict[str, Any]],
                 make_bridge: Callable[[Dict[str, Any]], Bridge] = Bridge):
        self.bridge = bridge
        self._load_config, self._make_bridge = load_config, make_bridge
        self._listeners: list[Callable[[Bridge, Dict[str, Any]], None]] = []
        self._lock = asyncio.Lock()
        self._used = False              # the current bridge was stopped: it can't run again
        self._state: Optional[str] = None
        self._task: Optional[asyncio.Task] = None
        self._pending: Optional[str] = None     # submitted, its task not done (busy before it takes the lock)
        self._closing = False                   # shutdown began: nothing starts any more

    @property
    def state(self) -> str:
        if self._state is not None:
            return self._state
        if self._pending is not None:
            return STARTING if self._pending == "start" else STOPPING
        if self._used:
            return STOPPED
        if self.bridge.running:
            return RUNNING
        return STARTING if self.bridge._starting else STOPPED

    def add_listener(self, listener: Callable[[Bridge, Dict[str, Any]], None]) -> None:
        self._listeners.append(listener)

    @property
    def busy(self) -> bool:
        return self._lock.locked() or self._pending is not None

    async def start(self) -> None:
        """The bridge and its enabled sources; a new bridge from the file if the current one was stopped."""
        async with self._lock:
            await self._start()

    async def stop(self) -> None:
        async with self._lock:
            await self._stop()

    async def restart(self) -> None:
        """Stop (drained), then a new bridge from the file: the settings saved since apply."""
        async with self._lock:
            await self._stop()
            await self._start()

    def submit(self, action: str) -> asyncio.Task:
        """``start``/``stop``/``restart`` in the background (the page polls the state); one at a time."""
        self._pending = action                  # a second request in the same loop turn sees it busy
        task = asyncio.create_task(getattr(self, action)())

        def done(t: asyncio.Task) -> None:
            if self._task is t:
                self._pending = None
            if not t.cancelled() and t.exception() is not None:
                logger.error("Bridge %s failed: %s", action, t.exception())
        task.add_done_callback(done)
        self._task = task
        return task

    async def shutdown(self) -> None:
        """The connector exits: stop the bridge unless it was stopped already. A start in progress is cancelled
        (its stop cleans up what it started); a stop in progress finishes its drain first, and a restart's start
        doesn't follow it (no sources connecting just to be stopped again)."""
        self._closing = True
        if self._task is not None and not self._task.done():
            if self.state == STARTING:
                self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        async with self._lock:
            if not self._used:
                await self._stop()

    async def _start(self) -> None:
        if self._closing or (self.bridge.running and not self._used):
            return
        if self._used:
            config = self._load_config()
            self.bridge = self._make_bridge(config)
            self._used = False
            for listener in self._listeners:
                listener(self.bridge, config)
            logger.info("A new bridge from the configuration file")
        self._state = STARTING
        try:
            await self.bridge.start()
        finally:
            self._state = None

    async def _stop(self) -> None:
        if self._used:
            return
        self._state = STOPPING
        try:
            await self.bridge.stop()
        finally:
            self._used = True
            self._state = None
