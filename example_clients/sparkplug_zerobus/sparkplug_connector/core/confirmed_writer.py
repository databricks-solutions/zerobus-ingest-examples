"""Zerobus writes confirmed before rows leave the connector (zerobus.confirmed_writes).

One task owns the stream. It takes a batch from the queue, keeps it until Zerobus acknowledges it,
and on any failure closes the stream and resends the same batch on a fresh one, with backoff (1 s,
doubling, at most 30 s). Rows are written at least once; a row keeps its ``msg_id`` across resends.

It never decides ONLINE/OFFLINE itself: ``status()`` shows when writes started failing, how long the
current batch has waited and whether the queue is above its high-water mark.

Rows larger than Zerobus accepts are skipped and counted (Zerobus rejects them at ingest and would
reject them again on every resend). Measured 2026-10-06 with SDK 1.10.0: "Ingest payload too large:
11534603 bytes exceeds the configured limit of 10420224 bytes". So are rows that can't be turned into a
JSON record (``skip_row``); a take that fails as a whole is retried with backoff, like a write.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

logger = logging.getLogger(__name__)

MAX_RECORD_BYTES = 10_420_224
MIN_BACKOFF_S, MAX_BACKOFF_S = 1.0, 30.0


class RowStream(Protocol):
    """A Zerobus stream as the writer uses it (a seam for tests)."""

    async def ingest(self, records: list[str]) -> int:
        """Queues JSON records; returns the offset of the last one."""

    async def wait_for(self, offset: int) -> None:
        """Returns once everything up to ``offset`` is durably written."""

    async def close(self) -> None:
        ...


StreamFactory = Callable[[], Awaitable[RowStream]]
TakeBatch = Callable[[int, float, float], Awaitable[list[dict[str, Any]]]]


@dataclass(frozen=True)
class WriterStatus:
    """Point-in-time view for health and the host state. Times are epoch seconds, None when not applicable."""
    stream_open: bool
    failing_since: float | None
    pending_since: float | None
    last_success: float | None
    written: int
    failures: int
    batch_in_flight: int
    last_error: str | None
    rows_too_large: int
    queue_depth: int
    queue_high_water: bool
    rows_unconvertible: int = 0


class ConfirmedWriter:
    def __init__(
        self,
        take_batch: TakeBatch,
        open_stream: StreamFactory,
        batch_size: int = 500,
        linger_s: float = 0.2,
        idle_poll_s: float = 1.0,
        queue_depth: Callable[[], int] = lambda: 0,
        queue_high_water: int | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        on_written: Callable[[list[str]], None] | None = None,
        name: str = "zerobus",
        batch: list[str] | None = None,
    ):
        self._take_batch = take_batch
        self._open_stream = open_stream
        self._batch_size = batch_size
        self._linger_s = linger_s
        self._idle_poll_s = idle_poll_s
        self._queue_depth = queue_depth
        self._high_water = queue_high_water
        self._clock = clock
        self._sleep = sleep
        self._listeners: list[Callable[[list[str]], None]] = [on_written] if on_written else []
        self._name = name

        self._stream: RowStream | None = None
        self._batch: list[str] = list(batch or [])   # current, not yet acknowledged (an earlier writer's to resend)
        self._backoff = MIN_BACKOFF_S
        self._running = True
        self._finish_by: float | None = None     # finish(): stop once drained, or at this time
        self._idle = False                      # the last take found nothing to take
        self._take_failed = False               # failing_since was set by a take, not by Zerobus
        self._written = self._failures = self._too_large = self._unconvertible = 0
        self._failing_since: float | None = None
        self._pending_since: float | None = None
        self._last_success: float | None = None
        self._last_error: str | None = None

    async def run(self) -> None:
        """Writes until ``stop()``, or until drained after ``finish()``; closes the stream at the end."""
        try:
            while self._running:
                await self.step()
                if self._finish_by is not None:
                    if self._idle and not self._batch:
                        break
                    if self._clock() >= self._finish_by:
                        logger.warning("[%s] stopping with %d rows unacknowledged and %d queued",
                                       self._name, len(self._batch), self._queue_depth())
                        break
        finally:
            await self._close_stream()

    async def step(self) -> None:
        """One pass: open a stream if needed, take a batch if none is pending, write it."""
        if self._stream is None and not await self._open():
            return
        if not self._batch:
            try:
                self._batch = await self._take()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - retried, the task must not end
                self._idle = False
                self._take_failed = True
                self._failed("taking rows from the queue", e)
                await self._back_off()
                return
            if self._take_failed:
                # the queue works again; with no data arriving no acknowledgement would ever clear the failure
                # (an OFFLINE primary host gets none), so this does
                self._take_failed = False
                self._succeeded()
            if not self._batch:
                return
        if self._pending_since is None:
            self._pending_since = self._clock()
        try:
            offset = await self._stream.ingest(self._batch)
            await self._stream.wait_for(offset)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - any failure: the same batch goes out again on a fresh stream
            self._failed(f"write of {len(self._batch)} rows", e)
            await self._close_stream()
            await self._back_off()
            return
        acked, self._batch = self._batch, []
        self._written += len(acked)
        self._pending_since = None
        self._succeeded()
        for listener in self._listeners:
            try:
                listener(acked)
            except Exception as e:  # noqa: BLE001 - a listener must not stop writing
                logger.warning("[%s] handling the written batch failed: %s", self._name, e)

    def add_listener(self, listener: Callable[[list[str]], None]) -> None:
        """Called with each batch of JSON records once Zerobus acknowledged it (keep it quick)."""
        self._listeners.append(listener)

    def remove_listener(self, listener: Callable[[list[str]], None]) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    def finish(self, timeout_s: float) -> None:
        """Ends ``run()`` once everything queued is acknowledged (a take finds nothing and no batch is
        pending), or after ``timeout_s``; the caller awaits the ``run()`` task."""
        self._finish_by = self._clock() + timeout_s

    def stop(self) -> None:
        """Ends ``run()`` after the current step, written or not."""
        self._running = False

    def unacknowledged(self) -> list[str]:
        """The batch sent but not acknowledged (after ``run()`` ended: for the next writer to resend)."""
        return list(self._batch)

    def status(self) -> WriterStatus:
        depth = self._queue_depth()
        return WriterStatus(
            stream_open=self._stream is not None, failing_since=self._failing_since, pending_since=self._pending_since,
            last_success=self._last_success, written=self._written, failures=self._failures,
            batch_in_flight=len(self._batch), last_error=self._last_error, rows_too_large=self._too_large,
            queue_depth=depth, queue_high_water=self._high_water is not None and depth >= self._high_water,
            rows_unconvertible=self._unconvertible)

    def skip_row(self, topic: Any, e: Exception) -> None:
        """A row that can't be made a JSON record: skipped and counted (it would fail on every resend)."""
        self._unconvertible += 1
        logger.error("[%s] row skipped: it can't be written as a Zerobus record (topic %s): %s: %s",
                     self._name, topic, type(e).__name__, e)

    async def _take(self) -> list[str]:
        rows = await self._take_batch(self._batch_size, self._linger_s, self._idle_poll_s)
        self._idle = not rows                   # rows taken but all skipped don't make the queue empty
        batch = []
        for row in rows:
            try:
                text = json.dumps(row, separators=(",", ":"), ensure_ascii=False, allow_nan=False)   # NaN isn't JSON
            except (TypeError, ValueError) as e:
                self.skip_row(row.get("msg_topic") if isinstance(row, dict) else None, e)
                continue
            size = len(text.encode("utf-8"))
            if size > MAX_RECORD_BYTES:
                self._too_large += 1
                logger.warning("[%s] row of %d bytes skipped: over Zerobus's %d-byte limit (topic %s)",
                               self._name, size, MAX_RECORD_BYTES, row.get("msg_topic"))
                continue
            batch.append(text)
        return batch

    async def _open(self) -> bool:
        try:
            self._stream = await self._open_stream()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - opening doubles as the health probe
            self._failed("opening a Zerobus stream", e)
            await self._back_off()
            return False
        if not self._batch:
            self._succeeded()                   # nothing to resend: the open is the health probe
        # with a batch to resend only its acknowledgement counts (opens may work while writes keep failing)
        return True

    def _succeeded(self) -> None:
        if self._failing_since is not None:
            logger.info("[%s] Zerobus writes recovered after failing since %s", self._name, time.ctime(self._failing_since))
        self._failing_since = None
        self._last_success = self._clock()
        self._backoff = MIN_BACKOFF_S

    def _failed(self, what: str, e: Exception) -> None:
        self._failures += 1
        self._last_error = f"{what}: {type(e).__name__}: {e}"
        if self._failing_since is None:
            self._failing_since = self._clock()
        logger.warning("[%s] Zerobus %s failed (retrying in %.0f s): %s", self._name, what, self._backoff, e)

    async def _back_off(self) -> None:
        await self._sleep(self._backoff)
        self._backoff = min(self._backoff * 2, MAX_BACKOFF_S)

    async def _close_stream(self) -> None:
        if self._stream is not None:
            stream, self._stream = self._stream, None
            try:
                await stream.close()
            except Exception as e:  # noqa: BLE001 - a broken stream may not close cleanly
                logger.debug("[%s] closing a Zerobus stream: %s", self._name, e)


class ZerobusRowStreams:
    """``RowStream``s backed by the Zerobus Ingest SDK (JSON records; one SDK instance)."""

    def __init__(self, endpoint: str, workspace_url: str, table: str, client_id: str, client_secret: str):
        from zerobus.sdk.aio import ZerobusSdk

        self._sdk = ZerobusSdk(endpoint, workspace_url, "sparkplug-zerobus-connector")
        self._table, self._client_id, self._client_secret = table, client_id, client_secret

    async def open(self) -> RowStream:
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

        # The writer keeps each batch until it is acknowledged and resends it on a new stream, so the
        # SDK's own recovery only needs to cover short blips
        options = StreamConfigurationOptions(record_type=RecordType.JSON, recovery=True, recovery_retries=2,
                                             recovery_timeout_ms=10_000, server_lack_of_ack_timeout_ms=30_000)
        stream = await self._sdk.create_stream(self._client_id, self._client_secret, TableProperties(self._table), options)
        logger.info("Zerobus stream open to %s", self._table)
        return _SdkRowStream(stream)


class _SdkRowStream:
    def __init__(self, stream: Any):
        self._stream = stream

    async def ingest(self, records: list[str]) -> int:
        return await self._stream.ingest_records_offset(records)

    async def wait_for(self, offset: int) -> None:
        await self._stream.wait_for_offset(offset)

    async def close(self) -> None:
        await self._stream.close()


def convert_rows(records: Any, to_record: Callable[[dict[str, Any]], dict[str, Any]],
                 on_bad_row: Callable[[Any, Exception], None] | None = None) -> list[dict[str, Any]]:
    """``to_record`` of each record; one that fails is left out and handed to ``on_bad_row`` (topic, error)."""
    out = []
    for record in records:
        try:
            out.append(to_record(record))
        except Exception as e:  # noqa: BLE001 - one bad row must not lose the others or end the writer
            if on_bad_row is None:
                logger.error("row skipped: it can't be written as a Zerobus record: %s: %s", type(e).__name__, e)
            else:
                on_bad_row(record.get("msg_topic") if isinstance(record, dict) else None, e)
    return out


async def take_from_backpressure(backpressure: Any, to_record: Callable[[dict[str, Any]], dict[str, Any]],
                                 max_rows: int, linger_s: float, max_wait_s: float,
                                 on_bad_row: Callable[[Any, Exception], None] | None = None) -> list[dict[str, Any]]:
    """A batch from the connector's BackpressureManager (its memory queue): waits up to
    ``max_wait_s`` for the first record, then up to ``linger_s`` for the batch to fill. A record
    ``to_record`` fails on is left out (``convert_rows``)."""
    batch: list[dict[str, Any]] = []
    deadline = time.monotonic() + max_wait_s
    while not batch:
        record = await backpressure.dequeue()
        if record is not None:
            batch += convert_rows([record], to_record, on_bad_row)
        elif time.monotonic() >= deadline:
            return batch
        else:
            await asyncio.sleep(0.02)
    linger_until = time.monotonic() + linger_s
    while len(batch) < max_rows:
        try:
            record = await backpressure.dequeue()
        except Exception as e:  # noqa: BLE001 - the rows taken so far would be lost; the next take meets the error
            logger.warning("taking rows stopped after %d (%s: %s); writing those first", len(batch), type(e).__name__, e)
            break
        if record is not None:
            batch += convert_rows([record], to_record, on_bad_row)
        elif time.monotonic() >= linger_until:
            break
        else:
            await asyncio.sleep(0.01)
    return batch
