"""A write-ahead log for one Zerobus target: rows on disk before they are queued, replayed after a crash.

Sparkplug publishes at QoS 0 and the edge buffers only while the primary host is OFFLINE, so a row the
connector has received but Zerobus hasn't acknowledged yet is gone if the process dies: the batch in
flight, and during a Databricks outage everything queued until the host goes OFFLINE. With the log, the
bridge appends each row before it queues it and marks it done once Zerobus acknowledged it (or the queue
dropped it); on the next start the rows not done are queued again first, with their ``msg_id``.

Layout: segment files ``wal-<first seq>.log`` (one line per row: ``<seq> <crc32 of the JSON> <JSON>``)
and ``checkpoint`` (every row up to this sequence is done). Each row reaches the OS when appended (a
process crash loses nothing); the file is fsynced every ``fsync_interval_s`` off the event loop (a power
loss loses at most that). Segments are deleted once all their rows are done. Rows can be done out of
order (a row the queue refuses, drop_policy newest or reject, is done before the rows queued ahead of it),
so the checkpoint is the highest sequence below which nothing is outstanding; after a crash, rows done
above it are written again (same ``msg_id``: at least once).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

SEQ_FIELD = "_wal_seq"      # a queued record's sequence (never written to Zerobus)
CHECKPOINT = "checkpoint"


@dataclass(frozen=True)
class WalStatus:
    directory: str
    pending: int              # appended or replayed, not done yet
    checkpoint: int
    rows_appended: int
    rows_replayed: int
    rows_not_logged: int      # queued without the log (log full or a write failed)
    bytes: int
    segments: int
    corrupt_lines: int
    failures: int             # write, fsync, checkpoint or delete failures
    last_error: str | None


@dataclass
class _Segment:
    first_seq: int
    path: Path
    last_seq: int             # first_seq - 1 while empty
    bytes: int


class WriteAheadLog:
    def __init__(self, directory: Path, fsync_interval_s: float = 0.2, segment_bytes: int = 16 << 20,
                 max_bytes: int = 1 << 30, name: str = "wal"):
        self.directory = Path(directory)
        self._fsync_interval_s = fsync_interval_s
        self._segment_bytes = segment_bytes
        self._max_bytes = max_bytes
        self._name = name

        self._segments: list[_Segment] = []
        self._current: _Segment | None = None     # the segment appended to (created on the first append)
        self._fd: int | None = None
        self._lock = threading.Lock()             # the fd between appends/rotation (loop) and fsync (thread)
        self._dirty = self._dir_dirty = False
        self._next_seq = 1
        self._checkpoint = 0
        self._outstanding: set[int] = set()
        self._appended = self._replayed = self._not_logged = self._corrupt = self._failures = 0
        self._last_error: str | None = None
        self._full = False

    # ---------------------------------------------------------------- start and stop

    def open(self) -> list[dict[str, Any]]:
        """Reads the log and returns the rows not done yet, in order, each with ``SEQ_FIELD``; they count
        as outstanding until ``done``. Appends then go to a new segment."""
        self.directory.mkdir(parents=True, exist_ok=True)
        try:
            self._checkpoint = int((self.directory / CHECKPOINT).read_text().strip() or 0)
        except FileNotFoundError:
            self._checkpoint = 0
        pending: list[dict[str, Any]] = []
        last = self._checkpoint
        for path in sorted(self.directory.glob("wal-*.log")):
            segment = _Segment(int(path.stem[4:]), path, int(path.stem[4:]) - 1, path.stat().st_size)
            for seq, record in self._read(path):
                segment.last_seq = max(segment.last_seq, seq)
                if seq > self._checkpoint:
                    record[SEQ_FIELD] = seq
                    pending.append(record)
            if segment.last_seq < segment.first_seq:
                # Not one good line (created just before a crash, or only a torn write): nothing to replay, and
                # the next segment would get this name (it starts at the next sequence) and be appended to it
                try:
                    path.unlink()
                except OSError as e:
                    self._fail(f"deleting {path.name}", e)
                continue
            last = max(last, segment.last_seq)
            self._segments.append(segment)
        pending.sort(key=lambda r: r[SEQ_FIELD])
        self._next_seq = last + 1
        self._outstanding = {r[SEQ_FIELD] for r in pending}
        self._replayed = len(pending)
        self._delete_done_segments()
        if pending or self._corrupt:
            logger.warning("[%s] write-ahead log %s: %d rows not acknowledged before the last stop, queued again "
                           "(%d unreadable lines skipped)", self._name, self.directory, len(pending), self._corrupt)
        return pending

    async def run(self) -> None:
        """Fsyncs every ``fsync_interval_s`` while there are unsynced appends (cancel to stop)."""
        while True:
            await asyncio.sleep(self._fsync_interval_s)
            if self._dirty or self._dir_dirty:
                await asyncio.to_thread(self._sync)

    def close(self) -> None:
        """Fsyncs and closes the current segment (outstanding rows stay in the log for the next start)."""
        with self._lock:
            if self._fd is not None:
                self._fsync_fd()
                os.close(self._fd)
                self._fd = None
            self._current = None
        self._sync()

    # ---------------------------------------------------------------- rows

    def append(self, record: dict[str, Any]) -> int | None:
        """Writes the row (without ``SEQ_FIELD``) and returns its sequence; None if it couldn't be logged
        (then it is only in memory, as without the log)."""
        if self._full and self._bytes() < self._max_bytes * 0.9:
            self._full = False
            logger.info("[%s] write-ahead log below its limit again: logging rows", self._name)
        if self._full or self._bytes() >= self._max_bytes:
            if not self._full:
                self._full = True
                logger.warning("[%s] write-ahead log %s full (%d bytes): rows are queued without it until acknowledged "
                               "rows free space", self._name, self.directory, self._bytes())
            self._not_logged += 1
            return None
        seq = self._next_seq
        text = json.dumps({k: v for k, v in record.items() if k != SEQ_FIELD}, separators=(",", ":"),
                          ensure_ascii=False, default=str).encode("utf-8")
        line = b"%d %08x %s\n" % (seq, zlib.crc32(text), text)
        try:
            if self._current is None or self._current.bytes >= self._segment_bytes:
                self._rotate(seq)
            _write_all(self._fd, line)
        except OSError as e:
            self._fail("writing a row", e)
            self._not_logged += 1
            if self._fd is not None and self._current is not None:
                # Cut off what was written of the line (a full disk takes part of it first): the next row's
                # line would follow it and be unreadable at the next start
                try:
                    os.ftruncate(self._fd, self._current.bytes)
                except OSError as e2:
                    self._fail("cutting off a part-written row", e2)
            return None
        self._next_seq += 1
        self._current.last_seq = seq
        self._current.bytes += len(line)
        self._dirty = True
        self._outstanding.add(seq)
        self._appended += 1
        return seq

    def done(self, seqs: Iterable[int]) -> None:
        """Rows acknowledged by Zerobus or dropped by the queue: the checkpoint moves past them once nothing
        before them is outstanding, and fully done segments are deleted."""
        for seq in seqs:
            self._outstanding.discard(seq)
        low = self._checkpoint             # sequences only grow: each one is stepped over once
        while low + 1 < self._next_seq and low + 1 not in self._outstanding:
            low += 1
        if low <= self._checkpoint:
            return
        self._checkpoint = low
        tmp = self.directory / (CHECKPOINT + ".tmp")
        try:
            tmp.write_text(str(low))
            os.replace(tmp, self.directory / CHECKPOINT)
        except OSError as e:
            self._fail("writing the checkpoint", e)
        self._delete_done_segments()

    def status(self) -> WalStatus:
        return WalStatus(
            directory=str(self.directory), pending=len(self._outstanding), checkpoint=self._checkpoint,
            rows_appended=self._appended, rows_replayed=self._replayed, rows_not_logged=self._not_logged,
            bytes=self._bytes(), segments=len(self._segments), corrupt_lines=self._corrupt,
            failures=self._failures, last_error=self._last_error)

    # ---------------------------------------------------------------- internals

    def _read(self, path: Path):
        """(seq, record) per good line; a bad line (a torn last write, damage) is skipped and counted."""
        data = path.read_bytes()
        lines = data.split(b"\n")
        if lines and lines[-1] == b"":
            lines.pop()
        for line in lines:
            try:
                seq_text, crc_text, text = line.split(b" ", 2)
                if zlib.crc32(text) != int(crc_text, 16):
                    raise ValueError("checksum mismatch")
                yield int(seq_text), json.loads(text)
            except ValueError as e:
                self._corrupt += 1
                logger.warning("[%s] write-ahead log %s: unreadable line skipped (%s)", self._name, path.name, e)

    def _rotate(self, first_seq: int) -> None:
        path = self.directory / f"wal-{first_seq:020d}.log"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with self._lock:
            if self._fd is not None:
                self._fsync_fd()
                os.close(self._fd)
            self._fd = fd
        self._current = _Segment(first_seq, path, first_seq - 1, 0)
        self._segments.append(self._current)
        self._dir_dirty = True
        self._delete_done_segments()

    def _sync(self) -> None:
        with self._lock:
            if self._fd is not None and self._dirty:
                self._dirty = False
                self._fsync_fd()
        if self._dir_dirty:
            self._dir_dirty = False
            try:
                fd = os.open(self.directory, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except OSError as e:
                self._fail("fsync of the directory", e)

    def _fsync_fd(self) -> None:
        try:
            os.fsync(self._fd)
        except OSError as e:
            self._fail("fsync", e)

    def _delete_done_segments(self) -> None:
        while self._segments and self._segments[0] is not self._current and self._segments[0].last_seq <= self._checkpoint:
            segment = self._segments.pop(0)
            try:
                segment.path.unlink(missing_ok=True)
            except OSError as e:
                self._fail(f"deleting {segment.path.name}", e)

    def _bytes(self) -> int:
        return sum(s.bytes for s in self._segments)

    def _fail(self, what: str, e: OSError) -> None:
        self._failures += 1
        self._last_error = f"{what}: {e}"
        logger.warning("[%s] write-ahead log %s: %s failed: %s", self._name, self.directory, what, e)


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]
