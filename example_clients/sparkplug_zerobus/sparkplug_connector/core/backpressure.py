"""The queue between a source and its target table's writer: bounded, in memory, with a drop policy.

A primary host goes OFFLINE once its table's queue is 80% full (``core/confirmed_writer.py``,
``sparkplug/host.py``), so the edges buffer and the queue rarely fills; a plain subscriber can fill it, and
then the drop policy decides. Rows that must survive a crash go through the write-ahead log (``core/wal.py``).
"""

import asyncio
import logging
from enum import Enum
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


class DropPolicy(Enum):
    """Policy for dropping records when queue is full."""
    OLDEST = "oldest"  # Drop oldest record (FIFO)
    NEWEST = "newest"  # Drop newest record (reject incoming)
    REJECT = "reject"  # Raise exception


class BackpressureManager:
    """A bounded in-memory queue (``memory_queue.max_size``, default 10000) with a drop policy
    (``memory_queue.drop_policy``: oldest, newest or reject; default oldest)."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        memory_config = config.get('memory_queue', {})
        self.max_queue_size = int(memory_config.get('max_size', 10000))
        self.drop_policy = DropPolicy(memory_config.get('drop_policy', 'oldest'))
        self.memory_queue: asyncio.Queue = asyncio.Queue(maxsize=self.max_queue_size)
        # Called with each record the drop policy throws away (the bridge's write-ahead log marks it done)
        self.on_drop: Optional[Callable[[Dict[str, Any]], None]] = None
        self.metrics = {
            'records_enqueued': 0,
            'records_dequeued': 0,
            'records_dropped': 0,
            'current_queue_depth': 0,
        }
        if (config.get('disk_spool') or {}).get('enabled'):
            logger.warning("backpressure.disk_spool is not supported (ignored): the write-ahead log, zerobus.wal, "
                           "keeps rows across a crash")

    async def enqueue(self, record: Dict[str, Any]) -> bool:
        """Queue a record; when the queue is full the drop policy decides.

        Returns:
            True if queued, False if dropped

        Raises:
            ValueError: If drop_policy is REJECT and queue is full
        """
        try:
            self.memory_queue.put_nowait(record)
        except asyncio.QueueFull:
            return self._handle_full_queue(record)
        self.metrics['records_enqueued'] += 1
        self.metrics['current_queue_depth'] = self.memory_queue.qsize()
        return True

    async def dequeue(self) -> Optional[Dict[str, Any]]:
        """The next record, or None if the queue is empty."""
        try:
            record = self.memory_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None
        self.metrics['records_dequeued'] += 1
        self.metrics['current_queue_depth'] = self.memory_queue.qsize()
        return record

    def _handle_full_queue(self, record: Dict[str, Any]) -> bool:
        if self.drop_policy == DropPolicy.REJECT:
            raise ValueError("Queue is full, cannot enqueue")
        if self.drop_policy == DropPolicy.NEWEST:
            logger.warning("Dropping newest record (queue full)")
            self.metrics['records_dropped'] += 1
            self.dropped(record)
            return False
        # OLDEST: make room by dropping the oldest record
        try:
            dropped_record = self.memory_queue.get_nowait()
        except asyncio.QueueEmpty:
            return False
        logger.warning("Dropping oldest record (queue full)")
        self.metrics['records_dropped'] += 1
        self.dropped(dropped_record)
        try:
            self.memory_queue.put_nowait(record)
        except asyncio.QueueFull:
            return False
        self.metrics['records_enqueued'] += 1
        return True

    def dropped(self, record: Dict[str, Any]) -> None:
        """Tells ``on_drop`` about a record that won't be delivered."""
        if self.on_drop is not None:
            try:
                self.on_drop(record)
            except Exception as e:
                logger.warning(f"on_drop failed: {e}")

    def get_metrics(self) -> Dict[str, Any]:
        """Current queue metrics."""
        self.metrics['current_queue_depth'] = self.memory_queue.qsize()
        return self.metrics.copy()
