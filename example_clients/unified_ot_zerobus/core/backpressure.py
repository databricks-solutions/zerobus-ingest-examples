"""
Backpressure Management for Databricks IoT Connector

Handles data flow control with in-memory queue, encrypted disk spooling,
and Dead Letter Queue for production deployments.
"""

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Dict, Any, Optional
from enum import Enum
import aiofiles
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


class DropPolicy(Enum):
    """Policy for dropping records when queue is full."""
    OLDEST = "oldest"  # Drop oldest record (FIFO)
    NEWEST = "newest"  # Drop newest record (reject incoming)
    REJECT = "reject"  # Raise exception


class BackpressureManager:
    """
    Production-ready backpressure management with multiple tiers:
    1. In-memory queue (fast, limited capacity)
    2. Encrypted disk spool (slower, larger capacity)
    3. Dead Letter Queue (invalid/failed records)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize backpressure manager.

        Args:
            config: Configuration dictionary with backpressure settings
        """
        self.config = config

        # Memory queue settings
        memory_config = config.get('memory_queue', {})
        self.max_queue_size = memory_config.get('max_size', 10000)
        drop_policy_str = memory_config.get('drop_policy', 'oldest')
        self.drop_policy = DropPolicy(drop_policy_str)

        # Disk spool settings
        spool_config = config.get('disk_spool', {})
        self.spool_enabled = spool_config.get('enabled', True)
        self.spool_dir = Path(spool_config.get('path', 'spool'))
        self.max_spool_size_mb = spool_config.get('max_size_mb', 1000)
        self.spool_encryption = spool_config.get('encryption', True)

        # Initialize queue
        self.memory_queue = asyncio.Queue(maxsize=self.max_queue_size)

        # Dead Letter Queue
        self.dlq_dir = self.spool_dir / "dlq"

        # Spool file tracking
        self.spool_files = []
        self.current_spool_file = None
        self.spool_file_counter = 0

        # Encryption key
        self.cipher = None
        if self.spool_encryption:
            self._init_encryption()

        # Metrics
        self.metrics = {
            'records_enqueued': 0,
            'records_dequeued': 0,
            'records_spooled': 0,
            'records_dropped': 0,
            'records_dlq': 0,
            'current_queue_depth': 0,
            'current_spool_size_mb': 0,
        }

        # Ensure directories exist
        self._setup_directories()

        logger.info(f"BackpressureManager initialized: "
                   f"queue_size={self.max_queue_size}, "
                   f"spool_enabled={self.spool_enabled}, "
                   f"spool_size={self.max_spool_size_mb}MB, "
                   f"drop_policy={self.drop_policy.value}")

    def _setup_directories(self) -> None:
        """Create spool and DLQ directories."""
        if self.spool_enabled:
            self.spool_dir.mkdir(parents=True, exist_ok=True)
            self.dlq_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Spool directory: {self.spool_dir}")
            logger.info(f"DLQ directory: {self.dlq_dir}")

    def _init_encryption(self) -> None:
        """Initialize encryption for disk spool."""
        key_file = self.spool_dir.parent / "certs" / "spool_encryption.key"

        if key_file.exists():
            with open(key_file, 'rb') as f:
                key = f.read()
        else:
            # Generate new key
            key = Fernet.generate_key()
            key_file.parent.mkdir(parents=True, exist_ok=True)
            with open(key_file, 'wb') as f:
                f.write(key)
            logger.info(f"Generated new spool encryption key: {key_file}")

        self.cipher = Fernet(key)

    async def enqueue(self, record: Dict[str, Any]) -> bool:
        """
        Enqueue a record with backpressure handling.

        Priority:
        1. Try in-memory queue (fast)
        2. If full and spool enabled, spool to disk
        3. If spool full, apply drop policy

        Args:
            record: Record dictionary to enqueue

        Returns:
            True if enqueued successfully, False if dropped

        Raises:
            ValueError: If drop_policy is REJECT and queue is full
        """
        try:
            # Try in-memory queue first (non-blocking)
            self.memory_queue.put_nowait(record)
            self.metrics['records_enqueued'] += 1
            self.metrics['current_queue_depth'] = self.memory_queue.qsize()
            return True

        except asyncio.QueueFull:
            # Memory queue full, try disk spool
            if self.spool_enabled:
                if await self._spool_to_disk(record):
                    self.metrics['records_spooled'] += 1
                    return True

            # Both queue and spool full, apply drop policy
            return await self._handle_full_queue(record)

    async def dequeue(self) -> Optional[Dict[str, Any]]:
        """
        Dequeue next record.

        Priority:
        1. Memory queue (fast)
        2. Disk spool (slower, but ensures FIFO)

        Returns:
            Next record dictionary, or None if empty
        """
        # Try memory queue first
        if not self.memory_queue.empty():
            record = await self.memory_queue.get()
            self.metrics['records_dequeued'] += 1
            self.metrics['current_queue_depth'] = self.memory_queue.qsize()
            return record

        # If memory queue empty, try loading from spool
        if self.spool_enabled and self.spool_files:
            return await self._load_from_spool()

        return None

    async def _spool_to_disk(self, record: Dict[str, Any]) -> bool:
        """
        Spool record to encrypted disk file.

        Args:
            record: Record to spool

        Returns:
            True if spooled successfully, False if spool full
        """
        # Check spool size limit
        current_size_mb = await self._get_spool_size_mb()
        if current_size_mb >= self.max_spool_size_mb:
            logger.warning(f"Spool directory full: {current_size_mb}MB / {self.max_spool_size_mb}MB")
            return False

        # Create new spool file
        spool_file = self.spool_dir / f"spool_{int(time.time())}_{self.spool_file_counter}.json"
        self.spool_file_counter += 1

        # Serialize and encrypt record
        record_json = json.dumps(record)
        if self.spool_encryption:
            record_data = self.cipher.encrypt(record_json.encode())
        else:
            record_data = record_json.encode()

        # Write to file
        async with aiofiles.open(spool_file, 'wb') as f:
            await f.write(record_data)

        self.spool_files.append(spool_file)
        self.metrics['current_spool_size_mb'] = await self._get_spool_size_mb()

        logger.debug(f"Spooled record to {spool_file}")
        return True

    async def _load_from_spool(self) -> Optional[Dict[str, Any]]:
        """
        Load next record from disk spool (FIFO).

        Returns:
            Record dictionary, or None if no spool files
        """
        if not self.spool_files:
            return None

        spool_file = self.spool_files[0]

        try:
            # Read and decrypt
            async with aiofiles.open(spool_file, 'rb') as f:
                record_data = await f.read()

            if self.spool_encryption:
                record_json = self.cipher.decrypt(record_data).decode()
            else:
                record_json = record_data.decode()

            record = json.loads(record_json)

            # Delete spool file
            spool_file.unlink()
            self.spool_files.pop(0)
            self.metrics['current_spool_size_mb'] = await self._get_spool_size_mb()

            logger.debug(f"Loaded record from spool: {spool_file}")
            return record

        except Exception as e:
            logger.error(f"Failed to load from spool {spool_file}: {e}")
            # Move to DLQ
            await self._move_to_dlq(spool_file, str(e))
            self.spool_files.pop(0)
            return None

    async def _handle_full_queue(self, record: Dict[str, Any]) -> bool:
        """
        Handle full queue according to drop policy.

        Args:
            record: Record that couldn't be enqueued

        Returns:
            True if record was enqueued (by dropping another), False if rejected

        Raises:
            ValueError: If drop_policy is REJECT
        """
        if self.drop_policy == DropPolicy.REJECT:
            raise ValueError("Queue and spool are full, cannot enqueue")

        elif self.drop_policy == DropPolicy.NEWEST:
            # Reject incoming record
            logger.warning(f"Dropping newest record (queue full)")
            self.metrics['records_dropped'] += 1
            return False

        elif self.drop_policy == DropPolicy.OLDEST:
            # Drop oldest record from queue and enqueue new one
            try:
                dropped_record = self.memory_queue.get_nowait()
                logger.warning(f"Dropping oldest record (queue full)")
                self.metrics['records_dropped'] += 1

                # Try to enqueue new record
                self.memory_queue.put_nowait(record)
                self.metrics['records_enqueued'] += 1
                return True
            except:
                return False

        return False

    async def send_to_dlq(self, record: Dict[str, Any], error: str):
        """
        Send failed/invalid record to Dead Letter Queue.

        Args:
            record: Failed record
            error: Error message
        """
        dlq_file = self.dlq_dir / f"dlq_{int(time.time())}_{self.metrics['records_dlq']}.json"

        dlq_record = {
            'record': record,
            'error': error,
            'timestamp': time.time(),
        }

        async with aiofiles.open(dlq_file, 'w') as f:
            await f.write(json.dumps(dlq_record, indent=2))

        self.metrics['records_dlq'] += 1
        logger.error(f"Sent record to DLQ: {error}")

    async def _move_to_dlq(self, spool_file: Path, error: str):
        """
        Move corrupt spool file to DLQ.

        Args:
            spool_file: Path to spool file
            error: Error message
        """
        dlq_file = self.dlq_dir / f"dlq_spool_{spool_file.name}"
        spool_file.rename(dlq_file)

        # Create error file
        error_file = dlq_file.with_suffix('.error')
        async with aiofiles.open(error_file, 'w') as f:
            await f.write(f"Error: {error}\nTimestamp: {time.time()}\n")

        self.metrics['records_dlq'] += 1
        logger.error(f"Moved corrupt spool file to DLQ: {dlq_file}")

    async def _get_spool_size_mb(self) -> float:
        """
        Get current spool directory size in MB.

        Returns:
            Size in megabytes
        """
        if not self.spool_dir.exists():
            return 0.0

        total_size = sum(f.stat().st_size for f in self.spool_dir.rglob('*') if f.is_file())
        return total_size / (1024 * 1024)

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current backpressure metrics.

        Returns:
            Metrics dictionary
        """
        self.metrics['current_queue_depth'] = self.memory_queue.qsize()
        return self.metrics.copy()

    async def clear(self):
        """Clear all queues and spool (for testing/maintenance)."""
        # Clear memory queue
        while not self.memory_queue.empty():
            try:
                self.memory_queue.get_nowait()
            except:
                break

        # Delete spool files
        if self.spool_enabled:
            for spool_file in self.spool_files:
                try:
                    spool_file.unlink()
                except:
                    pass
            self.spool_files.clear()

        logger.info("Backpressure manager cleared")
