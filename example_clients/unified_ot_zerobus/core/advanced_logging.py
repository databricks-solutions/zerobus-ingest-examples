"""
Advanced Logging with Rotation, Archiving, and Retention.

Features:
- Automatic log rotation (size-based and time-based)
- Log compression and archiving
- Retention policy enforcement
- Audit trail for compliance
- Performance metrics logging
- Log analysis utilities

NIS2 Compliance: Article 21.2(f) - Logging and Monitoring
"""

import asyncio
import gzip
import logging
import logging.handlers
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from unified_connector.core.structured_logging import (
    StructuredLogger,
    SecurityEventCategory,
    get_correlation_id,
)


@dataclass
class LogRotationConfig:
    """Configuration for log rotation."""

    # Size-based rotation
    max_bytes: int = 100 * 1024 * 1024  # 100 MB per file
    backup_count: int = 10  # Keep 10 backup files

    # Time-based rotation
    when: str = 'midnight'  # midnight, W0-W6 (weekday), or interval
    interval: int = 1  # Interval count

    # Compression
    compress_backups: bool = True  # Compress old log files
    compress_level: int = 6  # Gzip compression level (1-9)

    # Archiving
    archive_enabled: bool = True
    archive_dir: Path = Path.home() / '.unified_connector' / 'logs' / 'archive'

    # Retention policy
    retention_days: int = 90  # Keep logs for 90 days
    min_free_space_mb: int = 1000  # Minimum free space (MB)


@dataclass
class AuditLogEntry:
    """Audit trail log entry for compliance."""

    timestamp: str
    correlation_id: str
    user: str
    action: str
    resource: str
    result: str
    source_ip: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'timestamp': self.timestamp,
            'correlation_id': self.correlation_id,
            'user': self.user,
            'action': self.action,
            'resource': self.resource,
            'result': self.result,
            'source_ip': self.source_ip or 'unknown',
            'details': self.details or {},
            'log_type': 'audit',
        }


@dataclass
class PerformanceMetric:
    """Performance metric log entry."""

    timestamp: str
    metric_name: str
    metric_value: float
    metric_unit: str
    component: str
    correlation_id: Optional[str] = None
    tags: Optional[Dict[str, str]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'timestamp': self.timestamp,
            'metric_name': self.metric_name,
            'metric_value': self.metric_value,
            'metric_unit': self.metric_unit,
            'component': self.component,
            'correlation_id': self.correlation_id or 'none',
            'tags': self.tags or {},
            'log_type': 'performance',
        }


class CompressingRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """
    Rotating file handler that compresses old log files.

    When a log file reaches max size, it rotates and compresses the old file.
    """

    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0,
                 encoding=None, delay=False, compress=True, compress_level=6):
        """
        Initialize handler with compression support.

        Args:
            filename: Log file path
            mode: File open mode
            maxBytes: Maximum file size before rotation
            backupCount: Number of backup files to keep
            encoding: File encoding
            delay: Delay file opening
            compress: Enable compression of rotated files
            compress_level: Gzip compression level (1-9)
        """
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)
        self.compress = compress
        self.compress_level = compress_level

    def doRollover(self):
        """
        Do a rollover and compress the old file.
        """
        # Close current file
        if self.stream:
            self.stream.close()
            self.stream = None

        # Rotate files
        if self.backupCount > 0:
            for i in range(self.backupCount - 1, 0, -1):
                sfn = self.rotation_filename(f"{self.baseFilename}.{i}")
                dfn = self.rotation_filename(f"{self.baseFilename}.{i + 1}")

                # Check for compressed version
                if os.path.exists(f"{sfn}.gz"):
                    sfn = f"{sfn}.gz"
                    dfn = f"{dfn}.gz"

                if os.path.exists(sfn):
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)

            # Rotate current file to .1
            dfn = self.rotation_filename(self.baseFilename + ".1")
            if os.path.exists(dfn):
                os.remove(dfn)
            self.rotate(self.baseFilename, dfn)

            # Compress the rotated file
            if self.compress:
                self._compress_file(dfn)

        # Open new log file
        if not self.delay:
            self.stream = self._open()

    def _compress_file(self, filepath: str):
        """
        Compress a log file using gzip.

        Args:
            filepath: Path to file to compress
        """
        try:
            gz_filepath = f"{filepath}.gz"
            with open(filepath, 'rb') as f_in:
                with gzip.open(gz_filepath, 'wb', compresslevel=self.compress_level) as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Remove original file after successful compression
            os.remove(filepath)
        except Exception as e:
            # If compression fails, keep the original file
            logging.error(f"Failed to compress log file {filepath}: {e}")


class CompressingTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """
    Timed rotating file handler that compresses old log files.

    Rotates based on time interval (e.g., daily, weekly) and compresses old files.
    """

    def __init__(self, filename, when='h', interval=1, backupCount=0,
                 encoding=None, delay=False, utc=False, atTime=None,
                 compress=True, compress_level=6):
        """
        Initialize handler with compression support.

        Args:
            filename: Log file path
            when: Time unit (S, M, H, D, midnight, W0-W6)
            interval: Interval count
            backupCount: Number of backup files to keep
            encoding: File encoding
            delay: Delay file opening
            utc: Use UTC time
            atTime: Specific time for rotation
            compress: Enable compression of rotated files
            compress_level: Gzip compression level (1-9)
        """
        super().__init__(filename, when, interval, backupCount, encoding, delay, utc, atTime)
        self.compress = compress
        self.compress_level = compress_level

    def doRollover(self):
        """
        Do a rollover and compress the old file.
        """
        # Close current file
        if self.stream:
            self.stream.close()
            self.stream = None

        # Get time for rotation suffix
        currentTime = int(time.time())
        dstNow = time.localtime(currentTime)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstNow != dstThen:
                if dstNow:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)

        dfn = self.rotation_filename(self.baseFilename + "." +
                                     time.strftime(self.suffix, timeTuple))

        if os.path.exists(dfn):
            os.remove(dfn)

        self.rotate(self.baseFilename, dfn)

        # Compress the rotated file
        if self.compress:
            self._compress_file(dfn)

        # Delete old files
        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)

        # Open new log file
        if not self.delay:
            self.stream = self._open()

        # Calculate next rollover time
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval
        self.rolloverAt = newRolloverAt

    def _compress_file(self, filepath: str):
        """
        Compress a log file using gzip.

        Args:
            filepath: Path to file to compress
        """
        try:
            gz_filepath = f"{filepath}.gz"
            with open(filepath, 'rb') as f_in:
                with gzip.open(gz_filepath, 'wb', compresslevel=self.compress_level) as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Remove original file after successful compression
            os.remove(filepath)
        except Exception as e:
            # If compression fails, keep the original file
            logging.error(f"Failed to compress log file {filepath}: {e}")


class AdvancedLoggingManager:
    """
    Advanced logging manager with rotation, archiving, and retention.

    Features:
    - Size-based and time-based log rotation
    - Automatic compression of old logs
    - Archive management
    - Retention policy enforcement
    - Audit trail logging
    - Performance metrics logging
    """

    def __init__(self, config: LogRotationConfig):
        """
        Initialize advanced logging manager.

        Args:
            config: Log rotation configuration
        """
        self.config = config
        self.log_dir = Path('/tmp/unified_connector_logs')
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Ensure archive directory exists
        if self.config.archive_enabled:
            self.config.archive_dir.mkdir(parents=True, exist_ok=True)

        # Separate loggers for different purposes
        self.main_logger = self._setup_main_logger()
        self.audit_logger = self._setup_audit_logger()
        self.performance_logger = self._setup_performance_logger()

        # Structured logger for security events
        self.security_logger = StructuredLogger(
            logging.getLogger('unified_connector.security')
        )

    def _setup_main_logger(self) -> logging.Logger:
        """
        Setup main application logger with rotation.

        Returns:
            Configured logger
        """
        logger = logging.getLogger('unified_connector')
        logger.setLevel(logging.INFO)
        logger.propagate = False

        # Console handler (no rotation needed)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # File handler with size-based rotation and compression
        main_log_file = self.log_dir / 'unified_connector.log'
        file_handler = CompressingRotatingFileHandler(
            filename=str(main_log_file),
            maxBytes=self.config.max_bytes,
            backupCount=self.config.backup_count,
            compress=self.config.compress_backups,
            compress_level=self.config.compress_level,
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        return logger

    def _setup_audit_logger(self) -> logging.Logger:
        """
        Setup audit trail logger (NIS2 compliance).

        Returns:
            Configured audit logger
        """
        logger = logging.getLogger('unified_connector.audit')
        logger.setLevel(logging.INFO)
        logger.propagate = False

        # Audit log with daily rotation (never delete audit logs during retention)
        audit_log_file = self.log_dir / 'audit.log'
        file_handler = CompressingTimedRotatingFileHandler(
            filename=str(audit_log_file),
            when='midnight',
            interval=1,
            backupCount=self.config.retention_days,  # Keep all audit logs
            compress=self.config.compress_backups,
            compress_level=self.config.compress_level,
        )
        file_handler.setLevel(logging.INFO)

        # JSON formatter for audit logs
        class JsonFormatter(logging.Formatter):
            def format(self, record):
                return record.getMessage()

        file_handler.setFormatter(JsonFormatter())
        logger.addHandler(file_handler)

        return logger

    def _setup_performance_logger(self) -> logging.Logger:
        """
        Setup performance metrics logger.

        Returns:
            Configured performance logger
        """
        logger = logging.getLogger('unified_connector.performance')
        logger.setLevel(logging.INFO)
        logger.propagate = False

        # Performance log with hourly rotation
        perf_log_file = self.log_dir / 'performance.log'
        file_handler = CompressingTimedRotatingFileHandler(
            filename=str(perf_log_file),
            when='H',
            interval=1,
            backupCount=24 * 7,  # Keep 7 days of hourly logs
            compress=self.config.compress_backups,
            compress_level=self.config.compress_level,
        )
        file_handler.setLevel(logging.INFO)

        # JSON formatter for performance logs
        class JsonFormatter(logging.Formatter):
            def format(self, record):
                return record.getMessage()

        file_handler.setFormatter(JsonFormatter())
        logger.addHandler(file_handler)

        return logger

    def log_audit(self, entry: AuditLogEntry):
        """
        Log an audit trail entry (NIS2 compliance).

        Args:
            entry: Audit log entry
        """
        self.audit_logger.info(json.dumps(entry.to_dict(), default=str))

    def log_performance(self, metric: PerformanceMetric):
        """
        Log a performance metric.

        Args:
            metric: Performance metric
        """
        self.performance_logger.info(json.dumps(metric.to_dict(), default=str))

    def log_user_action(self, user: str, action: str, resource: str,
                       result: str, source_ip: Optional[str] = None,
                       **details):
        """
        Log a user action for audit trail.

        Args:
            user: User email/ID
            action: Action performed
            resource: Resource affected
            result: Result (success/failure)
            source_ip: Source IP address
            **details: Additional details
        """
        entry = AuditLogEntry(
            timestamp=datetime.utcnow().isoformat() + 'Z',
            correlation_id=get_correlation_id() or 'none',
            user=user,
            action=action,
            resource=resource,
            result=result,
            source_ip=source_ip,
            details=details if details else None,
        )
        self.log_audit(entry)

    def log_metric(self, metric_name: str, metric_value: float,
                  metric_unit: str, component: str, **tags):
        """
        Log a performance metric.

        Args:
            metric_name: Name of the metric
            metric_value: Numeric value
            metric_unit: Unit of measurement
            component: Component name
            **tags: Additional tags
        """
        metric = PerformanceMetric(
            timestamp=datetime.utcnow().isoformat() + 'Z',
            metric_name=metric_name,
            metric_value=metric_value,
            metric_unit=metric_unit,
            component=component,
            correlation_id=get_correlation_id(),
            tags=tags if tags else None,
        )
        self.log_performance(metric)

    async def archive_old_logs(self):
        """
        Archive old log files to archive directory.

        Moves compressed log files older than 30 days to archive.
        """
        if not self.config.archive_enabled:
            return

        archive_threshold = datetime.now() - timedelta(days=30)
        archived_count = 0

        for log_file in self.log_dir.glob('*.log.*'):
            # Skip if not compressed
            if not log_file.suffix == '.gz':
                continue

            # Check file age
            file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
            if file_time < archive_threshold:
                # Move to archive
                archive_path = self.config.archive_dir / log_file.name
                shutil.move(str(log_file), str(archive_path))
                archived_count += 1

        if archived_count > 0:
            self.main_logger.info(f"Archived {archived_count} old log files")

    async def enforce_retention_policy(self):
        """
        Enforce log retention policy.

        Deletes log files older than retention period and ensures minimum free space.
        """
        retention_threshold = datetime.now() - timedelta(days=self.config.retention_days)
        deleted_count = 0

        # Delete old files from archive
        if self.config.archive_enabled and self.config.archive_dir.exists():
            for log_file in self.config.archive_dir.glob('*.log.*'):
                file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
                if file_time < retention_threshold:
                    log_file.unlink()
                    deleted_count += 1

        # Check free space and delete oldest files if needed
        statvfs = os.statvfs(self.log_dir)
        free_space_mb = (statvfs.f_frsize * statvfs.f_bavail) / (1024 * 1024)

        if free_space_mb < self.config.min_free_space_mb:
            # Delete oldest archived files until we have enough space
            if self.config.archive_enabled and self.config.archive_dir.exists():
                archived_files = sorted(
                    self.config.archive_dir.glob('*.log.*'),
                    key=lambda p: p.stat().st_mtime
                )

                for log_file in archived_files:
                    if free_space_mb >= self.config.min_free_space_mb:
                        break

                    file_size_mb = log_file.stat().st_size / (1024 * 1024)
                    log_file.unlink()
                    deleted_count += 1
                    free_space_mb += file_size_mb

        if deleted_count > 0:
            self.main_logger.info(f"Deleted {deleted_count} old log files per retention policy")

    async def start_maintenance_task(self):
        """
        Start background task for log maintenance.

        Runs daily to archive old logs and enforce retention policy.
        """
        while True:
            try:
                await asyncio.sleep(86400)  # 24 hours

                await self.archive_old_logs()
                await self.enforce_retention_policy()

                self.main_logger.info("Log maintenance completed")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.main_logger.error(f"Error in log maintenance: {e}", exc_info=True)


# Global instance
_logging_manager: Optional[AdvancedLoggingManager] = None


def get_logging_manager(config: Optional[LogRotationConfig] = None) -> AdvancedLoggingManager:
    """
    Get global logging manager instance.

    Args:
        config: Optional configuration (used on first call)

    Returns:
        AdvancedLoggingManager instance
    """
    global _logging_manager
    if _logging_manager is None:
        if config is None:
            config = LogRotationConfig()
        _logging_manager = AdvancedLoggingManager(config)
    return _logging_manager


def configure_advanced_logging(config: Optional[LogRotationConfig] = None):
    """
    Configure advanced logging with rotation and archiving.

    Args:
        config: Log rotation configuration
    """
    if config is None:
        config = LogRotationConfig()

    # Initialize logging manager
    manager = get_logging_manager(config)

    # Replace root logger handlers
    root_logger = logging.getLogger()
    root_logger.handlers = []
    root_logger.addHandler(manager.main_logger.handlers[0])  # Console
    root_logger.addHandler(manager.main_logger.handlers[1])  # File

    return manager
