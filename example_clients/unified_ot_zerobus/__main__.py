"""Unified OT Zerobus Connector Client - Main Entry Point.

Enterprise-grade connector for OPC-UA, MQTT, and Modbus to Databricks Delta tables.
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from unified_connector.core.config_loader import ConfigLoader
from unified_connector.core.unified_bridge import UnifiedBridge
from unified_connector.core.discovery import DiscoveryService
from unified_connector.web.web_server import WebServer
from unified_connector.core.advanced_logging import (
    LogRotationConfig,
    configure_advanced_logging,
    get_logging_manager
)

# Configure advanced logging with rotation and archiving (NIS2 compliance)
log_config = LogRotationConfig(
    max_bytes=100 * 1024 * 1024,     # 100 MB per file
    backup_count=10,                  # Keep 10 backups
    compress_backups=True,            # Compress rotated logs
    archive_enabled=True,             # Enable archiving
    retention_days=90,                # 90-day retention
)

logging_manager = configure_advanced_logging(log_config)
logger = logging.getLogger(__name__)


class UnifiedConnector:
    """Main application orchestrator."""

    def __init__(self, config_path: str = None):
        """Initialize unified connector.

        Args:
            config_path: Path to config.yaml (optional)
        """
        # Load configuration
        self.config_loader = ConfigLoader(config_path=Path(config_path) if config_path else None)
        self.config = self.config_loader.load()

        # Initialize components
        self.bridge = UnifiedBridge(self.config)
        self.discovery = DiscoveryService(self.config.get('discovery', {}))
        self.web_server = WebServer(
            config=self.config,
            bridge=self.bridge,
            discovery=self.discovery
        )

        # Shutdown event
        self._shutdown_event = asyncio.Event()

        # Background bridge task (so Web UI can start immediately)
        self._bridge_task: asyncio.Task | None = None

        # Background log maintenance task
        self._log_maintenance_task: asyncio.Task | None = None

        logger.info("✓ Unified Connector initialized")

    async def start(self):
        """Start all services."""
        logger.info("=" * 80)
        logger.info("Unified OT Zerobus Connector Client - Starting")
        logger.info("=" * 80)

        # Start discovery service
        if self.config.get('discovery', {}).get('enabled', True):
            await self.discovery.start()
            logger.info("✓ Discovery service started")

        # Start web server first (so diagnostics/UI is responsive immediately)
        if self.config.get('web_ui', {}).get('enabled', True):
            await self.web_server.start()
            logger.info("✓ Web UI started")

        # Start bridge in the background (optional)
        if self.config.get('bridge', {}).get('autostart', True):
            self._bridge_task = asyncio.create_task(self.bridge.start())
            logger.info("⏳ Bridge starting in background")
        else:
            logger.info("Bridge autostart disabled (web UI / diagnostics mode)")

        # Start log maintenance task (NIS2 compliance - log retention)
        self._log_maintenance_task = asyncio.create_task(
            logging_manager.start_maintenance_task()
        )
        logger.info("✓ Log maintenance task started")

        logger.info("=" * 80)
        logger.info("✓ Unified Connector is running")
        logger.info("=" * 80)

        # Print connection info
        web_ui_config = self.config.get('web_ui', {})
        if web_ui_config.get('enabled', True):
            host = web_ui_config.get('host', '0.0.0.0')
            port = web_ui_config.get('port', 8080)
            logger.info(f"Web UI: http://{host}:{port}")

        logger.info("Press Ctrl+C to stop")

    async def stop(self):
        """Stop all services gracefully."""
        logger.info("Shutting down...")

        # Stop bridge first (stop data flow)
        # If bridge is still starting, cancel start task
        if self._bridge_task is not None and not self._bridge_task.done():
            self._bridge_task.cancel()
            try:
                await self._bridge_task
            except Exception:
                pass

        await self.bridge.stop()

        # Stop log maintenance task
        if self._log_maintenance_task is not None and not self._log_maintenance_task.done():
            self._log_maintenance_task.cancel()
            try:
                await self._log_maintenance_task
            except asyncio.CancelledError:
                pass

        # Stop discovery
        await self.discovery.stop()

        # Stop web server
        await self.web_server.stop()

        logger.info("✓ Unified Connector stopped")

    async def run(self):
        """Run the connector until interrupted."""
        # Setup signal handlers
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_event_loop().add_signal_handler(
                sig,
                lambda: asyncio.create_task(self._handle_shutdown())
            )

        # Start services
        await self.start()

        # Wait for shutdown signal
        await self._shutdown_event.wait()

        # Stop services
        await self.stop()

    async def _handle_shutdown(self):
        """Handle shutdown signal."""
        logger.info("Shutdown signal received")
        self._shutdown_event.set()


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Unified OT Zerobus Connector Client - OPC-UA, MQTT, Modbus to Databricks"
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Path to config.yaml (default: unified_connector/config/config.yaml)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level'
    )

    args = parser.parse_args()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Create and run connector
    connector = UnifiedConnector(config_path=args.config)

    try:
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
