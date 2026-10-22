"""The connector's entry point: Sparkplug B (MQTT) to Databricks Delta tables through Zerobus."""

import asyncio
import logging
import signal
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sparkplug_connector.core.config_loader import ConfigLoader
from sparkplug_connector.core.bridge import Bridge
from sparkplug_connector.web.web_server import WebServer
from sparkplug_connector.core.advanced_logging import (
    LogRotationConfig,
    configure_advanced_logging,
    get_logging_manager
)

# Logging with rotation and archiving
log_config = LogRotationConfig(
    max_bytes=100 * 1024 * 1024,     # 100 MB per file
    backup_count=10,                  # Keep 10 backups
    compress_backups=True,            # Compress rotated logs
    archive_enabled=True,             # Enable archiving
    retention_days=90,                # 90-day retention
)

logging_manager = configure_advanced_logging(log_config)
logger = logging.getLogger(__name__)


class Connector:
    """Main application orchestrator."""

    def __init__(self, config_path: str = None):
        """Initialize the connector.

        Args:
            config_path: Path to config.yaml (optional)
        """
        # Load configuration
        self.config_loader = ConfigLoader(config_path=Path(config_path) if config_path else None)
        self.config = self.config_loader.load()

        # Initialize components
        self.bridge = Bridge(self.config)
        self.web_server = WebServer(
            config=self.config,
            bridge=self.bridge,
            config_path=self.config_loader.config_path,   # the page saves sources back to this file
        )

        # Shutdown event
        self._shutdown_event = asyncio.Event()

        # Background bridge task (so Web UI can start immediately)
        self._bridge_task: asyncio.Task | None = None

        # Background log maintenance task
        self._log_maintenance_task: asyncio.Task | None = None

        logger.info("✓ Sparkplug Zerobus connector initialized")

    async def start(self):
        """Start all services."""
        logger.info("=" * 80)
        logger.info("Sparkplug Zerobus connector - Starting")
        logger.info("=" * 80)

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

        # Start log maintenance task
        self._log_maintenance_task = asyncio.create_task(
            logging_manager.start_maintenance_task()
        )
        logger.info("✓ Log maintenance task started")

        logger.info("=" * 80)
        logger.info("✓ Sparkplug Zerobus connector is running")
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

        # Stop web server
        await self.web_server.stop()

        logger.info("✓ Sparkplug Zerobus connector stopped")

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
        prog="python -m sparkplug_connector",
        description="Sparkplug B (MQTT) to Databricks through Zerobus"
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Path to config.yaml (default: sparkplug_connector/config/config.yaml)'
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
    connector = Connector(config_path=args.config)

    try:
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
