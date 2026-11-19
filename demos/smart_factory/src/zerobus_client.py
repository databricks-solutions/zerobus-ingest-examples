"""
ZeroBus Ingest Client Wrapper.

Pushes IoT sensor events to Unity Catalog Delta tables via ZeroBus.
Falls back to SQL INSERT via statement execution if ZeroBus SDK is unavailable.
"""

import logging
import os
from datetime import datetime

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# Try to import ZeroBus SDK
try:
    from zerobus.sdk.aio import ZerobusSdk as AsyncZerobusSdk
    from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
    ZEROBUS_SDK_AVAILABLE = True
except ImportError:
    ZEROBUS_SDK_AVAILABLE = False
    logger.info("ZeroBus SDK not installed, will use SQL INSERT fallback")


class ZeroBusClient:
    """Wraps ZeroBus ingest API with SQL INSERT fallback."""

    def __init__(self, table_name: str | None = None):
        self.table_name = table_name or os.environ["ZEROBUS_TABLE"]
        self.w = WorkspaceClient()
        self._use_fallback = not ZEROBUS_SDK_AVAILABLE
        self._warehouse_id = os.getenv("WAREHOUSE_ID")
        self._stream = None
        self._zerobus_sdk = None

    async def _init_zerobus_stream(self):
        """Initialize ZeroBus async stream."""
        if not ZEROBUS_SDK_AVAILABLE or self._stream is not None:
            return

        try:
            server_endpoint = os.getenv("ZEROBUS_SERVER_ENDPOINT")
            workspace_url = os.getenv("DATABRICKS_HOST", "")
            client_id = os.getenv("DATABRICKS_CLIENT_ID", "")
            client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "")

            if not server_endpoint:
                # Derive from workspace URL if not set
                # Pattern: <workspace-id>.zerobus.<region>.cloud.databricks.com
                logger.warning("ZEROBUS_SERVER_ENDPOINT not set, falling back to SQL")
                self._use_fallback = True
                return

            self._zerobus_sdk = AsyncZerobusSdk(server_endpoint, workspace_url)
            options = StreamConfigurationOptions(record_type=RecordType.JSON)
            table_props = TableProperties(self.table_name)

            self._stream = await self._zerobus_sdk.create_stream(
                client_id, client_secret, table_props, options
            )
            logger.info(f"ZeroBus stream created for table: {self.table_name}")
        except Exception as e:
            logger.warning(f"ZeroBus stream init failed ({e}), using SQL INSERT fallback")
            self._use_fallback = True

    async def push_events(self, events: list[dict]) -> bool:
        """Push sensor events to the landing table."""
        if not events:
            return True

        try:
            if not self._use_fallback and self._stream is None:
                await self._init_zerobus_stream()

            if self._use_fallback:
                return self._push_via_sql(events)
            else:
                return await self._push_via_zerobus(events)
        except Exception as e:
            logger.error(f"Failed to push {len(events)} events: {e}")
            return False

    async def _push_via_zerobus(self, events: list[dict]) -> bool:
        """Push events via ZeroBus async SDK."""
        try:
            offset = await self._stream.ingest_records_nowait(events)
            logger.debug(f"ZeroBus: pushed {len(events)} events")
            return True
        except Exception as e:
            logger.warning(f"ZeroBus push failed ({e}), falling back to SQL")
            self._use_fallback = True
            return self._push_via_sql(events)

    def _push_via_sql(self, events: list[dict]) -> bool:
        """Fallback: push events via SQL INSERT using statement execution."""
        try:
            values_clauses = []
            for e in events:
                ts = e["timestamp"]
                if isinstance(ts, str):
                    ts_sql = f"TIMESTAMP '{ts}'"
                else:
                    ts_sql = f"TIMESTAMP '{ts.isoformat()}'"

                values_clauses.append(
                    f"('{e['machine_id']}', '{e['machine_type']}', "
                    f"'{e['sensor_name']}', {e['value']}, '{e['unit']}', "
                    f"{ts_sql}, {str(e.get('is_fault', False)).lower()})"
                )

            sql = (
                f"INSERT INTO {self.table_name} "
                f"(machine_id, machine_type, sensor_name, value, unit, timestamp, is_fault) "
                f"VALUES {', '.join(values_clauses)}"
            )

            self.w.statement_execution.execute_statement(
                statement=sql,
                warehouse_id=self._warehouse_id or self._get_warehouse_id(),
            )
            logger.debug(f"SQL fallback: inserted {len(events)} events")
            return True
        except Exception as e:
            logger.error(f"SQL INSERT fallback failed: {e}")
            return False

    def _get_warehouse_id(self) -> str:
        """Find an available SQL warehouse."""
        if self._warehouse_id:
            return self._warehouse_id
        warehouses = self.w.warehouses.list()
        for wh in warehouses:
            if wh.state and wh.state.value == "RUNNING":
                self._warehouse_id = wh.id
                return wh.id
        raise RuntimeError("No running SQL warehouse found")

    async def close(self):
        """Close the ZeroBus stream."""
        if self._stream:
            await self._stream.close()
            self._stream = None
