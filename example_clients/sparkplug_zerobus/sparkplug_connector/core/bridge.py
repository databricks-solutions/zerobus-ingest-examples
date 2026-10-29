"""The bridge: Sparkplug B sources to Databricks tables through Zerobus.

    Sparkplug sources → one queue per target table (+ write-ahead log) → ConfirmedWriter → Zerobus → raw_messages

Each Sparkplug message becomes one ``raw_messages`` row, written unchanged. Every table's batches are kept
until Zerobus acknowledges them (``core/confirmed_writer.py``), so a primary host can follow its table's writer:
ONLINE while Databricks accepts the rows, OFFLINE (the edges buffer) otherwise (``sparkplug/host.py``). After
an NBIRTH is written, the source's NBIRTH procedures are called (``sparkplug/edge_node_schemas.py``). With
``diagnostics.enabled``, the connector's own diagnostics edge nodes go into the default table too
(``sparkplug/diagnostics.py``).
"""

import asyncio
import collections
import dataclasses
import logging
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from sparkplug_connector.protocols.base import ProtocolClient
from sparkplug_connector.protocols.factory import create_protocol_client
from sparkplug_connector.core.backpressure import BackpressureManager
from sparkplug_connector.core.confirmed_writer import ConfirmedWriter, ZerobusRowStreams, convert_rows, take_from_backpressure
from sparkplug_connector.core.wal import SEQ_FIELD, WriteAheadLog
from sparkplug_connector.core import __version__
from sparkplug_connector.core.cert_store import certs_dir, resolve_tls_paths
from sparkplug_connector.core import targets as source_tables
from sparkplug_connector.sparkplug import raw_row
from sparkplug_connector.sparkplug.diagnostics import REFRESH_S, Diagnostics, Tracker
from sparkplug_connector.sparkplug.edge_node_schemas import BirthNotifier, DatabricksSql, nbirth_procedures, quoted_table
from sparkplug_connector.sparkplug.host import HostState, HostSupervisor

logger = logging.getLogger(__name__)

HEALTHY_SESSION_S = 60.0   # a source connection that lasted this long starts its reconnect backoff again
NBIRTH_SOURCES_KEPT = 10000   # NBIRTH msg_ids remembered with their source, for the NBIRTH callers (_owns_nbirth)
# (an NBIRTH forgotten before Zerobus acknowledged it goes to the first caller on its writer: with sources sharing a
# table, more than this many NBIRTHs awaiting Zerobus at once could send one to the other source's procedures)


def _iso_ms(ms: Optional[float]) -> Optional[str]:
    """Epoch milliseconds as UTC ISO 8601 with milliseconds (None stays None)."""
    if ms is None:
        return None
    ms = int(ms)
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(ms // 1000)) + f".{ms % 1000:03d}Z"


def _enabled(value: Any) -> bool:
    """A config switch that may come from the environment as a string."""
    return str(value).strip().lower() not in ('false', '0', 'no', 'off', '', 'none')


@dataclasses.dataclass(frozen=True)
class TargetTable:
    """A Zerobus target table and how to reach it (from zerobus.* and the source's target)."""
    workspace_url: str
    zerobus_endpoint: str
    full_table_name: str
    client_id: str
    client_secret: str


class Bridge:
    """Sparkplug B sources to Databricks tables through Zerobus (one queue and one confirmed writer per table)."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the bridge.

        Args:
            config: Configuration dictionary from config.yaml
        """
        self.config = config
        self.sources = config.get('sources', [])

        # ZeroBus configuration
        self.zerobus_config = config.get('zerobus', {})
        # Config flag (what user configured)
        self.zerobus_config_enabled = self.zerobus_config.get('enabled', False)
        # Runtime flag (whether the writers are running)
        self.zerobus_enabled = False

        # Backpressure: one queue per Zerobus target table, so each table's writer takes only its own sources' rows
        self.backpressure_config = config.get('backpressure', {})
        self.queues: Dict[str, BackpressureManager] = {}
        self._source_queues: Dict[str, BackpressureManager] = {}
        self.backpressure = self._queue(self._get_target_id(self.zerobus_config.get('default_target', {})))

        # The open target tables, and their writers (rows leave the queue only once Zerobus acknowledged them)
        self.tables: Dict[str, TargetTable] = {}
        self.writers: Dict[str, ConfirmedWriter] = {}
        self.writer_tasks: Dict[str, asyncio.Task] = {}
        self.drain_timeout_s = float(self.zerobus_config.get('drain_timeout_seconds', 15.0))

        # zerobus.wal: a write-ahead log per target (rows on disk before they are queued, replayed after a
        # crash; core/wal.py), off by default
        self.wal_config = self.zerobus_config.get('wal') or {}
        self.wal_enabled = _enabled(self.wal_config.get('enabled', False))
        self.wals: Dict[str, WriteAheadLog] = {}
        self.wal_tasks: Dict[str, asyncio.Task] = {}
        self._queue_wals: Dict[BackpressureManager, WriteAheadLog] = {}
        # rows the log replays, per target: the writer takes them before the queue (in order, none dropped)
        self._wal_replay: Dict[str, collections.deque] = {}
        # per target, across a Zerobus stop/start: the log sequences taken since the last acknowledgement and
        # the stopped writer's unacknowledged batch (the next writer sends it first)
        self._taken: Dict[str, List[int]] = {}
        self._unacked: Dict[str, List[str]] = {}

        # Sparkplug sources: the primary host's supervisor and the NBIRTH procedure caller
        self.host_supervisors: Dict[str, HostSupervisor] = {}
        self.host_tasks: Dict[str, asyncio.Task] = {}
        self.birth_notifiers: Dict[str, BirthNotifier] = {}
        self._notifier_writers: Dict[str, ConfirmedWriter] = {}
        # msg_id -> the source that handed the NBIRTH on (its caller handles it; the latest NBIRTH_SOURCES_KEPT)
        self._nbirth_sources: "collections.OrderedDict[str, str]" = collections.OrderedDict()

        # Source clients
        self.clients: Dict[str, ProtocolClient] = {}
        self.client_tasks: Dict[str, asyncio.Task] = {}
        # per source, while its MQTT connection fails (e.g. a refused login): the last error, since when (epoch s)
        self.mqtt_errors: Dict[str, Dict[str, Any]] = {}

        # diagnostics: the connector's own edge nodes, rows into the default table (off by default)
        self.diagnostics_config = config.get('diagnostics') or {}
        self.diagnostics: Optional[Diagnostics] = None
        self.diagnostics_task: Optional[asyncio.Task] = None
        self.diagnostics_rows = 0
        if _enabled(self.diagnostics_config.get('enabled', False)):
            tracker = Tracker()
            for source in self.sources:
                if source.get('enabled', True) and source.get('protocol', 'sparkplug') == 'sparkplug':
                    name = source.get('name', '')
                    tracker.add_client(name, str(source.get('client_id') or f'dbx-{name}'), str(source.get('endpoint') or ''))
            self.diagnostics = Diagnostics(tracker, self.diagnostics_config, version=__version__,
                                           primary_host_id=self._primary_host_id)

        # Reconnection settings
        self.reconnect_enabled = True
        self.reconnect_initial_delay_sec = 1.0
        self.reconnect_max_delay_sec = 300.0
        self.reconnect_multiplier = 2.0

        # Metrics
        self.metrics = {
            'records_received': 0,
            'records_enqueued': 0,
            'records_dropped': 0,
            'reconnections': 0,
        }

        self._shutdown = False
        self._stopping_sources: set = set()     # stop_source in progress: their subscription's end isn't an error
        self.running = False
        self._starting = False
        self._start_lock = asyncio.Lock()
        self.started_at: Optional[float] = None    # epoch s: the counters count from here (a restart: a new bridge)

        logger.info(f"Bridge initialized: {len(self.sources)} sources configured")

    async def start_infra(self) -> None:
        """Start the bridge's infrastructure (the target tables' writers) only.

        Does NOT automatically start all configured sources.
        Use start_source()/stop_source() to control sources individually.
        """
        async with self._start_lock:
            if self.running or self._starting:
                logger.info("Bridge already running/starting")
                return
            self._starting = True
            self._shutdown = False

        logger.info("Starting the bridge infrastructure...")

        if self.zerobus_config_enabled:
            logger.info("ZeroBus enabled - opening the target tables...")
            await self._open_targets()
            self.zerobus_enabled = True
        else:
            logger.info("ZeroBus disabled - data collection only mode")
        if self.diagnostics is not None and self.diagnostics_task is None:
            self.diagnostics_task = asyncio.create_task(self._run_diagnostics())

        self.running = True
        self._starting = False
        self.started_at = time.time()
        logger.info("✓ Bridge infrastructure started")

    async def start(self):
        """Start all components (infra + all enabled sources)."""
        await self.start_infra()

        for source_config in self.sources:
            source_name = source_config.get('name', 'unknown')
            if not source_config.get('enabled', True):
                logger.info(f"Source '{source_name}' is disabled, skipping")
                continue

            if source_name in self.clients:
                continue

            try:
                await self._start_client(source_config)
                logger.info(f"✓ Started source: {source_name}")
            except Exception as e:
                logger.error(f"Failed to start source {source_name}: {e}")

        logger.info(f"✓ Bridge started with {len(self.clients)} active sources")

    async def _open_targets(self):
        """The enabled sources' target tables, each with its writer (a disabled source's table is opened when it
        is enabled and started: _ensure_target; until then nothing is opened for it, e.g. the example config's
        source)."""
        self._source_queues.clear()   # the targets may have changed
        targets: Dict[str, Dict[str, Any]] = {}
        for source in self.sources:
            if not source.get('enabled', True):
                continue
            try:
                target_config = source_tables.target_config(source, self.zerobus_config)
            except ValueError as e:
                logger.error(f"[{source.get('name')}] {e}")
                continue
            if target_config.get('workspace_host'):
                targets[self._get_target_id(target_config)] = target_config
        default_target = self.zerobus_config.get('default_target') or {}
        if self.diagnostics is not None and default_target.get('workspace_host'):
            targets.setdefault(self._get_target_id(default_target), default_target)     # the diagnostics' table

        for target_id, target_config in targets.items():
            await self._create_target(target_id, target_config)
            self._start_writer(target_id)
        self._reattach_notifiers()   # Zerobus started again: the running Sparkplug sources' NBIRTH callers

    async def _create_target(self, target_id: str, target_config: Dict[str, Any]) -> None:
        """The table, its write-ahead log and its ConfirmedWriter."""
        try:
            auth = self.zerobus_config.get('auth', {}) or {}
            table = TargetTable(
                workspace_url=target_config['workspace_host'],
                zerobus_endpoint=self.zerobus_config.get('zerobus_endpoint'),
                full_table_name=source_tables.table_name(target_config),
                client_id=auth.get('client_id', ''),
                client_secret=auth.get('client_secret', ''),
            )
            streams = ZerobusRowStreams(table.zerobus_endpoint, table.workspace_url, table.full_table_name,
                                        table.client_id, table.client_secret)
            await self._open_wal(target_id)
            self.writers[target_id] = self._make_writer(target_id, streams.open)
            self.tables[target_id] = table
            logger.info(f"✓ ZeroBus target ready: {target_id} (confirmed writes)")
        except Exception as e:
            logger.error(f"Failed to set up the ZeroBus target {target_id}: {e}")

    def _start_writer(self, target_id: str) -> None:
        writer = self.writers.get(target_id)
        task = self.writer_tasks.get(target_id)
        if writer is None or (task is not None and not task.done()):
            return
        self.writer_tasks[target_id] = asyncio.create_task(self._run_writer(target_id, writer))

    async def _run_writer(self, target_id: str, writer: ConfirmedWriter) -> None:
        logger.info(f"Confirmed writer started for {target_id}")
        await writer.run()
        logger.info(f"Confirmed writer stopped for {target_id}")

    async def _ensure_target(self, source_config: Dict[str, Any]) -> None:
        """While Zerobus runs: the table of a source being started (e.g. a target_table set from the page)."""
        if not self.zerobus_enabled:
            return
        target_config = source_tables.target_config(source_config, self.zerobus_config)
        if not target_config.get('workspace_host'):
            return
        target_id = self._get_target_id(target_config)
        if target_id in self.tables:
            return
        self._source_queues.clear()
        await self._create_target(target_id, target_config)
        self._start_writer(target_id)

    async def _open_wal(self, target_id: str) -> None:
        """The target's write-ahead log (once per process: Zerobus may be stopped and started again), its rows
        not acknowledged before the last stop written first."""
        if not self.wal_enabled or target_id in self.wals:
            return
        cfg = self.wal_config
        wal = WriteAheadLog(Path(cfg.get('path', 'wal')) / self._target_dir(target_id),
                            fsync_interval_s=float(cfg.get('fsync_interval_seconds', 0.2)),
                            segment_bytes=int(float(cfg.get('segment_mb', 16)) * (1 << 20)),
                            max_bytes=int(float(cfg.get('max_size_mb', 1024)) * (1 << 20)), name=target_id)
        pending = wal.open()
        queue = self._queue(target_id)
        self.wals[target_id] = wal
        self._queue_wals[queue] = wal
        queue.on_drop = lambda record: wal.done([record[SEQ_FIELD]] if SEQ_FIELD in record else [])
        # Not through the bounded queue: nothing drains it yet, so rows beyond its size would be dropped (and
        # count as done) or refused. The writer takes them first, before any newer row (a pipeline's watermark
        # on inserted_at would drop them if newer rows were written before them).
        self._wal_replay[target_id] = collections.deque(pending)
        self.wal_tasks[target_id] = asyncio.create_task(wal.run())
        logger.info(f"Write-ahead log for {target_id}: {wal.directory} ({len(pending)} rows to write first)")

    def _make_writer(self, target_id: str, open_stream) -> ConfirmedWriter:
        """A ConfirmedWriter for one target, taking batches from the target's queue."""
        batch = self.zerobus_config.get('batch', {})
        bp = self._queue(target_id)
        wal = self.wals.get(target_id)
        taken = self._taken.setdefault(target_id, [])   # log sequences since the last acknowledgement (one batch in flight)

        replay = self._wal_replay.setdefault(target_id, collections.deque())

        def to_record(record: Dict[str, Any]) -> Dict[str, Any]:
            if SEQ_FIELD in record:
                taken.append(record[SEQ_FIELD])
            return self._to_zerobus_message(record)

        def bad_row(topic: Any, e: Exception) -> None:
            writer.skip_row(topic, e)

        async def take(n: int, linger: float, wait: float) -> List[Dict[str, Any]]:
            if replay:
                return convert_rows([replay.popleft() for _ in range(min(n, len(replay)))], to_record, bad_row)
            kept: List[Dict[str, Any]] = []   # this take's rows: back in front if it is cancelled part-way

            def keep(record: Dict[str, Any]) -> Dict[str, Any]:
                out = to_record(record)
                kept.append(record)
                return out
            try:
                return await take_from_backpressure(bp, keep, n, linger, wait, bad_row)
            except asyncio.CancelledError:
                for record in reversed(kept):
                    if SEQ_FIELD in record and record[SEQ_FIELD] in taken:
                        taken.remove(record[SEQ_FIELD])   # not sent: not done with the next acknowledgement
                    replay.appendleft(record)             # the next writer's first rows
                raise

        writer = ConfirmedWriter(
            take_batch=take,
            open_stream=open_stream,
            batch_size=int(batch.get('max_records', 500)),
            linger_s=float(batch.get('linger_seconds', 0.2)),
            queue_depth=lambda: len(replay) + bp.memory_queue.qsize(),
            queue_high_water=int(bp.max_queue_size * 0.8),
            name=target_id,
            batch=self._unacked.pop(target_id, None),
        )
        if wal is not None:
            def acknowledged(_batch: List[str]) -> None:
                # also covers rows of an earlier take skipped as too large (taken, never sent)
                wal.done(taken)
                taken.clear()
            writer.add_listener(acknowledged)
        return writer

    def _target_dir(self, target_id: str) -> str:
        """A target id as a directory name (the write-ahead log's)."""
        return re.sub(r'[^A-Za-z0-9._-]', '_', target_id).lstrip('.') or 'default'

    def _queue(self, target_id: str) -> BackpressureManager:
        """The queue of one target (created on first use)."""
        queue = self.queues.get(target_id)
        if queue is None:
            queue = self.queues[target_id] = BackpressureManager(self.backpressure_config)
        return queue

    def _queue_for_source(self, source_name: Optional[str]) -> BackpressureManager:
        """The queue of the target a source writes to (the default target's for an unknown source)."""
        queue = self._source_queues.get(source_name)
        if queue is None:
            queue = self._source_queues[source_name] = self._queue(self._get_target_for_source(source_name or ''))
        return queue

    def _get_target_id(self, target_config: Dict[str, Any]) -> str:
        """A target's id: ``<workspace id>.<catalog>.<schema>.<table>``."""
        workspace = target_config.get('workspace_host', '')
        workspace_id = workspace.split('//')[1].split('.')[0] if '//' in workspace else workspace
        return f"{workspace_id}.{source_tables.table_name(target_config)}"

    def _get_target_for_source(self, source_name: str) -> str:
        """The target id a source writes to: its target_table, else zerobus.source_targets.<name>, else the
        default (core/targets.py)."""
        source = next((s for s in self.sources if s.get('name') == source_name), {'name': source_name})
        try:
            return self._get_target_id(source_tables.target_config(source, self.zerobus_config))
        except ValueError:          # a target_table that isn't catalog.schema.table: refused at start
            return self._get_target_id(self.zerobus_config.get('default_target', {}))

    async def _start_client(self, source_config: Dict[str, Any]):
        """Start a single source client with its reconnection loop.

        Args:
            source_config: Source configuration dict
        """
        source_name = source_config.get('name', 'unknown')
        if self._shutdown:      # stopping (or stopped): nothing would stop this client again
            raise RuntimeError(f"The bridge is stopping: source {source_name} not started")
        source_tables.target_config(source_config, self.zerobus_config)   # ValueError: target_table malformed
        await self._ensure_target(source_config)

        client = create_protocol_client(
            protocol_type=source_config.get('protocol', 'sparkplug'),
            source_name=source_name,
            endpoint=source_config['endpoint'],
            config=resolve_tls_paths(source_config, certs_dir(self.config)),   # uploaded files by bare name
            on_record=lambda record: self._on_record(record, source_name),
            on_stats=self._on_stats
        )

        self.clients[source_name] = client
        if self.diagnostics is not None:
            self.diagnostics.tracker.add_client(source_name, str(getattr(client, 'client_id', '') or ''), str(client.endpoint))
        self._attach_sparkplug(source_name, source_config, client)

        task = asyncio.create_task(self._client_lifecycle(source_name, client))
        self.client_tasks[source_name] = task

    def _attach_sparkplug(self, source_name: str, source_config: Dict[str, Any], client) -> None:
        """A Sparkplug source's primary host supervisor and NBIRTH caller, on its target's confirmed writer."""
        target_id = self._get_target_for_source(source_name)
        if client.primary_host_id:
            # follows the table's writer of the moment: none (Zerobus stopped or not started) is OFFLINE, so
            # the edges buffer
            supervisor = HostSupervisor(client, lambda: self._writer_status(target_id),
                                        HostState(client.offline_after_seconds, client.online_after_seconds), name=source_name)
            self.host_supervisors[source_name] = supervisor
            self.host_tasks[source_name] = asyncio.create_task(supervisor.run())
        self._attach_notifier(source_name, source_config)

    def _writer_status(self, target_id: str):
        writer = self.writers.get(target_id)
        return None if writer is None else writer.status()

    def _reattach_notifiers(self) -> None:
        """After the writers were (re)created: each running source's NBIRTH caller on its target's new writer."""
        configs = {s.get('name'): s for s in self.sources}
        for name in self.clients:
            if name in configs:
                try:
                    self._attach_notifier(name, configs[name])
                except Exception as e:  # noqa: BLE001 - one source must not stop Zerobus from starting
                    logger.error(f"[{name}] NBIRTH procedure calls not set up: {e}")

    def _attach_notifier(self, source_name: str, source_config: Dict[str, Any]) -> None:
        """The NBIRTH caller, listening on the target's current writer (moved there if it listened on an
        earlier one; created once a writer exists)."""
        schemas = source_config.get('edge_node_schemas')
        if schemas and _enabled(schemas.get('enabled', True)):
            target_id = self._get_target_for_source(source_name)
            writer = self.writers.get(target_id)
            notifier = self.birth_notifiers.get(source_name)
            if notifier is not None:
                old = self._notifier_writers.pop(source_name, None)
                if old is not None and old is not writer:
                    old.remove_listener(notifier.accept)
                if writer is not None:
                    if old is not writer:
                        writer.add_listener(notifier.accept)
                    self._notifier_writers[source_name] = writer
                return
            table = self.tables.get(target_id)
            if writer is None or table is None:
                logger.warning(f"[{source_name}] no NBIRTH procedure calls until Zerobus runs")
                return
            try:
                procedures = nbirth_procedures(schemas)
                quoted_table(table.full_table_name)
            except ValueError as e:                 # the source still writes its rows
                logger.error(f"[{source_name}] {e}; no NBIRTH procedure calls")
                return
            warehouse_id = str(schemas.get('warehouse_id') or '').strip()
            if not warehouse_id:
                logger.error(f"[{source_name}] edge_node_schemas needs warehouse_id (a SQL warehouse); no NBIRTH procedure calls")
                return
            sql = DatabricksSql(table.workspace_url, table.client_id, table.client_secret, warehouse_id)
            notifier = BirthNotifier(sql, table.full_table_name, procedures,
                                     prefix=str(schemas.get('schema_prefix') or '').strip() or 'nb',
                                     check_views=_enabled(schemas.get('check_views', True)),
                                     schema_only_groups=frozenset({self.diagnostics.group_id}) if self.diagnostics else frozenset(),
                                     owns=self._owns_nbirth(source_name))
            writer.add_listener(notifier.accept)
            self.birth_notifiers[source_name] = notifier
            self._notifier_writers[source_name] = writer
            logger.info(f"[{source_name}] on NBIRTH: CALL {', '.join(procedures)}")

    async def _client_lifecycle(self, source_name: str, client: ProtocolClient):
        """Manage a source client's lifecycle with automatic reconnection.

        Args:
            source_name: Name of the source
            client: Source client instance
        """
        reconnect_delay = self.reconnect_initial_delay_sec

        while not self._shutdown:
            connected_at = None
            try:
                logger.info(f"[{source_name}] Connecting to {client.endpoint}...")
                await client.connect()
                logger.info(f"[{source_name}] ✓ Connected")
                self.mqtt_errors.pop(source_name, None)
                connected_at = time.monotonic()
                self._diagnose_client(source_name, True)

                logger.info(f"[{source_name}] Starting data collection...")
                await client.subscribe()

                # Keep running until shutdown
                while not self._shutdown:
                    await asyncio.sleep(1)

                # Cleanup on shutdown
                await client.disconnect()
                self._diagnose_client(source_name, False)
                reconnect_delay = self.reconnect_initial_delay_sec

                if self._shutdown:
                    break

            except Exception as e:
                if connected_at is not None:
                    self._diagnose_client(source_name, False)
                if self._shutdown or source_name in self._stopping_sources:
                    logger.info(f"[{source_name}] subscription ended: the source is stopping")
                    break
                requested = getattr(client, 'disconnect_requested', False)
                if requested:
                    # the client disconnected itself (a primary host going OFFLINE): it reconnects once allowed
                    logger.info(f"[{source_name}] disconnected by the connector ({e})")
                else:
                    logger.error(f"[{source_name}] Error: {e}")
                    now = time.time()
                    since = (self.mqtt_errors.get(source_name) or {}).get('since', now)
                    self.mqtt_errors[source_name] = {'message': f"{type(e).__name__}: {e}", 'since': since, 'at': now}

                # Cleanup
                try:
                    await client.stop()
                    await client.disconnect()
                except Exception:
                    pass

                if not self.reconnect_enabled or self._shutdown:
                    break

                # Exponential backoff with jitter; a connection that lasted starts it again (one failing right
                # after each connect, e.g. a refused subscribe, backs off more each time)
                if connected_at is not None and time.monotonic() - connected_at >= HEALTHY_SESSION_S:
                    reconnect_delay = self.reconnect_initial_delay_sec
                jitter = reconnect_delay * 0.1 * (2 * (time.time() % 1) - 0.5)
                sleep_time = min(reconnect_delay + jitter, self.reconnect_max_delay_sec)

                (logger.info if requested else logger.warning)(f"[{source_name}] Reconnecting in {sleep_time:.1f}s...")
                self.metrics['reconnections'] += 1

                await asyncio.sleep(sleep_time)

                reconnect_delay = min(
                    reconnect_delay * self.reconnect_multiplier,
                    self.reconnect_max_delay_sec
                )

        logger.info(f"[{source_name}] Client lifecycle ended")

    async def start_source(self, source_name: str) -> Dict[str, Any]:
        """Start a single configured source (does not modify config)."""
        if self._shutdown:
            return {'status': 'error', 'message': 'Bridge is shut down'}

        if source_name in self.clients:
            return {'status': 'ok', 'message': f"Source already active: {source_name}"}

        cfg = next((s for s in self.sources if s.get('name') == source_name), None)
        if not cfg:
            return {'status': 'error', 'message': f"Source not found: {source_name}"}

        if not cfg.get('enabled', True):
            return {'status': 'error', 'message': f"Source is disabled in config: {source_name}"}

        try:
            await self._start_client(cfg)
            logger.info(f"✓ Started source: {source_name}")
            return {'status': 'ok', 'message': f"Source started: {source_name}"}
        except Exception as e:
            logger.error(f"Failed to start source {source_name}: {e}", exc_info=True)
            return {'status': 'error', 'message': str(e)}

    async def stop_source(self, source_name: str) -> Dict[str, Any]:
        """Stop a single active source (does not delete it from config)."""
        if source_name not in self.clients:
            return {'status': 'ok', 'message': f"Source already stopped: {source_name}"}

        self._stopping_sources.add(source_name)
        try:
            return await self._stop_source(source_name)
        finally:
            self._stopping_sources.discard(source_name)

    async def _stop_source(self, source_name: str) -> Dict[str, Any]:
        host_task = self.host_tasks.pop(source_name, None)
        if host_task is not None:
            host_task.cancel()
            await asyncio.gather(host_task, return_exceptions=True)
        self.host_supervisors.pop(source_name, None)
        client = self.clients.get(source_name)
        try:
            if client is not None:
                await client.stop()
                await client.disconnect()
        except Exception as e:
            logger.warning(f"Error stopping source {source_name}: {e}")
        # its NBIRTH caller off the writer, or a restarted source would add a second one
        notifier = self.birth_notifiers.pop(source_name, None)
        writer = self._notifier_writers.pop(source_name, None)
        if notifier is not None:
            if writer is not None:
                writer.remove_listener(notifier.accept)
            await notifier.stop()

        if source_name in self.client_tasks:
            task = self.client_tasks[source_name]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self.client_tasks.pop(source_name, None)

        if client is not None:
            try:
                await client.close()
            except Exception as e:
                logger.warning(f"Error closing source {source_name}: {e}")
        self.clients.pop(source_name, None)
        self.mqtt_errors.pop(source_name, None)
        return {'status': 'ok', 'message': f"Source stopped: {source_name}"}

    def _on_record(self, row: "raw_row.RawRow", source_name: Optional[str] = None):
        """A Sparkplug row from a source: queued for its target table unchanged (no source: the default table)."""
        if source_name is not None and row.message_type == 'NBIRTH':
            self._nbirth_sources[row.msg_id] = source_name
            if len(self._nbirth_sources) > NBIRTH_SOURCES_KEPT:
                self._nbirth_sources.popitem(last=False)
        if self.diagnostics is not None:
            try:
                self.diagnostics.tracker.on_row(row)
            except Exception as e:  # noqa: BLE001 - the counters must not cost the row
                logger.warning(f"[{source_name}] diagnostics counters: {type(e).__name__}: {e}")
        self._on_raw_row(row, self._queue_for_source(source_name))

    def _owns_nbirth(self, source_name: str) -> Callable[[Dict[str, Any]], bool]:
        """Whether an acknowledged NBIRTH row is for this source's caller: the writer reports every row of its
        table, so with sources sharing a table each caller takes the NBIRTHs its source handed on; one no source
        handed on (the diagnostics', a write-ahead log replay) goes to the first caller on that writer."""
        def owns(row: Dict[str, Any]) -> bool:
            owner = self._nbirth_sources.get(row.get('msg_id'))
            if owner is not None:
                return owner == source_name
            writer = self._notifier_writers.get(source_name)
            return next((n for n, w in self._notifier_writers.items() if w is writer), None) == source_name
        return owns

    def _diagnose_client(self, source_name: str, connected: bool) -> None:
        if self.diagnostics is not None:
            self.diagnostics.tracker.on_client(source_name, connected)

    def _primary_host_id(self) -> Optional[str]:
        """The diagnostics' PrimaryHostId: the first enabled Sparkplug source's primary host id (None: none is one)."""
        for source in self.sources:
            if source.get('enabled', True) and str(source.get('primary_host_id') or '').strip():
                return str(source['primary_host_id']).strip()
        return None

    async def _run_diagnostics(self) -> None:
        """The diagnostics' births at start, then every 10 s a DDATA per diagnostics edge node if anything changed.
        An error is logged and the next turn tries again (the births until they are queued)."""
        queue = self.backpressure                   # the default table's
        logger.info(f"Diagnostics: edge nodes in group {self.diagnostics.group_id}, into the default table")
        born = False
        while True:
            try:
                rows = self.diagnostics.refresh_rows() if born else self.diagnostics.start_rows()
                for row in rows:
                    self._enqueue_fast(row.to_zerobus_record(), queue)
                self.diagnostics_rows += len(rows)
                born = True
            except Exception as e:  # noqa: BLE001 - the next turn tries again
                logger.warning(f"Diagnostics: {'refresh' if born else 'the births'} failed: {type(e).__name__}: {e}")
            await asyncio.sleep(REFRESH_S)

    def _on_raw_row(self, row: "raw_row.RawRow", queue: BackpressureManager) -> None:
        self.metrics['records_received'] += 1
        self._enqueue_fast(row.to_zerobus_record(), queue)

    def _enqueue_fast(self, record_dict: Dict[str, Any], queue: BackpressureManager) -> None:
        """Enqueue without a task per record (one per record can starve the event loop)."""
        wal = self._queue_wals.get(queue)
        if wal is not None:
            seq = wal.append(record_dict)   # on disk before it is queued
            if seq is not None:
                record_dict[SEQ_FIELD] = seq
        try:
            queue.memory_queue.put_nowait(record_dict)
            queue.metrics['records_enqueued'] += 1
            queue.metrics['current_queue_depth'] = queue.memory_queue.qsize()
            self.metrics['records_enqueued'] += 1
        except asyncio.QueueFull:
            # Slow-path: the drop policy
            asyncio.create_task(self._enqueue_record(record_dict, queue))

    async def _enqueue_record(self, record: Dict[str, Any], queue: BackpressureManager):
        """Enqueue a record into a full queue (the drop policy decides).

        Args:
            record: Record dict
            queue: the target's queue
        """
        try:
            success = await queue.enqueue(record)
        except ValueError:          # drop_policy reject: queue full
            queue.dropped(record)
            success = False
        except Exception as e:  # noqa: BLE001 - dropped and done in the log, not lost in the task
            logger.error(f"Record dropped: the queue couldn't take it: {type(e).__name__}: {e}")
            queue.dropped(record)
            success = False
        if success:
            self.metrics['records_enqueued'] += 1
        else:
            self.metrics['records_dropped'] += 1

    def _on_stats(self, stats: Dict[str, Any]):
        """Callback for source client statistics."""
        logger.debug(f"Source stats: {stats}")

    def _to_zerobus_message(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """A queued row as its Zerobus record: the raw_messages columns."""
        return {column: record[column] for column in raw_row.COLUMNS}

    async def stop(self):
        """Stop all components gracefully."""
        logger.info("Stopping the bridge...")
        self._shutdown = True
        self._starting = False

        if self.diagnostics_task is not None:
            self.diagnostics_task.cancel()
            await asyncio.gather(self.diagnostics_task, return_exceptions=True)
            self.diagnostics_task = None

        # Sparkplug primary hosts: no supervisor may go ONLINE again while stopping
        for task in self.host_tasks.values():
            task.cancel()
        if self.host_tasks:
            await asyncio.gather(*self.host_tasks.values(), return_exceptions=True)

        # Stop all source clients (a primary host publishes OFFLINE first, so the edges start buffering); copies: a
        # source's stop or edit in progress changes them meanwhile
        for source_name, client in list(self.clients.items()):
            try:
                logger.info(f"Stopping source: {source_name}")
                await client.stop()
                await client.disconnect()
            except Exception as e:
                logger.error(f"Error stopping {source_name}: {e}")

        # Wait for client tasks (one waiting to go ONLINE is cancelled)
        if self.client_tasks:
            _, pending = await asyncio.wait(list(self.client_tasks.values()), timeout=5.0)
            for task in pending:
                task.cancel()
            await asyncio.gather(*self.client_tasks.values(), return_exceptions=True)
        for source_name, client in list(self.clients.items()):
            try:
                await client.close()
            except Exception as e:
                logger.error(f"Error closing {source_name}: {e}")

        # Writers: write what is queued (up to drain_timeout_seconds), then stop
        for writer in self.writers.values():
            writer.finish(self.drain_timeout_s)
        if self.writer_tasks:
            await asyncio.wait(list(self.writer_tasks.values()), timeout=self.drain_timeout_s + 5.0)
            for task in self.writer_tasks.values():
                task.cancel()
            await asyncio.gather(*self.writer_tasks.values(), return_exceptions=True)

        # Write-ahead logs: fsync and close (rows not acknowledged stay in them for the next start)
        for task in self.wal_tasks.values():
            task.cancel()
        await asyncio.gather(*self.wal_tasks.values(), return_exceptions=True)
        for wal in self.wals.values():
            wal.close()

        for notifier in list(self.birth_notifiers.values()):
            await notifier.stop()

        # for good: a start after a stop is a new Bridge (core/lifecycle.py); no source is shown running, nor Zerobus
        # (the writers stay, for their final counts)
        self.clients.clear()
        self.client_tasks.clear()
        self.host_tasks.clear()
        self.mqtt_errors.clear()
        self.tables.clear()
        self.zerobus_enabled = False
        self.running = False
        logger.info("✓ Bridge stopped")

    def sparkplug_health(self) -> Dict[str, Dict[str, Any]]:
        """Per Sparkplug source: host state, MQTT, rows, Zerobus writer, NBIRTH calls, write-ahead log."""
        def iso(t):
            return None if t is None else _iso_ms(t * 1000)
        out: Dict[str, Dict[str, Any]] = {}
        for name, client in self.clients.items():
            s = client.stats()
            supervisor = self.host_supervisors.get(name)
            h: Dict[str, Any] = {
                'state': supervisor.state.mode if supervisor else ('ONLINE' if client.is_connected else 'OFFLINE'),
                'primary_host_id': client.primary_host_id, 'mqtt_connected': client.is_connected,
                'mqtt_online': client.is_online, 'state_topic': client.state_topic,
                'messages_received': s['messages_received'], 'rows_queued': s['rows'],
                'messages_skipped': sum(v for k, v in s.items() if k.startswith('skipped_')),
                'topics_malformed': s['skipped_malformed_topic'], 'payloads_undecodable': s['skipped_undecodable_payload'],
                'state_republished': s['state_republished'], 'rows_failed': s['rows_failed'],
                # rows and rebirth requests (sparkplug/sequencer.py); keep_all: the drops are only counted
                'sequence_handling': s['sequence_handling'], 'messages_held': s['messages_held'],
                'messages_reordered': s['messages_reordered'], 'messages_dropped': s['messages_dropped'],
                'dropped': {k.removeprefix('dropped_'): v for k, v in s.items() if k.startswith('dropped_') and v},
                'rebirth_requests': s['rebirth_requests'], 'rebirth_requests_sent': s['rebirth_requests_sent'],
                'rebirth_requests_on_request': s['rebirth_requests_on_request'],
                'rebirth_requests_rate_limited': s['rebirth_requests_rate_limited'],
                'rebirth_requests_blocked': s['rebirth_requests_blocked'],
                'rebirth_requests_failed': s['rebirth_requests_failed'],
                'rebirth_requests_not_sent_refused': s['rebirth_requests_not_sent_refused'],
                'rebirth_refused': s['rebirth_refused'], 'block_auto_rebirth': s['block_auto_rebirth'],
                'duplicate_aliases': s['duplicate_aliases'], 'sequence_errors': s['sequence_errors'],
            }
            mqtt_error = self.mqtt_errors.get(name) or {}
            h.update({'mqtt_last_error': mqtt_error.get('message'), 'mqtt_failing_since': iso(mqtt_error.get('since')),
                      'mqtt_last_error_at': iso(mqtt_error.get('at'))})
            writer = self.writers.get(self._get_target_for_source(name))
            if writer is not None:
                w = writer.status()
                h.update({'queue_depth': w.queue_depth, 'queue_high_water': w.queue_high_water,
                          'queue_dropped': self._queue_for_source(name).metrics.get('records_dropped', 0),
                          'zerobus_stream_open': w.stream_open, 'rows_written': w.written, 'rows_too_large': w.rows_too_large,
                          'rows_unconvertible': w.rows_unconvertible,
                          'zerobus_failures': w.failures, 'batch_in_flight': w.batch_in_flight,
                          'zerobus_failing_since': iso(w.failing_since), 'batch_pending_since': iso(w.pending_since),
                          'zerobus_last_success': iso(w.last_success), 'last_error': w.last_error,
                          'zerobus_last_error_at': iso(w.last_error_at)})
            wal = self.wals.get(self._get_target_for_source(name))
            if wal is not None:
                st = wal.status()
                h.update({'wal_pending': st.pending, 'wal_rows_appended': st.rows_appended,
                          'wal_rows_replayed': st.rows_replayed, 'wal_rows_not_logged': st.rows_not_logged,
                          'wal_bytes': st.bytes, 'wal_failures': st.failures, 'wal_last_error': st.last_error})
            notifier = self.birth_notifiers.get(name)
            if notifier is not None:
                n = notifier.status()
                h.update({'nbirths_written': n.births, 'edge_node_schema_calls': n.calls,
                          'edge_node_schema_failures': n.failures, 'edge_node_schema_gave_up': n.gave_up,
                          'edge_node_schema_pending': n.pending, 'edge_node_schema_last_error': n.last_error,
                          'edge_node_schema_checked': n.checked})
            out[name] = h
        return out

    def sparkplug_edge_nodes(self) -> Dict[str, List[Dict[str, Any]]]:
        """Per Sparkplug source: its edge nodes and their devices, online or offline, since when (epoch ms and ISO)."""
        out: Dict[str, List[Dict[str, Any]]] = {}
        for name, client in self.clients.items():
            nodes = client.edge_nodes()
            for n in nodes:
                n['since_iso'] = _iso_ms(n['since'])
                for d in n['devices']:
                    d['since_iso'] = _iso_ms(d['since'])
            out[name] = nodes
        return out

    async def request_rebirth(self, source_name: str, group_id: Any, edge_node_id: Any) -> Dict[str, Any]:
        """An operator's rebirth request: the source sends an NCMD to the edge node now."""
        client = self.clients.get(source_name)
        if client is None:
            return {'status': 'error', 'code': 'not_found', 'message': f"Source not running: {source_name}"}
        try:
            sent, message = await client.request_rebirth(group_id, edge_node_id)
        except ValueError as e:
            return {'status': 'error', 'code': 'invalid', 'message': str(e)}
        if sent:
            logger.info(f"[{source_name}] rebirth requested by an operator: {group_id}/{edge_node_id}")
        return {'status': 'ok' if sent else 'error', 'message': message}

    def get_metrics(self) -> Dict[str, Any]:
        """Metrics of the bridge, its queues, write-ahead logs, writers and sources."""
        return {
            'bridge': self.metrics,
            'backpressure': self._backpressure_metrics(),
            'wal': {target_id: dataclasses.asdict(wal.status()) for target_id, wal in self.wals.items()},
            'zerobus_targets': {target_id: dataclasses.asdict(writer.status()) for target_id, writer in self.writers.items()},
            'clients': {
                name: {
                    'connected': client.is_connected,
                    'protocol': client.protocol_type.value,
                    'endpoint': client.endpoint,
                    'sparkplug': client.stats(),
                }
                for name, client in self.clients.items()
            }
        }

    def _backpressure_metrics(self) -> Dict[str, Any]:
        """The queues' metrics summed (the keys of one queue), and per target under ``targets``."""
        per_target = {target_id: queue.get_metrics() for target_id, queue in self.queues.items()}
        keys = next(iter(per_target.values()), {}).keys()
        return {**{k: sum(m.get(k, 0) for m in per_target.values()) for k in keys}, 'targets': per_target}

    async def add_source(self, source_name: str, source_type: str, source_config: Dict[str, Any]):
        """Add a new data source dynamically.

        Args:
            source_name: Unique name for the source
            source_type: Protocol type ('sparkplug')
            source_config: Source configuration dict

        Raises:
            ValueError: If source already exists or config is invalid
        """
        if source_name in self.clients:
            raise ValueError(f"Source '{source_name}' already exists")

        logger.info(f"Adding new source: {source_name} (type: {source_type})")

        full_config = {
            'name': source_name,
            'protocol': source_type,
            'enabled': True,
            **source_config
        }

        self.sources.append(full_config)

        # Start the client if bridge is running
        if self.running and not self._shutdown:
            try:
                await self._start_client(full_config)
                logger.info(f"✓ Source '{source_name}' added and started successfully")
            except Exception as e:
                # Remove from sources list if start failed
                self.sources.remove(full_config)
                logger.error(f"Failed to start source '{source_name}': {e}")
                raise ValueError(f"Failed to start source: {e}")
        else:
            logger.info(f"✓ Source '{source_name}' added (will start when bridge starts)")

    async def remove_source(self, source_name: str):
        """Remove an existing data source dynamically.

        Args:
            source_name: Name of the source to remove

        Raises:
            ValueError: If source doesn't exist
        """
        if source_name not in self.clients and not any(s.get('name') == source_name for s in self.sources):
            raise ValueError(f"Source '{source_name}' not found")

        logger.info(f"Removing source: {source_name}")

        # Stop it if it runs (a stopped source is removed from the configuration only)
        if source_name in self.clients:
            await self.stop_source(source_name)
        self._source_queues.pop(source_name, None)

        # Remove from sources list (in place: the web server lists the same list)
        self.sources[:] = [s for s in self.sources if s.get('name') != source_name]

        logger.info(f"✓ Source '{source_name}' removed successfully")

    async def update_source(self, source_name: str, source_config: Dict[str, Any]) -> None:
        """Replace a source's configuration; a running source is restarted with it."""
        index = next((i for i, s in enumerate(self.sources) if s.get('name') == source_name), None)
        if index is None:
            raise ValueError(f"Source '{source_name}' not found")
        was_active = source_name in self.clients
        if was_active:
            await self.stop_source(source_name)
        self._source_queues.pop(source_name, None)
        old = self.sources[index]
        self.sources[index] = source_config
        if was_active and self._shutdown:
            logger.info(f"✓ Source '{source_name}' updated; the bridge is stopping: it applies at the next start")
            return
        if was_active and source_config.get('enabled', True):
            try:
                await self._start_client(source_config)
            except Exception:
                # runs on as before (the page shows the error, nothing is saved)
                self.sources[index] = old
                self._source_queues.pop(source_name, None)
                if old.get('enabled', True):
                    await self._start_client(old)
                raise
        logger.info(f"✓ Source '{source_name}' updated" + (" and restarted" if was_active else ""))

    async def start_zerobus(self) -> Dict[str, Any]:
        """Start ZeroBus streaming dynamically.

        Returns:
            dict: Status message with success/error
        """
        if self._shutdown:                      # stopped for good: its writers would never be stopped again
            return {'status': 'error', 'message': 'The bridge is stopped: start it first'}
        try:
            if self.zerobus_enabled and self.tables:
                return {'status': 'ok', 'message': 'ZeroBus already enabled'}

            logger.info("Starting ZeroBus streaming...")
            await self._open_targets()
            self.zerobus_enabled = True

            logger.info("✓ ZeroBus streaming started successfully")
            return {'status': 'ok', 'message': 'ZeroBus streaming started successfully'}

        except Exception as e:
            logger.error(f"Failed to start ZeroBus: {e}", exc_info=True)
            return {'status': 'error', 'message': f'Failed to start ZeroBus: {str(e)}'}

    async def stop_zerobus(self) -> Dict[str, Any]:
        """Stop ZeroBus streaming dynamically.

        Returns:
            dict: Status message with success/error
        """
        if self._shutdown:
            return {'status': 'error', 'message': 'The bridge is stopped: start it first'}
        try:
            if not self.zerobus_enabled:
                return {'status': 'ok', 'message': 'ZeroBus already disabled'}

            logger.info("Stopping ZeroBus streaming...")

            # Writers end after their current step (a take in progress completes; a batch not acknowledged is
            # kept for the next writer), then their tasks are cancelled
            for writer in self.writers.values():
                writer.finish(0)
            if self.writer_tasks:
                await asyncio.wait(list(self.writer_tasks.values()), timeout=5.0)
                for task in self.writer_tasks.values():
                    task.cancel()
                await asyncio.gather(*self.writer_tasks.values(), return_exceptions=True)
                self.writer_tasks.clear()
                logger.info("Writers stopped")

            self.tables.clear()
            for target_id, writer in self.writers.items():
                self._unacked[target_id] = writer.unacknowledged()   # sent first by the next writer
            for name, writer in list(self._notifier_writers.items()):
                writer.remove_listener(self.birth_notifiers[name].accept)
                del self._notifier_writers[name]
            self.writers.clear()   # the write-ahead logs stay open: rows are still queued and logged
            self.zerobus_enabled = False

            logger.info("✓ ZeroBus streaming stopped successfully")
            return {'status': 'ok', 'message': 'ZeroBus streaming stopped successfully'}

        except Exception as e:
            logger.error(f"Failed to stop ZeroBus: {e}", exc_info=True)
            return {'status': 'error', 'message': f'Failed to stop ZeroBus: {str(e)}'}

    def get_status(self) -> Dict[str, Any]:
        """Get current status of all components.

        Returns:
            Status dict
        """
        return {
            'configured_sources': len(self.sources),
            'active_sources': len(self.clients),
            'bridge_running': bool(getattr(self, 'running', False)),
            'bridge_starting': bool(getattr(self, '_starting', False)) and not bool(getattr(self, 'running', False)),
            'bridge_started_at': None if self.started_at is None else int(self.started_at * 1000),   # epoch ms
            'zerobus_config_enabled': self.zerobus_config_enabled,
            'zerobus_connected': bool(self.tables),
            'zerobus_enabled': self.zerobus_enabled,
            'zerobus_targets': list(self.tables.keys()),
            'backpressure_queue_size': self._backpressure_metrics().get('current_queue_depth', 0),
            'diagnostics': None if self.diagnostics is None else {'group_id': self.diagnostics.group_id,
                                                                  'rows_queued': self.diagnostics_rows},
            'metrics': self.get_metrics(),
        }
