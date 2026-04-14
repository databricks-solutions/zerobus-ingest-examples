"""Unified Bridge - Enterprise OT/IoT Connector Core.

Orchestrates data flow from protocol sources to Databricks:
    Protocol Clients → Tag Normalization → Backpressure → ZeroBus → Delta Tables

Features:
- Multi-protocol support (OPC-UA, MQTT, Modbus)
- Tag normalization (ISA-95 hierarchies)
- Per-source ZeroBus targets (different tables per source)
- Credential encryption
- Auto-reconnection with exponential backoff
- Backpressure handling with disk spooling
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from unified_connector.protocols.base import ProtocolClient, ProtocolRecord, ProtocolType
from unified_connector.protocols.factory import create_protocol_client
from unified_connector.normalizer import get_normalization_manager
from unified_connector.core.backpressure import BackpressureManager
from unified_connector.core.zerobus_client import ZeroBusClient
from unified_connector.core.numeric_compression import (
    CompressionPolicy,
    CompressionRule,
    NumericCompressionMode,
    SdtState,
    changed,
    is_numeric,
    meaningful_numeric_change,
    parse_policy_dict,
    parse_rules,
    resolve_effective_policy,
)

logger = logging.getLogger(__name__)


class UnifiedBridge:
    """
    Unified bridge orchestrating OT/IoT data ingestion to Databricks.

    Architecture:
        Protocol Sources → Tag Normalization → Backpressure Queue → ZeroBus Batching → Delta Tables

    Key Features:
        - Per-source ZeroBus targets (route different sources to different tables)
        - Tag normalization with ISA-95/Purdue Model hierarchies
        - Credential encryption at rest
        - Automatic reconnection with exponential backoff
        - Backpressure with disk spooling for resilience
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize unified bridge.

        Args:
            config: Configuration dictionary from config.yaml
        """
        self.config = config
        self.sources = config.get('sources', [])
        self._source_cfg_by_name: Dict[str, Dict[str, Any]] = {}
        self._refresh_source_lookup()

        # ZeroBus configuration
        self.zerobus_config = config.get('zerobus', {})
        # Config flag (what user configured)
        self.zerobus_config_enabled = self.zerobus_config.get('enabled', False)
        # Runtime flag (whether client(s)/batchers are running)
        self.zerobus_enabled = False

        # Tag normalization
        self.normalization_config = config.get('normalization', {})
        self.normalization_enabled = self.normalization_config.get('enabled', True)

        # Initialize normalizer if enabled
        if self.normalization_enabled:
            self.norm_manager = get_normalization_manager()
            self.norm_manager.set_enabled(True)
            logger.info("Tag normalization enabled")
        else:
            self.norm_manager = None
            logger.info("Tag normalization disabled")

        # Initialize backpressure manager
        self.backpressure = BackpressureManager(config.get('backpressure', {}))

        # ZeroBus clients (one per unique target)
        self.zerobus_clients: Dict[str, ZeroBusClient] = {}

        # Protocol clients
        self.clients: Dict[str, ProtocolClient] = {}
        self.client_tasks: Dict[str, asyncio.Task] = {}

        # Batch processing tasks (one per ZeroBus target)
        self.batch_tasks: Dict[str, asyncio.Task] = {}

        # Batch settings
        self.batch_size = self.zerobus_config.get('batch', {}).get('max_records', 1000)
        self.batch_timeout_sec = self.zerobus_config.get('batch', {}).get('timeout_seconds', 5.0)

        # Numeric compression settings (global + per-source override support).
        self.numeric_compression_cfg = config.get("numeric_compression", {}) or {}
        self.global_compression_policy: CompressionPolicy = parse_policy_dict(self.numeric_compression_cfg)
        self.global_compression_rules: List[CompressionRule] = parse_rules(self.numeric_compression_cfg)
        self.last_emitted_by_key: Dict[str, Any] = {}
        self.sdt_state_by_key: Dict[str, SdtState] = {}

        # Reconnection settings
        self.reconnect_enabled = True
        self.reconnect_initial_delay_sec = 1.0
        self.reconnect_max_delay_sec = 300.0
        self.reconnect_multiplier = 2.0

        # Per-protocol-vendor pipeline stages - separate tracking for each protocol+vendor combination
        # This ensures OPC UA and MQTT samples don't compete for the same buffer slots
        from collections import deque, defaultdict
        self.vendor_pipelines = defaultdict(lambda: {
            'raw_protocol': deque(maxlen=3),
            'after_vendor_detection': deque(maxlen=3),
            'after_normalization': deque(maxlen=3),
            'zerobus_batch': deque(maxlen=3),
            'record_count': 0
        })

        # Metrics
        self.metrics = {
            'records_received': 0,
            'records_normalized': 0,
            'vendor_formats': {
                'kepware': 0,
                'sparkplug_b': 0,
                'honeywell': 0,
                'opcua': 0,
                'modbus': 0,
                'generic': 0,
                'unknown': 0
            },
            # Protocol-vendor breakdown: tracks vendor formats per source protocol
            'protocol_vendor_breakdown': {},
            'records_enqueued': 0,
            'records_dropped': 0,
            'batches_sent': 0,
            'reconnections': 0,
            'compression': {
                'mode_default': self.global_compression_policy.mode.value,
                'incoming': 0,
                'emitted': 0,
                'filtered_change_only': 0,
                'filtered_deadband': 0,
                'filtered_sdt': 0,
                'forced_sdt_max_interval': 0,
                'sdt_out_of_order_resets': 0,
            },
        }

        self._shutdown = False
        self.running = False
        self._starting = False
        self._start_lock = asyncio.Lock()

        logger.info(f"UnifiedBridge initialized: {len(self.sources)} sources configured")

    def _refresh_source_lookup(self) -> None:
        self._source_cfg_by_name = {
            str(s.get("name", "")): s
            for s in self.sources
            if isinstance(s, dict) and s.get("name")
        }



    async def start_infra(self) -> None:
        """Start bridge infrastructure (ZeroBus + batch processors) only.

        Does NOT automatically start all configured sources.
        Use start_source()/stop_source() to control sources individually.
        """
        async with self._start_lock:
            if self.running or self._starting:
                logger.info("UnifiedBridge already running/starting")
                return
            self._starting = True
            self._shutdown = False

        logger.info("Starting UnifiedBridge infrastructure...")

        # Initialize ZeroBus clients if enabled
        if self.zerobus_config_enabled:
            logger.info("ZeroBus enabled - initializing connections...")
            await self._initialize_zerobus_clients()
            self.zerobus_enabled = True
        else:
            logger.info("ZeroBus disabled - data collection only mode")

        # Start batch processors if ZeroBus enabled
        if self.zerobus_enabled:
            for target_id in self.zerobus_clients.keys():
                if target_id in self.batch_tasks and not self.batch_tasks[target_id].done():
                    continue
                task = asyncio.create_task(self._batch_processor(target_id))
                self.batch_tasks[target_id] = task
                logger.info(f"Batch processor started for target: {target_id}")

        self.running = True
        self._starting = False
        logger.info("✓ UnifiedBridge infrastructure started")

    async def start(self):
        """Start all components (infra + all enabled sources)."""
        await self.start_infra()

        # Start protocol clients (enabled sources)
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

        logger.info(f"✓ UnifiedBridge started with {len(self.clients)} active sources")

    async def _initialize_zerobus_clients(self):
        """Initialize ZeroBus clients for each unique target."""
        # Get default target
        default_target = self.zerobus_config.get('default_target', {})

        # Get per-source targets
        source_targets = self.zerobus_config.get('source_targets', {})

        # Build unique targets
        targets = {}

        # Add default target
        if default_target.get('workspace_host'):
            target_id = self._get_target_id(default_target)
            targets[target_id] = default_target

        # Add per-source targets
        for source_name, target_config in source_targets.items():
            # Merge with default config
            merged_target = {**default_target, **target_config}
            target_id = self._get_target_id(merged_target)
            targets[target_id] = merged_target

        # Create ZeroBus clients for each target
        for target_id, target_config in targets.items():
            try:
                # Build ZeroBus config for this target
                zb_config = {
                    'zerobus': {
                        'enabled': True,
                        'workspace_host': target_config['workspace_host'],
                        'zerobus_endpoint': self.zerobus_config.get('zerobus_endpoint'),
                        'target': {
                            'catalog': target_config.get('catalog', 'main'),
                            'schema': target_config.get('schema', 'iot_data'),
                            'table': target_config.get('table', 'sensor_readings'),
                        },
                        'auth': self.zerobus_config.get('auth', {}),
                        'batch': self.zerobus_config.get('batch', {}),
                    }
                }

                client = ZeroBusClient(zb_config)
                await client.connect()

                self.zerobus_clients[target_id] = client
                logger.info(f"✓ ZeroBus client connected: {target_id}")

            except Exception as e:
                logger.error(f"Failed to initialize ZeroBus client for {target_id}: {e}")

    def _get_target_id(self, target_config: Dict[str, Any]) -> str:
        """Generate unique ID for ZeroBus target.

        Args:
            target_config: Target configuration dict

        Returns:
            Unique target ID string
        """
        workspace = target_config.get('workspace_host', '')
        catalog = target_config.get('catalog', 'main')
        schema = target_config.get('schema', 'iot_data')
        table = target_config.get('table', 'sensor_readings')

        # Extract workspace ID from URL
        workspace_id = workspace.split('//')[1].split('.')[0] if '//' in workspace else workspace

        return f"{workspace_id}.{catalog}.{schema}.{table}"

    def _get_target_for_source(self, source_name: str) -> str:
        """Get ZeroBus target ID for a source.

        Args:
            source_name: Name of the source

        Returns:
            Target ID
        """
        # Check if source has specific target
        source_targets = self.zerobus_config.get('source_targets', {})
        if source_name in source_targets:
            target_config = {
                **self.zerobus_config.get('default_target', {}),
                **source_targets[source_name]
            }
            return self._get_target_id(target_config)

        # Use default target
        default_target = self.zerobus_config.get('default_target', {})
        return self._get_target_id(default_target)

    async def _start_client(self, source_config: Dict[str, Any]):
        """Start a single protocol client with reconnection loop.

        Args:
            source_config: Source configuration dict
        """
        source_name = source_config.get('name', 'unknown')
        protocol_type = source_config.get('protocol', 'opcua')

        # Add normalization flag to source config
        source_config['normalization_enabled'] = self.normalization_enabled

        # Create protocol client
        client = create_protocol_client(
            protocol_type=protocol_type,
            source_name=source_name,
            endpoint=source_config['endpoint'],
            config=source_config,
            on_record=self._on_record,
            on_stats=self._on_stats
        )

        self.clients[source_name] = client

        # Start client with reconnection loop
        task = asyncio.create_task(self._client_lifecycle(source_name, client))
        self.client_tasks[source_name] = task

    async def _client_lifecycle(self, source_name: str, client: ProtocolClient):
        """Manage protocol client lifecycle with automatic reconnection.

        Args:
            source_name: Name of the source
            client: Protocol client instance
        """
        reconnect_delay = self.reconnect_initial_delay_sec

        while not self._shutdown:
            try:
                logger.info(f"[{source_name}] Connecting to {client.endpoint}...")
                await client.connect()
                logger.info(f"[{source_name}] ✓ Connected")

                # Start subscription/polling
                logger.info(f"[{source_name}] Starting data collection...")
                await client.subscribe()

                # Keep running until shutdown
                while not self._shutdown:
                    await asyncio.sleep(1)

                # Cleanup on shutdown
                await client.disconnect()
                reconnect_delay = self.reconnect_initial_delay_sec

                if self._shutdown:
                    break

            except Exception as e:
                logger.error(f"[{source_name}] Error: {e}")

                # Cleanup
                try:
                    await client.stop()
                    await client.disconnect()
                except Exception:
                    pass

                if not self.reconnect_enabled or self._shutdown:
                    break

                # Exponential backoff with jitter
                jitter = reconnect_delay * 0.1 * (2 * (time.time() % 1) - 0.5)
                sleep_time = min(reconnect_delay + jitter, self.reconnect_max_delay_sec)

                logger.warning(f"[{source_name}] Reconnecting in {sleep_time:.1f}s...")
                self.metrics['reconnections'] += 1

                await asyncio.sleep(sleep_time)

                # Increase backoff
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

        client = self.clients.get(source_name)
        try:
            if client is not None:
                await client.stop()
                await client.disconnect()
        except Exception as e:
            logger.warning(f"Error stopping source {source_name}: {e}")

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

        self.clients.pop(source_name, None)
        return {'status': 'ok', 'message': f"Source stopped: {source_name}"}

    def _detect_vendor_format(self, record) -> str:
        """Detect vendor format from record characteristics.

        Args:
            record: Protocol record (ProtocolRecord object or dict)

        Returns:
            Detected format: 'kepware', 'sparkplug_b', 'honeywell', 'opcua', 'modbus', 'generic', or 'unknown'
        """
        # Handle both dict and ProtocolRecord
        if isinstance(record, dict):
            topic = record.get('topic_or_path', '') or ""
            metadata = record.get('metadata', {}) or {}
            protocol = record.get('protocol', '') or record.get('protocol_type', '')
        else:
            topic = getattr(record, 'topic_or_path', '') or ""
            metadata = getattr(record, 'metadata', {}) or {}
            protocol_type = getattr(record, 'protocol_type', None)
            protocol = protocol_type.value if hasattr(protocol_type, 'value') else str(protocol_type) if protocol_type else ''

        # Sparkplug B detection: SparkplugB namespace or bdSeq/seq metadata
        if "SparkplugB" in topic or "sparkplug" in topic.lower():
            return "sparkplug_b"
        if "bdSeq" in metadata or "seq" in metadata:
            return "sparkplug_b"
        if any(msgtype in topic for msgtype in ["NBIRTH", "DBIRTH", "NDATA", "DDATA", "NDEATH", "DDEATH"]):
            return "sparkplug_b"

        # Kepware detection: Check for kepware in path (supports both MQTT and OPC UA)
        if "kepware" in topic.lower():
            return "kepware"
        # Kepware detection: Channel.Device.Tag pattern (MQTT-based)
        if "Siemens_S7" in topic or "Allen_Bradley" in topic or "Modicon" in topic:
            return "kepware"
        # Kepware IoT Gateway format check
        if "." in topic and topic.count(".") >= 2:  # Channel.Device.Tag has at least 2 dots
            parts = topic.split(".")
            if len(parts) >= 3 and all(p.replace("_", "").replace("-", "").isalnum() for p in parts[:3]):
                return "kepware"

        # Honeywell detection: Check for honeywell in path (supports both MQTT and OPC UA)
        if "honeywell" in topic.lower():
            return "honeywell"
        if any(suffix in topic for suffix in [".PV", ".SP", ".OP", ".MODE", "/PV", "/SP", "/OP", "/MODE"]):
            return "honeywell"
        if "FIM" in topic or "AIM" in topic or "PID" in topic or "EXPERION" in topic:
            return "honeywell"

        # Protocol-based detection for native protocols (OPC UA, Modbus)
        if protocol and 'opcua' in protocol.lower():
            return "opcua"
        if protocol and 'modbus' in protocol.lower():
            return "modbus"

        # Generic format
        if "/" in topic:  # Simple path-based format
            return "generic"

        return "unknown"

    def _on_record(self, record: ProtocolRecord):
        """Callback when protocol client receives a record.

        Args:
            record: Protocol record
        """
        self.metrics['records_received'] += 1

        # Detect vendor format first
        vendor_format = self._detect_vendor_format(record)
        self.metrics['vendor_formats'][vendor_format] += 1

        # Track protocol-vendor breakdown
        # Extract protocol from record
        if isinstance(record, dict):
            protocol = record.get('protocol_type', record.get('protocol', 'unknown'))
        else:
            protocol_type = getattr(record, 'protocol_type', None)
            protocol = protocol_type.value if hasattr(protocol_type, 'value') else str(protocol_type) if protocol_type else 'unknown'

        # Initialize protocol entry if needed
        if protocol not in self.metrics['protocol_vendor_breakdown']:
            self.metrics['protocol_vendor_breakdown'][protocol] = {}

        # Initialize vendor entry for this protocol if needed
        if vendor_format not in self.metrics['protocol_vendor_breakdown'][protocol]:
            self.metrics['protocol_vendor_breakdown'][protocol][vendor_format] = 0

        # Increment protocol-vendor counter
        self.metrics['protocol_vendor_breakdown'][protocol][vendor_format] += 1

        # Create protocol-vendor key for separate buffering
        protocol_vendor_key = f"{protocol}_{vendor_format}"

        # Increment per-protocol-vendor record count
        self.vendor_pipelines[protocol_vendor_key]['record_count'] += 1

        # Stage 1: Capture raw protocol message (per-protocol-vendor)
        try:
            # Handle both ProtocolRecord objects and dict objects
            if isinstance(record, dict):
                raw_message = {
                    'timestamp': time.time(),
                    'source_name': record.get('source_name', 'unknown'),
                    'protocol': record.get('protocol', 'unknown'),
                    'topic_or_path': record.get('topic_or_path', ''),
                    'value': record.get('value'),
                    'quality': record.get('quality', 'UNKNOWN'),
                    'metadata': record.get('metadata', {}),
                    'vendor_format': vendor_format
                }
            else:
                raw_message = {
                    'timestamp': time.time(),
                    'source_name': getattr(record, 'source_name', 'unknown'),
                    'protocol': record.protocol_type.value if hasattr(record, 'protocol_type') else 'unknown',
                    'topic_or_path': getattr(record, 'topic_or_path', ''),
                    'value': getattr(record, 'value', None),
                    'quality': getattr(record, 'quality', 'UNKNOWN'),
                    'metadata': getattr(record, 'metadata', {}) or {},
                    'vendor_format': vendor_format
                }
            self.vendor_pipelines[protocol_vendor_key]['raw_protocol'].append(raw_message)
        except Exception as e:
            logger.error(f"[DEBUG] EXCEPTION in sample capture: {e}", exc_info=True)

        # Convert to dict using the record's to_dict method
        record_dict = record.to_dict()

        # Add vendor format and protocol to record for downstream processing
        record_dict['vendor_format'] = vendor_format
        record_dict['protocol'] = protocol  # Store protocol for zerobus_batch stage

        # Stage 2: Capture after vendor detection (per-protocol-vendor)
        after_vendor = record_dict.copy()
        after_vendor['timestamp'] = time.time()
        after_vendor['stage'] = 'after_vendor_detection'
        self.vendor_pipelines[protocol_vendor_key]['after_vendor_detection'].append(after_vendor)

        # Stage 3: Apply normalization if enabled
        after_norm = record_dict.copy()
        after_norm['timestamp'] = time.time()
        after_norm['stage'] = 'after_normalization'

        if self.normalization_enabled and self.norm_manager:
            try:
                normalizer = self.norm_manager.get_normalizer(record.protocol_type.value)
                if normalizer:
                    # Create raw data format expected by normalizer
                    raw_data = self._protocol_record_to_raw_data(record)
                    normalized_tag = normalizer.normalize(raw_data)

                    # Update record with normalized values
                    record_dict['tag_path'] = normalized_tag.tag_path
                    record_dict['tag_id'] = normalized_tag.tag_id
                    record_dict['data_type'] = normalized_tag.data_type.value
                    record_dict['quality'] = normalized_tag.quality.value

                    # Update after_norm sample with normalized values
                    after_norm.update({
                        'tag_path': normalized_tag.tag_path,
                        'tag_id': normalized_tag.tag_id,
                        'data_type': normalized_tag.data_type.value,
                        'quality': normalized_tag.quality.value,
                        'normalized': True
                    })

                    self.metrics['records_normalized'] += 1
                else:
                    after_norm['normalized'] = False
                    after_norm['normalized_reason'] = 'No normalizer for protocol'
            except Exception as e:
                logger.warning(f"Normalization failed: {e}")
                after_norm['normalized'] = False
                after_norm['normalized_error'] = str(e)
        else:
            after_norm['normalized'] = False
            after_norm['normalized_reason'] = 'Normalization disabled'

        # Stage 3: Capture after normalization (per-protocol-vendor)
        self.vendor_pipelines[protocol_vendor_key]['after_normalization'].append(after_norm)

        # Stage 3.5: Numeric compression/filtering (uses source event time; keyed by normalized path).
        compressed = self._apply_numeric_compression(record, record_dict)
        if compressed is None:
            return
        record_dict = compressed

        # Enqueue for ZeroBus
        # Fast-path: avoid creating a task per record (can starve the event loop).
        try:
            self.backpressure.memory_queue.put_nowait(record_dict)
            self.backpressure.metrics['records_enqueued'] += 1
            self.backpressure.metrics['current_queue_depth'] = self.backpressure.memory_queue.qsize()
            self.metrics['records_enqueued'] += 1
        except asyncio.QueueFull:
            # Slow-path: fall back to async enqueue (disk spooling / drop policy)
            asyncio.create_task(self._enqueue_record(record_dict))

    def _resolve_compression_policy_for_record(self, source_name: str, tag_path: str) -> CompressionPolicy:
        source_cfg = self._source_cfg_by_name.get(source_name) or {}
        src_comp_cfg = source_cfg.get("numeric_compression", {}) if isinstance(source_cfg, dict) else {}
        src_policy = parse_policy_dict(src_comp_cfg) if src_comp_cfg else None
        src_rules = parse_rules(src_comp_cfg) if src_comp_cfg else []
        return resolve_effective_policy(
            self.global_compression_policy,
            self.global_compression_rules,
            src_policy,
            src_rules,
            tag_path,
        )

    def _compression_key(self, source_name: str, tag_path: str) -> str:
        # Normalized key first; source-scoped to avoid accidental cross-source coupling.
        return f"{source_name}::{tag_path}"

    def _apply_numeric_compression(self, record: ProtocolRecord, record_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        source_name = str(record_dict.get("source_name") or record.source_name or "")
        tag_path = str(record_dict.get("tag_path") or record_dict.get("topic_or_path") or "")
        key = self._compression_key(source_name, tag_path)
        event_time_ms = int(record.event_time_ms)
        policy = self._resolve_compression_policy_for_record(source_name, tag_path)
        self.metrics['compression']['incoming'] += 1

        value = record_dict.get("value")
        last = self.last_emitted_by_key.get(key)

        # Non-numeric values always use change-only semantics.
        if not is_numeric(value):
            if not changed(last, value):
                self.metrics['compression']['filtered_change_only'] += 1
                return None
            self.last_emitted_by_key[key] = value
            self.metrics['compression']['emitted'] += 1
            return record_dict

        # Numeric modes.
        if policy.mode == NumericCompressionMode.NONE:
            if policy.change_only and not changed(last, value):
                self.metrics['compression']['filtered_change_only'] += 1
                return None
            self.last_emitted_by_key[key] = value
            self.metrics['compression']['emitted'] += 1
            return record_dict

        if policy.mode == NumericCompressionMode.CHANGE_ONLY:
            if not changed(last, value):
                self.metrics['compression']['filtered_change_only'] += 1
                return None
            self.last_emitted_by_key[key] = value
            self.metrics['compression']['emitted'] += 1
            return record_dict

        if policy.mode == NumericCompressionMode.DEADBAND:
            if not meaningful_numeric_change(last, value, policy.deadband):
                self.metrics['compression']['filtered_deadband'] += 1
                return None
            self.last_emitted_by_key[key] = value
            self.metrics['compression']['emitted'] += 1
            return record_dict

        # SDT mode.
        if policy.mode == NumericCompressionMode.SDT:
            # Defensive fallback for bad config values.
            if policy.sdt_deviation <= 0:
                if not changed(last, value):
                    self.metrics['compression']['filtered_change_only'] += 1
                    return None
                self.last_emitted_by_key[key] = value
                self.metrics['compression']['emitted'] += 1
                return record_dict

            state = self.sdt_state_by_key.setdefault(key, SdtState())
            out = state.offer(
                payload=record_dict,
                t_ms=event_time_ms,
                numeric_value=float(value),
                deviation=policy.sdt_deviation,
                max_interval_ms=max(0, int(policy.sdt_max_interval_ms)),
                min_interval_ms=max(0, int(policy.sdt_min_interval_ms)),
            )
            if out.filtered or out.emit_payload is None:
                self.metrics['compression']['filtered_sdt'] += 1
                return None

            if out.forced_by_max_interval:
                self.metrics['compression']['forced_sdt_max_interval'] += 1
            if out.reset_due_to_out_of_order:
                self.metrics['compression']['sdt_out_of_order_resets'] += 1

            emitted_payload = dict(out.emit_payload)
            emitted_value = emitted_payload.get("value")
            self.last_emitted_by_key[key] = emitted_value
            self.metrics['compression']['emitted'] += 1
            return emitted_payload

        # Unknown mode fallback: emit.
        self.last_emitted_by_key[key] = value
        self.metrics['compression']['emitted'] += 1
        return record_dict

    def _protocol_record_to_raw_data(self, record: ProtocolRecord) -> Dict[str, Any]:
        """Convert ProtocolRecord to raw data format for normalizers.

        Args:
            record: Protocol record

        Returns:
            Raw data dict
        """
        return {
            'value': record.value,
            'timestamp': record.event_time_ms / 1000.0,  # Convert ms to seconds
            'quality': record.status,  # ProtocolRecord has 'status', not 'quality'
            'tag_name': record.topic_or_path,  # Use topic_or_path as tag_name
            'tag_id': record.topic_or_path,  # Use topic_or_path as tag_id
            'source_name': record.source_name,
            'metadata': record.metadata or {},
        }

    async def _enqueue_record(self, record: Dict[str, Any]):
        """Enqueue record to backpressure queue.

        Args:
            record: Record dict
        """
        success = await self.backpressure.enqueue(record)
        if success:
            self.metrics['records_enqueued'] += 1
        else:
            self.metrics['records_dropped'] += 1

    def _on_stats(self, stats: Dict[str, Any]):
        """Callback for protocol client statistics.

        Args:
            stats: Statistics dict
        """
        logger.debug(f"Protocol stats: {stats}")

    async def _batch_processor(self, target_id: str):
        """Process records from backpressure queue in batches to ZeroBus.

        Args:
            target_id: ZeroBus target identifier
        """
        logger.info(f"Batch processor started for {target_id}")
        batch: List[Any] = []
        last_flush_time = time.time()

        zerobus_client = self.zerobus_clients.get(target_id)
        if not zerobus_client:
            logger.error(f"No ZeroBus client for target {target_id}")
            return

        while not self._shutdown:
            try:
                # Try to dequeue with timeout
                try:
                    record = await asyncio.wait_for(
                        self.backpressure.dequeue(),
                        timeout=1.0
                    )

                    if record is not None:
                        # Convert to ZeroBus message format
                        message = self._to_zerobus_message(record)
                        batch.append(message)

                        # Stage 4: Capture ZeroBus batch stage sample (per-protocol-vendor)
                        vendor_format = record.get('vendor_format', 'unknown')
                        protocol = record.get('protocol', 'unknown')
                        protocol_vendor_key = f"{protocol}_{vendor_format}"

                        zerobus_sample = message.copy()
                        zerobus_sample['timestamp'] = time.time()
                        zerobus_sample['stage'] = 'zerobus_batch'
                        self.vendor_pipelines[protocol_vendor_key]['zerobus_batch'].append(zerobus_sample)

                    else:
                        # Nothing to send; avoid a hot loop that starves the web UI
                        await asyncio.sleep(0.1)

                except asyncio.TimeoutError:
                    pass

                # Flush batch if size or timeout reached
                current_time = time.time()
                should_flush = (
                    len(batch) >= self.batch_size or
                    (len(batch) > 0 and (current_time - last_flush_time) >= self.batch_timeout_sec)
                )

                if should_flush:
                    logger.debug(f"Flushing batch of {len(batch)} records to {target_id}")
                    success = await zerobus_client.send_batch(batch)

                    if success:
                        self.metrics['batches_sent'] += 1
                        batch.clear()
                        last_flush_time = current_time
                    else:
                        logger.warning(f"ZeroBus send failed for {target_id}, will retry")
                        await asyncio.sleep(5.0)

            except Exception as e:
                logger.error(f"Batch processor error for {target_id}: {e}")
                await asyncio.sleep(1.0)

        # Flush remaining records on shutdown
        if batch:
            logger.info(f"Flushing final batch of {len(batch)} records to {target_id}")
            await zerobus_client.send_batch(batch)

        logger.info(f"Batch processor stopped for {target_id}")


    def _to_zerobus_message(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal record dict to the UC OPC-UA bronze schema."""
        import re

        metadata = record.get('metadata') or {}

        # event_time/ingest_time are already microseconds in ProtocolRecord.to_dict
        event_time = int(record.get('event_time') or int(time.time() * 1_000_000))
        ingest_time = int(record.get('ingest_time') or int(time.time() * 1_000_000))

        endpoint = record.get('endpoint', '')
        source_name = record.get('source_name', '')

        raw_node_id = metadata.get('node_id') or record.get('node_id') or ''

        namespace = 0
        node_id_out = str(raw_node_id)

        # Normalize asyncua NodeId(...) -> ns=X;i=Y when possible
        m = re.search(r"Identifier=(\d+),\s*NamespaceIndex=(\d+)", str(raw_node_id))
        if m:
            ident = int(m.group(1))
            namespace = int(m.group(2))
            node_id_out = f"ns={namespace};i={ident}"
        else:
            m2 = re.search(r"NamespaceIndex=(\d+)", str(raw_node_id))
            if m2:
                namespace = int(m2.group(1))

        browse_path = record.get('topic_or_path') or metadata.get('tag_name') or metadata.get('browse_path') or ''

        # Detect vendor format from browse_path/topic
        vendor_format = record.get('vendor_format', '')
        if not vendor_format:
            # Detect from browse_path if not already set
            topic_lower = str(browse_path).lower()
            protocol_type = record.get('protocol_type', '') or record.get('protocol', '')

            # Check for vendor-specific patterns in browse_path FIRST
            if "sparkplugb" in topic_lower or "sparkplug" in topic_lower:
                vendor_format = "sparkplug_b"
            elif "siemens_s7" in topic_lower or "allen_bradley" in topic_lower or "modicon" in topic_lower or "kepware" in topic_lower:
                vendor_format = "kepware"
            elif topic_lower.startswith("honeywell/") or "experion" in topic_lower or "pks" in topic_lower:
                vendor_format = "honeywell"
            elif "channel" in topic_lower and "device" in topic_lower:
                # Kepware pattern: Channel1.Device1.Tag
                vendor_format = "kepware"
            # Then check protocol type as fallback
            elif 'modbus' in str(protocol_type).lower():
                vendor_format = "modbus"
            elif 'mqtt' in str(protocol_type).lower() and "/" in str(browse_path):
                vendor_format = "mqtt_generic"
            elif 'opcua' in str(protocol_type).lower():
                # Native OPC UA (no vendor wrapper)
                vendor_format = "opcua_native"
            elif "/" in str(browse_path):
                vendor_format = "generic"
            else:
                vendor_format = "unknown"

        msg: Dict[str, Any] = {
            'event_time': event_time,
            'ingest_time': ingest_time,
            'source_name': source_name,
            'endpoint': endpoint,
            'namespace': int(namespace),
            'node_id': node_id_out,
            'browse_path': str(browse_path),
            'status_code': int(record.get('status_code') or 0),
            'status': str(record.get('status') or 'Good'),
            'value_type': str(record.get('value_type') or ''),
            'value': str(record.get('value', '')),
            'value_num': record.get('value_num'),
            # bytes field in JSON form is base64; empty is valid
            'raw': '',
            'plc_name': str(metadata.get('plc_name') or ''),
            'plc_vendor': str(metadata.get('plc_vendor') or ''),
            'plc_model': str(metadata.get('plc_model') or ''),
            'vendor_format': str(vendor_format),  # Vendor format (kepware, honeywell, sparkplug_b, etc.)
        }

        if msg.get('value_num') is None:
            msg.pop('value_num', None)

        return msg


    async def stop(self):
        """Stop all components gracefully."""
        logger.info("Stopping UnifiedBridge...")
        self._shutdown = True
        self._starting = False

        # Stop all protocol clients
        for source_name, client in self.clients.items():
            try:
                logger.info(f"Stopping source: {source_name}")
                await client.stop()
                await client.disconnect()
            except Exception as e:
                logger.error(f"Error stopping {source_name}: {e}")

        # Wait for client tasks
        if self.client_tasks:
            await asyncio.gather(*self.client_tasks.values(), return_exceptions=True)

        # Stop batch processors
        if self.batch_tasks:
            for task in self.batch_tasks.values():
                task.cancel()
            await asyncio.gather(*self.batch_tasks.values(), return_exceptions=True)

        # Close ZeroBus clients
        for target_id, client in self.zerobus_clients.items():
            try:
                await client.close()
                logger.info(f"Closed ZeroBus client: {target_id}")
            except Exception as e:
                logger.error(f"Error closing ZeroBus client {target_id}: {e}")

        logger.info("✓ UnifiedBridge stopped")

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive metrics from all components.

        Returns:
            Metrics dict
        """
        return {
            'bridge': self.metrics,
            'backpressure': self.backpressure.get_metrics(),
            'zerobus_targets': {
                target_id: client.get_metrics()
                for target_id, client in self.zerobus_clients.items()
            },
            'clients': {
                name: {
                    'connected': client._client is not None if hasattr(client, '_client') else False,
                    'protocol': client.protocol_type.value,
                    'endpoint': client.endpoint,
                }
                for name, client in self.clients.items()
            }
        }

    def get_pipeline_diagnostics(self) -> Dict[str, Any]:
        """Get message transformation pipeline diagnostics per vendor format.

        Returns:
            Pipeline diagnostics with per-vendor pipeline stages
        """
        vendor_pipelines_data = {}

        for vendor_name, pipeline in self.vendor_pipelines.items():
            vendor_pipelines_data[vendor_name] = {
                'vendor_name': vendor_name,
                'record_count': pipeline['record_count'],
                'pipeline_stages': [
                    {
                        'stage': 'raw_protocol',
                        'name': 'Raw Protocol',
                        'description': 'Messages as received from OPC UA/MQTT/Modbus',
                        'sample_count': len(pipeline['raw_protocol']),
                        'samples': list(pipeline['raw_protocol'])
                    },
                    {
                        'stage': 'after_vendor_detection',
                        'name': 'Vendor Detection',
                        'description': f'After detecting {vendor_name} format',
                        'sample_count': len(pipeline['after_vendor_detection']),
                        'samples': list(pipeline['after_vendor_detection'])
                    },
                    {
                        'stage': 'after_normalization',
                        'name': 'ISA-95 Normalization',
                        'description': 'After applying ISA-95 tag standardization',
                        'sample_count': len(pipeline['after_normalization']),
                        'samples': list(pipeline['after_normalization'])
                    },
                    {
                        'stage': 'zerobus_batch',
                        'name': 'ZeroBus Batch',
                        'description': 'Final format sent to Databricks Delta table',
                        'sample_count': len(pipeline['zerobus_batch']),
                        'samples': list(pipeline['zerobus_batch'])
                    }
                ]
            }

        return {
            'vendor_pipelines': vendor_pipelines_data,
            'vendor_format_summary': self.metrics['vendor_formats'],
            'protocol_vendor_breakdown': self.metrics['protocol_vendor_breakdown'],
            'total_records_received': self.metrics['records_received'],
            'normalization_enabled': self.normalization_enabled
        }

    async def add_source(self, source_name: str, source_type: str, source_config: Dict[str, Any]):
        """Add a new data source dynamically.

        Args:
            source_name: Unique name for the source
            source_type: Protocol type ('opcua', 'mqtt', 'modbus')
            source_config: Source configuration dict

        Raises:
            ValueError: If source already exists or config is invalid
        """
        if source_name in self.clients:
            raise ValueError(f"Source '{source_name}' already exists")

        logger.info(f"Adding new source: {source_name} (type: {source_type})")

        # Build full source config
        full_config = {
            'name': source_name,
            'protocol': source_type,
            'enabled': True,
            **source_config
        }

        # Add to internal sources list
        self.sources.append(full_config)
        self._refresh_source_lookup()

        # Start the client if bridge is running
        if self.running and not self._shutdown:
            try:
                await self._start_client(full_config)
                logger.info(f"✓ Source '{source_name}' added and started successfully")
            except Exception as e:
                # Remove from sources list if start failed
                self.sources.remove(full_config)
                self._refresh_source_lookup()
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
        if source_name not in self.clients:
            raise ValueError(f"Source '{source_name}' not found")

        logger.info(f"Removing source: {source_name}")

        # Stop the client
        client = self.clients[source_name]
        try:
            await client.stop()
            await client.disconnect()
        except Exception as e:
            logger.error(f"Error stopping source '{source_name}': {e}")

        # Cancel the task
        if source_name in self.client_tasks:
            task = self.client_tasks[source_name]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            del self.client_tasks[source_name]

        # Remove from clients dict
        del self.clients[source_name]

        # Remove from sources list
        self.sources = [s for s in self.sources if s.get('name') != source_name]
        self._refresh_source_lookup()

        logger.info(f"✓ Source '{source_name}' removed successfully")

    async def start_zerobus(self) -> Dict[str, Any]:
        """Start ZeroBus streaming dynamically.

        Returns:
            dict: Status message with success/error
        """
        try:
            if self.zerobus_enabled and self.zerobus_clients:
                return {'status': 'ok', 'message': 'ZeroBus already enabled'}

            logger.info("Starting ZeroBus streaming...")

            # Initialize ZeroBus clients
            await self._initialize_zerobus_clients()

            # Start batch processors
            for target_id in self.zerobus_clients.keys():
                task = asyncio.create_task(self._batch_processor(target_id))
                self.batch_tasks[target_id] = task
                logger.info(f"Batch processor started for target: {target_id}")

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
        try:
            if not self.zerobus_enabled:
                return {'status': 'ok', 'message': 'ZeroBus already disabled'}

            logger.info("Stopping ZeroBus streaming...")

            # Stop batch processors
            if self.batch_tasks:
                for task in self.batch_tasks.values():
                    task.cancel()
                await asyncio.gather(*self.batch_tasks.values(), return_exceptions=True)
                self.batch_tasks.clear()
                logger.info("Batch processors stopped")

            # Close ZeroBus clients
            for target_id, client in self.zerobus_clients.items():
                try:
                    await client.close()
                    logger.info(f"Closed ZeroBus client: {target_id}")
                except Exception as e:
                    logger.error(f"Error closing ZeroBus client {target_id}: {e}")

            self.zerobus_clients.clear()
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
            'zerobus_config_enabled': self.zerobus_config_enabled,
            'zerobus_connected': bool(self.zerobus_clients),
            'zerobus_enabled': self.zerobus_enabled,
            'zerobus_targets': list(self.zerobus_clients.keys()),
            'normalization_enabled': self.normalization_enabled,
            'backpressure_queue_size': self.backpressure.get_metrics().get('queue_size', 0),
            'metrics': self.get_metrics(),
        }
