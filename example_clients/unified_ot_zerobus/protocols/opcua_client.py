"""OPC-UA protocol client implementation using unified protocol interface.

Security Features (OPC UA 10101 compliant):
- Basic256Sha256 encryption (Sign & SignAndEncrypt)
- Enterprise certificate-based authentication
- Username/password authentication
- Server certificate validation
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Callable

from asyncua import Client, Node
from asyncua.ua import DataChangeNotification, Variant
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.x509.oid import NameOID

from unified_connector.protocols.base import (
    ProtocolClient,
    ProtocolRecord,
    ProtocolTestResult,
    ProtocolType,
)
from unified_connector.protocols.opcua_security import (
    OPCUASecurityConfig,
    OPCUASecurityManager,
    create_security_config_from_dict,
)

logger = logging.getLogger(__name__)


class OPCUAClient(ProtocolClient):
    """OPC-UA protocol client with subscription and backpressure handling."""

    def __init__(
        self,
        source_name: str,
        endpoint: str,
        config: dict[str, Any],
        on_record: Callable[[ProtocolRecord], None],
        on_stats: Callable[[dict[str, Any]], None] | None = None,
    ):
        super().__init__(source_name, endpoint, config, on_record, on_stats)

        # OPC-UA specific config
        self.variable_limit = int(config.get("variable_limit", 500))  # Increased to accommodate all sensors
        self.publishing_interval_ms = int(config.get("publishing_interval_ms", 1000))
        self.timeout_s = float(config.get("timeout", 5.0))

        # Subscription mode: use OPC-UA data-change notifications (more efficient)
        # If subscription_mode is True, use subscriptions; otherwise use polling
        subscription_mode = config.get("subscription_mode", False)
        self.polling_mode = not subscription_mode  # Invert: subscription_mode=True means polling_mode=False
        self.polling_interval_ms = int(config.get("polling_interval_ms", 500))  # Match simulator's 2 Hz (500ms)

        # Security configuration
        security_config_dict = config.get("security", {})
        self.security_config = create_security_config_from_dict(security_config_dict)
        self.security_manager = OPCUASecurityManager(self.security_config)

        self._client: Client | None = None
        self._subscription: Any = None
        self._monitored_items: list[Any] = []
        self._variables: list[Node] = []  # Store discovered variables for polling
        self._poll_targets: list[tuple[Node, str, str]] = []  # (node, node_id, tag_name)
        self._node_paths: dict[str, str] = {}  # Mapping from node_id to full browse path
        self._namespaces: list[str] = []

        # Normalization support
        self._normalization_enabled = config.get("normalization_enabled", False)
        self._normalizer = None
        if self._normalization_enabled:
            try:
                from unified_connector.normalizer import get_normalization_manager
                self._norm_manager = get_normalization_manager()
                if self._norm_manager.is_enabled():
                    self._normalizer = self._norm_manager.get_normalizer("opcua")
            except Exception as e:
                logger.warning(f"Could not initialize normalizer: {e}")

    @property
    def protocol_type(self) -> ProtocolType:
        return ProtocolType.OPCUA

    async def _validate_server_certificate(self, cert_path: str) -> bool:
        """
        Validate server certificate before trusting.

        Performs the following validation checks:
        - Certificate file exists and is readable
        - Certificate can be parsed
        - Certificate is not expired
        - Certificate has valid issuer and subject
        - Certificate uses secure hash algorithm (SHA256+)

        Args:
            cert_path: Path to server certificate file

        Returns:
            True if certificate is valid, False otherwise
        """
        try:
            cert_file = Path(cert_path)

            # Check file exists
            if not cert_file.exists():
                logger.error(f"Server certificate file not found: {cert_path}")
                return False

            # Load certificate
            with open(cert_file, 'rb') as f:
                cert_data = f.read()

            # Try DER format first (OPC-UA typically uses DER)
            try:
                cert = x509.load_der_x509_certificate(cert_data, default_backend())
            except Exception:
                # Fallback to PEM format
                try:
                    cert = x509.load_pem_x509_certificate(cert_data, default_backend())
                except Exception as e:
                    logger.error(f"Failed to parse certificate {cert_path}: {e}")
                    return False

            # Validate certificate is not expired
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)

            if cert.not_valid_before_utc > now:
                logger.error(f"Certificate not yet valid. Valid from: {cert.not_valid_before_utc}")
                return False

            if cert.not_valid_after_utc < now:
                logger.error(f"Certificate expired. Valid until: {cert.not_valid_after_utc}")
                return False

            # Validate certificate has required fields
            try:
                subject = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
                if not subject:
                    logger.warning("Certificate has no Common Name (CN)")
                else:
                    cn = subject[0].value
                    logger.info(f"Certificate CN: {cn}")
            except Exception as e:
                logger.warning(f"Could not extract certificate subject: {e}")

            # Check signature algorithm (should be SHA256 or better)
            sig_alg = cert.signature_hash_algorithm
            if sig_alg and hasattr(sig_alg, 'name'):
                if 'sha1' in sig_alg.name.lower() or 'md5' in sig_alg.name.lower():
                    logger.warning(f"Certificate uses weak signature algorithm: {sig_alg.name}")
                    return False
                logger.info(f"Certificate signature algorithm: {sig_alg.name}")

            # Certificate is valid
            logger.info(f"✓ Server certificate validation passed: {cert_path}")
            logger.info(f"  Valid from: {cert.not_valid_before_utc}")
            logger.info(f"  Valid until: {cert.not_valid_after_utc}")

            return True

        except Exception as e:
            logger.error(f"Certificate validation failed for {cert_path}: {e}", exc_info=True)
            return False

    async def connect(self) -> None:
        """Establish OPC-UA connection with security."""
        if self._client is not None:
            return

        # Validate security configuration
        if not self.security_manager.validate_configuration():
            raise ValueError("Invalid OPC UA security configuration")

        # Log security status
        self.security_manager.log_security_status()

        # Create client with timeout
        # Increase watchdog timeout for large subscription counts (1500+ variables)
        # Default asyncua watchdog is 4 seconds, which is too aggressive
        self._client = Client(url=self.endpoint, timeout=self.security_config.timeout_s)

        # Set longer watchdog timeout for subscription mode with many variables
        if hasattr(self._client, 'uaclient') and hasattr(self._client.uaclient, 'set_watchdog_timeout'):
            # Calculate watchdog timeout based on variable count
            # Allow at least 2 seconds per 100 variables for initial subscription setup
            watchdog_timeout = max(30.0, (self.variable_limit / 100) * 2)
            self._client.uaclient.set_watchdog_timeout(watchdog_timeout)
            logger.info(f"Set OPC UA watchdog timeout to {watchdog_timeout}s for {self.variable_limit} variables")

        # Configure security if enabled
        if self.security_config.enabled:
            # Set security policy and mode
            cert_path = self.security_manager.get_certificate_path()
            key_path = self.security_manager.get_private_key_path()

            if cert_path and key_path:
                await self._client.set_security_string(
                    f"{self.security_config.security_policy},"
                    f"{self.security_config.security_mode},"
                    f"{cert_path},{key_path}"
                )
                logger.info("✓ Configured certificate-based security")

            # Set username/password if provided
            if self.security_config.username and self.security_config.password:
                self._client.set_user(self.security_config.username)
                self._client.set_password(self.security_config.password)
                logger.info(f"✓ Configured username authentication: {self.security_config.username}")

            # Configure server certificate trust and validation
            server_cert = self.security_manager.get_server_certificate_path()
            if server_cert:
                # Validate server certificate before trusting
                cert_valid = await self._validate_server_certificate(server_cert)
                if cert_valid:
                    logger.info(f"✓ Server certificate validated and trusted: {server_cert}")
                else:
                    logger.warning(f"⚠️  Server certificate validation failed but proceeding: {server_cert}")
            elif self.security_config.trust_all_certificates:
                logger.warning("⚠️  Trusting ALL server certificates (development mode - NOT for production)")
                # asyncua will accept any server certificate
            else:
                logger.info("Server certificate validation: strict mode (will reject unknown certs)")

        # Connect to server
        try:
            await self._client.connect()
            logger.info(f"✓ Connected to OPC UA server: {self.endpoint}")
        except Exception as e:
            logger.error(f"Failed to connect to {self.endpoint}: {e}")
            self._client = None  # Reset client on connection failure
            raise

        # Load namespace array
        try:
            ns_arr = await self._client.nodes.server_namespace_array.read_value()
            if isinstance(ns_arr, list):
                self._namespaces = ns_arr
                logger.info(f"✓ Loaded {len(self._namespaces)} namespaces")
        except Exception as e:
            logger.warning(f"Could not load namespace array: {e}")
            self._namespaces = []

    async def stop(self) -> None:
        """Stop the OPC-UA subscription."""
        self._stop_evt.set()

    async def disconnect(self) -> None:
        """Disconnect from OPC-UA server."""
        # Clean up monitored items
        if self._monitored_items and self._subscription:
            try:
                await self._subscription.delete(self._monitored_items)
            except Exception:
                pass
            self._monitored_items = []

        # Delete subscription
        if self._subscription:
            try:
                await self._subscription.delete()
            except Exception:
                pass
            self._subscription = None

        # Disconnect client
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                pass
            finally:
                self._client = None

    async def subscribe(self) -> None:
        """Subscribe to OPC-UA variables using polling or subscription mode."""
        if self._client is None:
            raise RuntimeError("Not connected")

        # Find variables to monitor
        variables = await self._discover_variables()
        self._variables = variables[:self.variable_limit]

        # Build polling cache (avoid expensive browse_name reads every poll)
        self._poll_targets = []
        for var_node in self._variables:
            node_id = str(var_node.nodeid)
            # Use the full browse path if available, otherwise fallback to tag name
            browse_path = self._node_paths.get(node_id, node_id)
            try:
                if node_id not in self._node_paths:
                    # Fallback: read browse name if path wasn't stored during discovery
                    browse_name = await var_node.read_browse_name()
                    browse_path = getattr(browse_name, 'Name', node_id)
            except Exception:
                pass
            self._poll_targets.append((var_node, node_id, browse_path))

        # IMPORTANT:
        # `UnifiedBridge` expects `subscribe()` to be a long-running coroutine.
        # If we spawn a background task and return immediately, lifecycle management can't
        # observe failures and reconnect, and read errors can be silently swallowed.
        if self.polling_mode:
            # POLLING MODE: Actively read all values at regular intervals
            # This bypasses server deadband filtering issues
            logger.info(
                f"🔄 Starting POLLING MODE with {len(self._variables)} variables at {self.polling_interval_ms}ms intervals"
            )
            await self._poll_loop()
        else:
            # SUBSCRIPTION MODE: Use OPC-UA data-change notifications
            # Note: May be affected by server deadband filtering
            logger.info(f"🔔 Starting SUBSCRIPTION MODE with {len(self._variables)} variables")
            await self._subscription_loop()

    async def _poll_loop(self) -> None:
        """Poll all variables at regular intervals."""
        logger.info(f"✓ Polling {len(self._variables)} variables every {self.polling_interval_ms}ms")

        poll_count = 0
        consecutive_zero_polls = 0
        while not self._stop_evt.is_set():
            poll_start = time.time()
            records_sent = 0
            errors = 0
            first_error: str | None = None

            # Read values concurrently in small batches to avoid blocking the event loop
            batch_size = 25
            targets = self._poll_targets if self._poll_targets else [(n, str(n.nodeid), str(n.nodeid)) for n in self._variables]

            for i in range(0, len(targets), batch_size):
                batch = targets[i:i+batch_size]

                async def _read_one(t):
                    node, node_id, browse_path = t
                    try:
                        v = await node.read_value()
                        return (node_id, browse_path, v, None)
                    except Exception as e:
                        return (node_id, browse_path, None, e)

                results = await asyncio.gather(*(_read_one(t) for t in batch))

                for node_id, browse_path, data_value, err in results:
                    if err is not None:
                        errors += 1
                        if first_error is None:
                            first_error = f"{type(err).__name__}: {err}"
                        # keep log noise low (we summarize periodically + on repeated total failure)
                        continue

                    record = ProtocolRecord(
                        event_time_ms=int(time.time() * 1000),
                        source_name=self.source_name,
                        endpoint=self.endpoint,
                        protocol_type=self.protocol_type,
                        topic_or_path=browse_path,
                        value=data_value,
                        value_type=type(data_value).__name__,
                        value_num=float(data_value) if isinstance(data_value, (int, float)) else None,
                        status="Good",
                        metadata={"node_id": node_id, "browse_path": browse_path},
                    )
                    self.on_record(record)
                    records_sent += 1

                # Yield to allow web server requests to be handled
                await asyncio.sleep(0)
            poll_count += 1
            if poll_count % 10 == 0:  # Log every 10 polls
                if errors > 0:
                    logger.info(
                        f"Poll #{poll_count}: Sent {records_sent} records "
                        f"(total variables: {len(self._variables)}, errors: {errors}, first_error: {first_error})"
                    )
                else:
                    logger.info(f"Poll #{poll_count}: Sent {records_sent} records (total variables: {len(self._variables)})")

            # If we get repeated total failure (0 successful reads), force reconnect by raising.
            # This fixes the "Poll: Sent 0 records forever" wedge (e.g., session invalid, connection closed, stale nodes).
            if records_sent == 0:
                consecutive_zero_polls += 1
                if consecutive_zero_polls >= 3:
                    raise RuntimeError(
                        f"OPC-UA polling returned 0/{len(self._variables)} values for {consecutive_zero_polls} polls; "
                        f"forcing reconnect (last_error={first_error})"
                    )
            else:
                consecutive_zero_polls = 0

            # Calculate sleep time to maintain polling interval
            poll_duration = (time.time() - poll_start) * 1000  # ms
            sleep_time = max(0, (self.polling_interval_ms - poll_duration) / 1000.0)

            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                logger.warning(f"Polling took {poll_duration:.1f}ms, exceeds interval {self.polling_interval_ms}ms")

    async def _subscription_loop(self) -> None:
        """Use OPC-UA subscriptions with data-change notifications."""
        # Create subscription
        self._subscription = await self._client.create_subscription(
            period=self.publishing_interval_ms,
            handler=self._DataChangeHandler(self._on_datachange),
        )

        # Create monitored items with sampling_interval=0.0 (report ALL value changes)
        # asyncua 1.1.5 doesn't support DataChangeFilter parameter in subscribe_data_change()
        # Instead, use sampling_interval=0.0 to report all changes

        logger.info(f"🔔 Starting to create {len(self._variables)} monitored items...")
        logger.debug(f"   Using sampling_interval=0.0 to report all value changes")

        for idx, var_node in enumerate(self._variables):
            try:
                # Log every 50th item to avoid log spam
                if idx % 50 == 0 or idx < 5:
                    logger.debug(f"   Creating monitored item {idx+1}/{len(self._variables)} for NodeId: {var_node.nodeid}")

                handle = await self._subscription.subscribe_data_change(
                    var_node,
                    sampling_interval=0.0,  # 0.0 = report all changes
                    queuesize=10  # Queue up to 10 notifications per item
                )
                self._monitored_items.append(handle)

                if idx % 50 == 0 or idx < 5:
                    logger.debug(f"   ✓ Created monitored item {idx+1}, handle: {handle}")
            except Exception as e:
                logger.error(f"   ✗ Failed to create monitored item {idx+1} for NodeId {var_node.nodeid}: {type(e).__name__}: {e}")
                self._emit_stats({
                    "subscribe_error": f"{type(e).__name__}: {e}",
                    "node": str(var_node),
                })

        logger.info(f"✓ Finished creating monitored items. Total: {len(self._monitored_items)}")
        self._emit_stats({
            "subscribed_variables": len(self._monitored_items),
            "subscription_interval_ms": self.publishing_interval_ms,
        })

        # Keep subscription alive until stop is requested
        while not self._stop_evt.is_set():
            await asyncio.sleep(1.0)

    async def _discover_variables(self) -> list[Node]:
        """Discover variables in the server."""
        if self._client is None:
            return []

        variables: list[Node] = []
        logger.info("🔍 Starting node discovery...")

        async def browse_node(node: Node, max_depth: int = 5, current_depth: int = 0, path: str = "") -> None:
            """Recursively browse nodes to find variables."""
            if current_depth >= max_depth or len(variables) >= self.variable_limit:
                if current_depth >= max_depth:
                    logger.debug(f"   Max depth {max_depth} reached at: {path}")
                return

            try:
                # Get node info for logging
                try:
                    browse_name = await node.read_browse_name()
                    node_name = browse_name.Name
                    current_path = f"{path}/{node_name}" if path else node_name
                except Exception:
                    current_path = f"{path}/[unknown]"
                    node_name = "[unknown]"

                # Get children
                children = await node.get_children()
                logger.debug(f"   Depth {current_depth}: {current_path} has {len(children)} children")

                for child in children:
                    if len(variables) >= self.variable_limit:
                        logger.info(f"   ✓ Variable limit {self.variable_limit} reached")
                        break

                    try:
                        # Get child info
                        child_browse_name = await child.read_browse_name()
                        child_name = child_browse_name.Name
                        child_path = f"{current_path}/{child_name}"

                        # Check if it's a variable
                        node_class = await child.read_node_class()
                        if node_class.value == 2:  # Variable NodeClass
                            variables.append(child)
                            # Store the full browse path for this node
                            self._node_paths[str(child.nodeid)] = child_path
                            logger.debug(f"   ✓ Found variable #{len(variables)}: {child_path}")
                        else:
                            # Log other node types
                            node_type = {1: "Object", 4: "Method", 8: "View"}.get(node_class.value, f"Type{node_class.value}")
                            logger.debug(f"   → {node_type}: {child_path}")
                            # Continue browsing
                            await browse_node(child, max_depth, current_depth + 1, current_path)
                    except Exception as e:
                        logger.debug(f"   ⚠ Error browsing child at {current_path}: {e}")
                        continue

            except Exception as e:
                logger.debug(f"   ⚠ Error browsing node at {path}: {e}")
                pass

        # Start from IndustrialSensors namespace (sensor data nodes)
        try:
            objects_node = self._client.nodes.objects
            logger.info("📂 Browsing Objects root node...")

            # Try to find IndustrialSensors and VendorModes folders
            try:
                children = await objects_node.get_children()
                logger.info(f"   Objects has {len(children)} direct children")

                industrial_sensors_node = None
                vendor_modes_node = None

                for child in children:
                    try:
                        browse_name = await child.read_browse_name()
                        logger.debug(f"   → Found child: {browse_name.Name}")
                        if browse_name.Name == "IndustrialSensors":
                            industrial_sensors_node = child
                            logger.info(f"✓ Found IndustrialSensors node!")
                        elif browse_name.Name == "VendorModes":
                            vendor_modes_node = child
                            logger.info(f"✓ Found VendorModes node!")
                    except Exception as e:
                        logger.debug(f"   ⚠ Error reading child browse name: {e}")
                        continue

                # Browse both IndustrialSensors and VendorModes if available
                if industrial_sensors_node or vendor_modes_node:
                    if industrial_sensors_node:
                        # Browse IndustrialSensors folder (standard sensor data)
                        logger.info("🎯 Browsing from IndustrialSensors namespace...")
                        await browse_node(industrial_sensors_node, path="IndustrialSensors")
                        logger.info(f"✓ IndustrialSensors: {len(variables)} variables found")

                    if vendor_modes_node:
                        # Browse VendorModes folder (Kepware, Honeywell, Sparkplug B, Generic)
                        logger.info("🎯 Browsing from VendorModes namespace...")
                        vendor_start = len(variables)
                        await browse_node(vendor_modes_node, path="VendorModes")
                        vendor_count = len(variables) - vendor_start
                        logger.info(f"✓ VendorModes: {vendor_count} vendor-specific variables found")

                    logger.info(f"✓ Discovery complete: {len(variables)} total variables found")
                else:
                    # Fallback to Objects root if neither found
                    logger.warning("⚠ IndustrialSensors and VendorModes not found, browsing from Objects root...")
                    await browse_node(objects_node, path="Objects")
                    logger.info(f"✓ Discovery complete: {len(variables)} variables found")

            except Exception as e:
                # Fallback to Objects root on error
                logger.error(f"Error during IndustrialSensors search: {e}, browsing from Objects root...")
                await browse_node(objects_node, path="Objects")
                logger.info(f"✓ Discovery complete: {len(variables)} variables found")

        except Exception as e:
            logger.error(f"Critical error during node discovery: {e}")
            pass

        # Log summary of discovered variables
        if variables:
            logger.info(f"📊 Discovered {len(variables)} variables:")
            for i, var in enumerate(variables[:10]):  # Show first 10
                try:
                    browse_name = await var.read_browse_name()
                    logger.info(f"   {i+1}. {browse_name.Name} (NodeId: {var.nodeid})")
                except Exception:
                    logger.info(f"   {i+1}. [error reading name] (NodeId: {var.nodeid})")
            if len(variables) > 10:
                logger.info(f"   ... and {len(variables) - 10} more")

        return variables

    def _on_datachange(
        self,
        node: Node,
        val: Any,
        data: DataChangeNotification,
    ) -> None:
        """Handle data change notification."""
        try:
            # Update last data time
            self._last_data_time = time.time()

            # Extract node information
            node_id_str = str(node.nodeid)
            namespace = node.nodeid.NamespaceIndex if hasattr(node.nodeid, 'NamespaceIndex') else 0

            # Extract value information
            value = val
            value_type = type(val).__name__
            value_num = None

            if isinstance(val, (int, float)):
                value_num = float(val)
            elif isinstance(val, Variant):
                value = val.Value
                value_type = str(val.VariantType)
                if isinstance(val.Value, (int, float)):
                    value_num = float(val.Value)

            # Extract status
            status_code = data.monitored_item.Value.StatusCode.value if hasattr(data, 'monitored_item') else 0
            status = "Good" if status_code == 0 else f"Bad({status_code})"

            # Get browse path from stored mapping (fallback to node_id)
            browse_path = self._node_paths.get(node_id_str, node_id_str)

            # Check if normalization is enabled
            if self._normalizer:
                # Create raw data dict for normalizer
                raw_data = self._create_raw_data(node_id_str, namespace, value, status_code, browse_path)
                try:
                    normalized = self._normalizer.normalize(raw_data)
                    # Send normalized data
                    self.on_record(normalized.to_dict())
                except Exception as norm_error:
                    # Normalization failed, fall back to raw mode
                    self._emit_stats({
                        "normalization_error": f"{type(norm_error).__name__}: {norm_error}",
                        "node_id": node_id_str,
                    })
                    # Create and send raw record as fallback
                    record = ProtocolRecord(
                        event_time_ms=int(time.time() * 1000),
                        source_name=self.source_name,
                        endpoint=self.endpoint,
                        protocol_type=self.protocol_type,
                        topic_or_path=browse_path,
                        value=value,
                        value_type=value_type,
                        value_num=value_num,
                        metadata={
                            "namespace": namespace,
                            "node_id": node_id_str,
                            "status_code": status_code,
                        },
                        status_code=status_code,
                        status=status,
                    )
                    self.on_record(record)
            else:
                # Raw mode (existing behavior)
                record = ProtocolRecord(
                    event_time_ms=int(time.time() * 1000),
                    source_name=self.source_name,
                    endpoint=self.endpoint,
                    protocol_type=self.protocol_type,
                    topic_or_path=browse_path,
                    value=value,
                    value_type=value_type,
                    value_num=value_num,
                    metadata={
                        "namespace": namespace,
                        "node_id": node_id_str,
                        "status_code": status_code,
                    },
                    status_code=status_code,
                    status=status,
                )
                self.on_record(record)

        except Exception as e:
            self._emit_stats({
                "datachange_error": f"{type(e).__name__}: {e}",
            })

    def _create_raw_data(
        self,
        node_id: str,
        namespace: int,
        value: Any,
        status_code: int,
        browse_path: str
    ) -> dict[str, Any]:
        """
        Create raw data dict for normalizer from OPC-UA data change.

        Args:
            node_id: Node ID string
            namespace: Namespace index
            value: Value from node
            status_code: OPC-UA status code
            browse_path: Browse path string

        Returns:
            Dictionary with normalized format expected by OPCUANormalizer
        """
        # Handle Variant type
        if isinstance(value, Variant):
            actual_value = value.Value
        else:
            actual_value = value

        return {
            "node_id": node_id,
            "value": {
                "value": actual_value,
                "source_timestamp": int(time.time() * 1000),  # Current time in ms
                "status_code": status_code,
            },
            "browse_path": browse_path,
            "server_url": self.endpoint,
            "config": self.config,  # Pass config for context extraction
        }

    class _DataChangeHandler:
        """Handler for OPC-UA data change notifications."""

        def __init__(self, callback: Callable) -> None:
            self.callback = callback

        def datachange_notification(
            self,
            node: Node,
            val: Any,
            data: DataChangeNotification,
        ) -> None:
            """Called when a monitored item value changes."""
            self.callback(node, val, data)

        def status_change_notification(self, status: Any) -> None:
            """Called when subscription status changes."""
            # Log status changes but don't raise errors
            pass

    async def test_connection(self) -> ProtocolTestResult:
        """Test OPC-UA connectivity."""
        start_time = time.time()
        error = None
        server_info: dict[str, Any] = {}

        try:
            await self.connect()

            # Read server information
            try:
                app_desc = await self._client.nodes.server.read_description()
                server_info["application_name"] = str(getattr(app_desc, 'Text', app_desc))
            except Exception:
                pass

            try:
                ns_arr = await self._client.nodes.server_namespace_array.read_value()
                if isinstance(ns_arr, list):
                    server_info["namespaces"] = len(ns_arr)
            except Exception:
                pass

            try:
                status = await self._client.nodes.server_status.read_value()
                if hasattr(status, 'BuildInfo'):
                    build_info = status.BuildInfo
                    server_info["product_uri"] = getattr(build_info, 'ProductUri', None)
                    server_info["manufacturer"] = getattr(build_info, 'ManufacturerName', None)
            except Exception:
                pass

            await self.disconnect()
            ok = True

        except Exception as e:
            ok = False
            error = f"{type(e).__name__}: {e}"
        finally:
            duration_ms = int((time.time() - start_time) * 1000)

        return ProtocolTestResult(
            ok=ok,
            endpoint=self.endpoint,
            protocol_type=self.protocol_type,
            duration_ms=duration_ms,
            server_info=server_info if ok else None,
            error=error,
        )
