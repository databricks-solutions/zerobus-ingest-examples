"""Protocol server discovery service.

Automatically discovers OPC-UA, MQTT, and Modbus servers on the network.
Combines network scanning with protocol-specific connectivity tests.
"""

import asyncio
import ipaddress
import logging
import socket
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ProtocolType(str, Enum):
    """Supported protocol types."""
    OPCUA = "opcua"
    MQTT = "mqtt"
    MODBUS = "modbus"


@dataclass
class DiscoveredServer:
    """Represents a discovered protocol server."""
    protocol: ProtocolType
    host: str
    port: int
    endpoint: str
    name: Optional[str] = None
    version: Optional[str] = None
    vendor: Optional[str] = None
    metadata: Dict[str, Any] = None
    discovered_at: datetime = None
    reachable: bool = True

    def __post_init__(self):
        if self.discovered_at is None:
            self.discovered_at = datetime.utcnow()
        if self.metadata is None:
            self.metadata = {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "protocol": self.protocol.value,
            "host": self.host,
            "port": self.port,
            "endpoint": self.endpoint,
            "name": self.name,
            "version": self.version,
            "vendor": self.vendor,
            "metadata": self.metadata,
            "discovered_at": self.discovered_at.isoformat() if self.discovered_at else None,
            "reachable": self.reachable
        }


class DiscoveryService:
    """Protocol server discovery service."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize discovery service.

        Args:
            config: Discovery configuration from config.yaml
        """
        self.config = config
        self.discovered_servers: Dict[str, DiscoveredServer] = {}
        self._discovery_task: Optional[asyncio.Task] = None
        self._shutdown = False

        # Protocol configurations
        self.opcua_config = config.get("opcua", {})
        self.mqtt_config = config.get("mqtt", {})
        self.modbus_config = config.get("modbus", {})

        # Network scan settings
        self.network_config = config.get("network", {})
        self.scan_enabled = self.network_config.get("scan_enabled", True)
        self.scan_interval_sec = self.network_config.get("scan_interval_sec", 300)
        self.subnets = self.network_config.get("subnets", ["192.168.0.0/24"])

    async def start(self):
        """Start periodic discovery."""
        if not self.scan_enabled:
            logger.info("Network scanning disabled")
            return

        logger.info(f"Starting protocol discovery service (scan interval: {self.scan_interval_sec}s)")
        self._discovery_task = asyncio.create_task(self._discovery_loop())

    async def stop(self):
        """Stop discovery service."""
        self._shutdown = True
        if self._discovery_task:
            self._discovery_task.cancel()
            try:
                await self._discovery_task
            except asyncio.CancelledError:
                pass
        logger.info("Discovery service stopped")

    async def _discovery_loop(self):
        """Periodic discovery loop."""
        while not self._shutdown:
            try:
                logger.info("Starting network scan for protocol servers...")
                await self.discover_all()
                logger.info(f"Discovery complete. Found {len(self.discovered_servers)} servers")

                # Wait for next scan
                await asyncio.sleep(self.scan_interval_sec)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Discovery error: {e}")
                await asyncio.sleep(60)  # Retry after error

    async def discover_all(self) -> List[DiscoveredServer]:
        """Discover all protocol servers on configured subnets.

        Returns:
            List of discovered servers
        """
        discovered = []

        # Get all IPs to scan
        ips_to_scan = []
        for subnet in self.subnets:
            try:
                network = ipaddress.ip_network(subnet, strict=False)

                # ipaddress.hosts() excludes the single address for /32 and /128.
                # Treat single-address networks as an explicit host scan.
                if getattr(network, 'num_addresses', 0) == 1:
                    ips_to_scan.append(str(network.network_address))
                else:
                    ips_to_scan.extend([str(ip) for ip in network.hosts()])
            except Exception as e:
                logger.error(f"Invalid subnet {subnet}: {e}")

        logger.info(f"Scanning {len(ips_to_scan)} IPs across {len(self.subnets)} subnets")

        # Scan protocols in parallel
        tasks = []

        if self.opcua_config.get("enabled", True):
            tasks.append(self._discover_opcua_servers(ips_to_scan))

        if self.mqtt_config.get("enabled", True):
            tasks.append(self._discover_mqtt_brokers(ips_to_scan))

        if self.modbus_config.get("enabled", True):
            tasks.append(self._discover_modbus_servers(ips_to_scan))

        # Gather all results
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Discovery error: {result}")
            elif isinstance(result, list):
                discovered.extend(result)

        # Update discovered servers cache
        for server in discovered:
            server_key = f"{server.protocol.value}://{server.host}:{server.port}"
            self.discovered_servers[server_key] = server

        return discovered

    def _cache_server(self, server: DiscoveredServer) -> None:
        """Insert/update a single discovered server in the cache."""
        server_key = f"{server.protocol.value}://{server.host}:{server.port}"
        self.discovered_servers[server_key] = server

    async def _discover_opcua_servers(self, ips: List[str]) -> List[DiscoveredServer]:
        """Discover OPC-UA servers.

        Args:
            ips: List of IP addresses to scan

        Returns:
            List of discovered OPC-UA servers
        """
        discovered = []
        default_port = self.opcua_config.get("default_port", 4840)
        vendor_ports = self.opcua_config.get("vendor_ports", [])
        timeout = self.opcua_config.get("timeout_sec", 5.0)

        # Scan both default and vendor-specific ports
        ports = [default_port] + vendor_ports
        logger.info(f"Discovering OPC-UA servers on ports {ports}...")

        # Scan IPs in batches to avoid overwhelming the network
        batch_size = 20
        for i in range(0, len(ips), batch_size):
            batch = ips[i:i+batch_size]
            tasks = []
            for ip in batch:
                for port in ports:
                    tasks.append(self._test_opcua_endpoint(ip, port, timeout))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, DiscoveredServer):
                    discovered.append(result)
                    self._cache_server(result)
                    logger.info(f"  ✓ Found OPC-UA server: {result.endpoint}")

        logger.info(f"Found {len(discovered)} OPC-UA servers")
        return discovered

    async def _test_opcua_endpoint(
        self,
        host: str,
        port: int,
        timeout: float
    ) -> Optional[DiscoveredServer]:
        """Test if OPC-UA server is available at endpoint.

        Args:
            host: Server hostname/IP
            port: Server port
            timeout: Connection timeout in seconds

        Returns:
            DiscoveredServer if reachable, None otherwise
        """
        # First check if port is open
        if not await self._is_port_open(host, port, timeout):
            return None

        # If the port is open but asyncua cannot complete an OPC-UA handshake
        # (e.g., dependency/runtime incompatibilities), still surface the server
        # in discovery as "unverified" rather than hiding it.
        base_endpoint = f"opc.tcp://{host}:{port}"

        # Try to connect with asyncua
        try:
            from asyncua import Client

            endpoints = [
                f"opc.tcp://{host}:{port}/ot-simulator/server/",
                f"opc.tcp://{host}:{port}/",
                f"opc.tcp://{host}:{port}",
            ]

            last_err: Exception | None = None
            for endpoint in endpoints:
                client = Client(url=endpoint, timeout=timeout)

                try:
                    await client.connect()

                    # Try to read server information
                    server_info = {}
                    try:
                        app_desc = await client.nodes.server.read_description()
                        server_info["name"] = str(getattr(app_desc, "Text", "Unknown"))
                    except Exception:
                        server_info["name"] = f"OPC-UA Server ({host})"

                    try:
                        ns_arr = await client.nodes.server_namespace_array.read_value()
                        if isinstance(ns_arr, list):
                            server_info["namespaces"] = len(ns_arr)
                    except Exception:
                        pass

                    try:
                        status = await client.nodes.server_status.read_value()
                        if hasattr(status, "BuildInfo"):
                            build_info = status.BuildInfo
                            server_info["vendor"] = getattr(build_info, "ManufacturerName", None)
                            server_info["version"] = getattr(build_info, "SoftwareVersion", None)
                    except Exception:
                        pass

                    await client.disconnect()

                    return DiscoveredServer(
                        protocol=ProtocolType.OPCUA,
                        host=host,
                        port=port,
                        endpoint=endpoint,
                        name=server_info.get("name", f"OPC-UA Server ({host})"),
                        version=server_info.get("version"),
                        vendor=server_info.get("vendor"),
                        metadata=server_info,
                        reachable=True
                    )

                except Exception as e:
                    last_err = e
                    logger.debug(f"OPC-UA connection failed for {endpoint}: {e}")
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

            # Port is open but we couldn't complete a handshake; report unverified.
            meta: Dict[str, Any] = {
                "verified": False,
                "error": str(last_err) if last_err else "unknown",
            }
            return DiscoveredServer(
                protocol=ProtocolType.OPCUA,
                host=host,
                port=port,
                endpoint=base_endpoint,
                name=f"OPC-UA Server ({host})",
                version=None,
                vendor=None,
                metadata=meta,
                reachable=False,
            )

        except ImportError:
            logger.warning("asyncua not installed, reporting OPC-UA server as unverified")
            return DiscoveredServer(
                protocol=ProtocolType.OPCUA,
                host=host,
                port=port,
                endpoint=base_endpoint,
                name=f"OPC-UA Server ({host})",
                version=None,
                vendor=None,
                metadata={"verified": False, "error": "asyncua not installed"},
                reachable=False,
            )
        except Exception as e:
            logger.debug(f"OPC-UA test failed for {host}:{port}: {e}")
            return DiscoveredServer(
                protocol=ProtocolType.OPCUA,
                host=host,
                port=port,
                endpoint=base_endpoint,
                name=f"OPC-UA Server ({host})",
                version=None,
                vendor=None,
                metadata={"verified": False, "error": str(e)},
                reachable=False,
            )

    async def _discover_mqtt_brokers(self, ips: List[str]) -> List[DiscoveredServer]:
        """Discover MQTT brokers.

        Args:
            ips: List of IP addresses to scan

        Returns:
            List of discovered MQTT brokers
        """
        discovered = []
        default_port = self.mqtt_config.get("default_port", 1883)
        timeout = self.mqtt_config.get("timeout_sec", 3.0)

        logger.info(f"Discovering MQTT brokers on port {default_port}...")

        # Scan IPs in batches
        batch_size = 20
        for i in range(0, len(ips), batch_size):
            batch = ips[i:i+batch_size]
            tasks = [self._test_mqtt_broker(ip, default_port, timeout) for ip in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, DiscoveredServer):
                    discovered.append(result)
                    self._cache_server(result)
                    logger.info(f"  ✓ Found MQTT broker: {result.host}:{result.port}")

        logger.info(f"Found {len(discovered)} MQTT brokers")
        return discovered

    async def _test_mqtt_broker(
        self,
        host: str,
        port: int,
        timeout: float
    ) -> Optional[DiscoveredServer]:
        """Test if MQTT broker is available.

        Args:
            host: Broker hostname/IP
            port: Broker port
            timeout: Connection timeout

        Returns:
            DiscoveredServer if reachable, None otherwise
        """
        # Check if port is open
        if not await self._is_port_open(host, port, timeout):
            return None

        # For MQTT, we just verify port is open
        # Full connection test would require asyncio_mqtt
        return DiscoveredServer(
            protocol=ProtocolType.MQTT,
            host=host,
            port=port,
            endpoint=f"mqtt://{host}:{port}",
            name=f"MQTT Broker ({host})",
            reachable=True
        )

    async def _discover_modbus_servers(self, ips: List[str]) -> List[DiscoveredServer]:
        """Discover Modbus TCP servers.

        Args:
            ips: List of IP addresses to scan

        Returns:
            List of discovered Modbus servers
        """
        discovered = []
        default_port = self.modbus_config.get("default_tcp_port", 502)
        alt_port = self.modbus_config.get("alt_tcp_port", 5020)
        timeout = self.modbus_config.get("timeout_sec", 2.0)

        logger.info(f"Discovering Modbus TCP servers on ports {default_port}, {alt_port}...")

        # Scan both standard and alternate ports
        ports = [default_port, alt_port]

        batch_size = 20
        for i in range(0, len(ips), batch_size):
            batch = ips[i:i+batch_size]
            tasks = []
            for ip in batch:
                for port in ports:
                    tasks.append(self._test_modbus_server(ip, port, timeout))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, DiscoveredServer):
                    discovered.append(result)
                    self._cache_server(result)
                    logger.info(f"  ✓ Found Modbus server: {result.host}:{result.port}")

        logger.info(f"Found {len(discovered)} Modbus servers")
        return discovered

    async def _test_modbus_server(
        self,
        host: str,
        port: int,
        timeout: float
    ) -> Optional[DiscoveredServer]:
        """Test if Modbus TCP server is available.

        Args:
            host: Server hostname/IP
            port: Server port
            timeout: Connection timeout

        Returns:
            DiscoveredServer if reachable, None otherwise
        """
        # Check if port is open
        if not await self._is_port_open(host, port, timeout):
            return None

        # For Modbus, we just verify port is open
        # Full connection test would require pymodbus
        return DiscoveredServer(
            protocol=ProtocolType.MODBUS,
            host=host,
            port=port,
            endpoint=f"modbus://{host}:{port}",
            name=f"Modbus Server ({host})",
            reachable=True
        )

    async def _is_port_open(self, host: str, port: int, timeout: float) -> bool:
        """Check if a TCP port is open.

        Args:
            host: Host to check
            port: Port to check
            timeout: Connection timeout in seconds

        Returns:
            True if port is open, False otherwise
        """
        try:
            # Create socket connection with timeout
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )
            writer.close()
            await writer.wait_closed()
            return True
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            return False

    def get_discovered_servers(
        self,
        protocol: Optional[ProtocolType] = None
    ) -> List[DiscoveredServer]:
        """Get list of discovered servers.

        Args:
            protocol: Filter by protocol type (None = all)

        Returns:
            List of discovered servers
        """
        servers = list(self.discovered_servers.values())

        if protocol:
            servers = [s for s in servers if s.protocol == protocol]

        return servers

    async def test_server_connection(
        self,
        protocol: ProtocolType,
        host: str,
        port: int
    ) -> Dict[str, Any]:
        """Test connection to a specific server.

        Args:
            protocol: Protocol type
            host: Server host
            port: Server port

        Returns:
            Test result dictionary
        """
        logger.info(f"Testing {protocol.value} connection to {host}:{port}")
        t0 = asyncio.get_running_loop().time()

        diagnostics: Dict[str, Any] = {
            "protocol": protocol.value,
            "host": host,
            "port": port,
        }

        try:
            # Basic TCP check first (fast + useful diagnostics)
            port_open = await self._is_port_open(host, port, 2.0)
            diagnostics["tcp_port_open"] = bool(port_open)

            if protocol == ProtocolType.OPCUA:
                result = await self._test_opcua_endpoint(host, port, 5.0)
            elif protocol == ProtocolType.MQTT:
                # We treat "reachable" as TCP-port-open for MQTT in discovery
                result = await self._test_mqtt_broker(host, port, 3.0)
            elif protocol == ProtocolType.MODBUS:
                result = await self._test_modbus_server(host, port, 2.0)
            else:
                return {"ok": False, "error": f"Unknown protocol: {protocol}", "diagnostics": diagnostics}

            diagnostics["duration_ms"] = int((asyncio.get_running_loop().time() - t0) * 1000)

            if result:
                return {"ok": True, "server": result.to_dict(), "diagnostics": diagnostics}
            return {"ok": False, "error": "Connection failed", "diagnostics": diagnostics}
        except Exception as e:
            diagnostics["duration_ms"] = int((asyncio.get_running_loop().time() - t0) * 1000)
            diagnostics["exception"] = f"{type(e).__name__}: {e}"
            return {"ok": False, "error": str(e), "diagnostics": diagnostics}
