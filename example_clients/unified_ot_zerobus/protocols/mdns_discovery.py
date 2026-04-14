"""
mDNS/Zeroconf Service Discovery

Provides automatic service discovery using multicast DNS (Bonjour/Avahi).
Discovers OPC UA servers, MQTT brokers, Modbus devices, and other IoT services
that advertise themselves via mDNS.
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
try:
    from zeroconf import Zeroconf, ServiceBrowser, ServiceInfo
    from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser, AsyncServiceInfo
    ZEROCONF_AVAILABLE = True
except ImportError:
    ZEROCONF_AVAILABLE = False
    logging.warning("zeroconf library not available - mDNS discovery disabled")

logger = logging.getLogger(__name__)


class MDNSDiscovery:
    """mDNS/Zeroconf service discovery."""

    # Service types to discover
    SERVICE_TYPES = [
        "_opcua-tcp._tcp.local.",      # OPC UA over TCP
        "_mqtt._tcp.local.",            # MQTT brokers
        "_mqtts._tcp.local.",           # MQTT over TLS
        "_modbus._tcp.local.",          # Modbus TCP
        "_bacnet._udp.local.",          # BACnet
        "_http._tcp.local.",            # HTTP (for web services)
        "_opc._tcp.local.",             # Generic OPC
    ]

    def __init__(self):
        """Initialize mDNS discovery service."""
        if not ZEROCONF_AVAILABLE:
            logger.warning("mDNS discovery unavailable - install zeroconf: pip install zeroconf")
        self.discovered_services = []

    async def discover_services(
        self,
        service_types: List[str] = None,
        timeout: float = 5.0
    ) -> List[Dict[str, Any]]:
        """
        Discover services via mDNS.

        Args:
            service_types: List of service types to discover (defaults to SERVICE_TYPES)
            timeout: Discovery timeout in seconds

        Returns:
            List of discovered services
        """
        if not ZEROCONF_AVAILABLE:
            logger.error("mDNS discovery not available - zeroconf library not installed")
            return []

        if service_types is None:
            service_types = self.SERVICE_TYPES

        discovered = []

        try:
            async_zeroconf = AsyncZeroconf()

            logger.info(f"Starting mDNS discovery for {len(service_types)} service types...")

            # Browse each service type
            tasks = []
            for service_type in service_types:
                tasks.append(self._browse_service_type(async_zeroconf, service_type, timeout))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Collect all discovered services
            for result in results:
                if isinstance(result, list):
                    discovered.extend(result)

            await async_zeroconf.async_close()

            logger.info(f"mDNS discovery complete: found {len(discovered)} service(s)")

        except Exception as e:
            logger.error(f"mDNS discovery error: {e}")

        return discovered

    async def _browse_service_type(
        self,
        async_zeroconf: 'AsyncZeroconf',
        service_type: str,
        timeout: float
    ) -> List[Dict[str, Any]]:
        """Browse a specific mDNS service type."""
        discovered = []

        try:
            # Get service browser
            zc = await async_zeroconf.async_get_service_info(service_type, timeout=timeout)

            if zc:
                # Query for all services of this type
                services = zc.list_services()

                for service_name in services:
                    info = await async_zeroconf.async_get_service_info(service_type, service_name, timeout=2.0)

                    if info:
                        service_info = self._parse_service_info(info, service_type)
                        if service_info:
                            discovered.append(service_info)
                            logger.info(f"✓ Found {service_type}: {service_info.get('name')}")

        except Exception as e:
            logger.debug(f"Error browsing {service_type}: {e}")

        return discovered

    def _parse_service_info(self, info: 'ServiceInfo', service_type: str) -> Optional[Dict[str, Any]]:
        """Parse mDNS service info into standardized format."""
        try:
            # Determine protocol from service type
            protocol = "unknown"
            if "_opcua" in service_type or "_opc" in service_type:
                protocol = "opcua"
            elif "_mqtt" in service_type:
                protocol = "mqtt"
            elif "_modbus" in service_type:
                protocol = "modbus"
            elif "_bacnet" in service_type:
                protocol = "bacnet"

            # Get addresses
            addresses = []
            if info.parsed_addresses():
                addresses = [addr for addr in info.parsed_addresses()]

            if not addresses:
                return None

            # Build service info
            service_info = {
                'protocol': protocol,
                'name': info.name,
                'service_type': service_type,
                'addresses': addresses,
                'port': info.port,
                'server': info.server,
                'discovered_via': 'mdns'
            }

            # Add protocol-specific endpoint
            primary_addr = addresses[0]
            if protocol == "opcua":
                service_info['endpoint'] = f"opc.tcp://{primary_addr}:{info.port}"
            elif protocol == "mqtt":
                service_info['endpoint'] = f"{primary_addr}:{info.port}"
                service_info['tls'] = "_mqtts" in service_type
            elif protocol == "modbus":
                service_info['endpoint'] = f"{primary_addr}:{info.port}"
                service_info['type'] = "TCP"

            # Parse TXT records
            if info.properties:
                properties = {}
                for key, value in info.properties.items():
                    try:
                        properties[key.decode('utf-8')] = value.decode('utf-8')
                    except:
                        pass
                if properties:
                    service_info['properties'] = properties

            return service_info

        except Exception as e:
            logger.debug(f"Error parsing service info: {e}")
            return None

    async def advertise_service(
        self,
        service_type: str,
        name: str,
        port: int,
        properties: Dict[str, str] = None
    ) -> bool:
        """
        Advertise a service via mDNS.

        Args:
            service_type: mDNS service type (e.g., "_opcua-tcp._tcp.local.")
            name: Service name
            port: Service port
            properties: Optional TXT record properties

        Returns:
            True if service registered successfully
        """
        if not ZEROCONF_AVAILABLE:
            logger.error("mDNS advertising not available - zeroconf library not installed")
            return False

        try:
            import socket

            # Get local IP
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)

            # Create service info
            info = ServiceInfo(
                service_type,
                f"{name}.{service_type}",
                addresses=[socket.inet_aton(local_ip)],
                port=port,
                properties=properties or {},
                server=f"{hostname}.local."
            )

            # Register service
            zeroconf = Zeroconf()
            zeroconf.register_service(info)

            logger.info(f"✓ Advertised {name} via mDNS on port {port}")
            return True

        except Exception as e:
            logger.error(f"Failed to advertise service via mDNS: {e}")
            return False


# Global discovery instance
_discovery = None


def get_discovery() -> MDNSDiscovery:
    """Get global discovery instance."""
    global _discovery
    if _discovery is None:
        _discovery = MDNSDiscovery()
    return _discovery


def is_available() -> bool:
    """Check if mDNS discovery is available."""
    return ZEROCONF_AVAILABLE
