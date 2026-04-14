"""
OPC UA Server Discovery

Provides network scanning capabilities to discover OPC UA servers on the local network.
Uses the OPC UA Discovery Service to find available servers.
"""

import asyncio
import logging
from typing import List, Dict, Any
from asyncua import Client

logger = logging.getLogger(__name__)


class OPCUADiscovery:
    """OPC UA server discovery service."""

    def __init__(self):
        """Initialize discovery service."""
        pass

    async def discover_servers(self, discovery_url: str = "opc.tcp://localhost:4840", timeout: float = 5.0) -> List[Dict[str, Any]]:
        """
        Discover OPC UA servers on the network.

        Args:
            discovery_url: Discovery server URL (usually localhost:4840)
            timeout: Discovery timeout in seconds

        Returns:
            List of discovered servers with their details
        """
        discovered = []

        try:
            client = Client(url=discovery_url, timeout=timeout)

            # Find servers
            logger.info(f"Discovering OPC UA servers via {discovery_url}...")
            servers = await asyncio.wait_for(
                client.find_servers(),
                timeout=timeout
            )

            for server in servers:
                server_info = {
                    'application_uri': server.ApplicationUri,
                    'product_uri': server.ProductUri if hasattr(server, 'ProductUri') else None,
                    'application_name': str(server.ApplicationName) if server.ApplicationName else 'Unknown',
                    'application_type': str(server.ApplicationType) if hasattr(server, 'ApplicationType') else 'Server',
                    'discovery_urls': server.DiscoveryUrls if server.DiscoveryUrls else [],
                    'discovery_url': discovery_url,
                }

                # Get endpoints for each discovery URL
                for url in server.DiscoveryUrls[:1]:  # Just check first URL
                    try:
                        endpoint_client = Client(url=url, timeout=2.0)
                        endpoints = await asyncio.wait_for(
                            endpoint_client.get_endpoints(),
                            timeout=2.0
                        )

                        server_info['endpoints'] = []
                        for ep in endpoints:
                            server_info['endpoints'].append({
                                'endpoint_url': ep.EndpointUrl,
                                'security_mode': str(ep.SecurityMode) if hasattr(ep, 'SecurityMode') else 'None',
                                'security_policy': ep.SecurityPolicyUri.split('#')[-1] if ep.SecurityPolicyUri else 'None',
                            })

                    except Exception as e:
                        logger.debug(f"Could not get endpoints for {url}: {e}")
                        server_info['endpoints'] = []

                discovered.append(server_info)

            logger.info(f"Discovered {len(discovered)} OPC UA server(s)")

        except asyncio.TimeoutError:
            logger.warning(f"Discovery timeout after {timeout}s for {discovery_url}")
        except Exception as e:
            logger.error(f"Discovery error: {e}")

        return discovered

    async def scan_network(self, ip_range: str = "192.168.1.0/24", port: int = 4840, timeout: float = 2.0) -> List[Dict[str, Any]]:
        """
        Scan network for OPC UA servers.

        Args:
            ip_range: IP range to scan (CIDR notation)
            port: OPC UA port to scan (default 4840)
            timeout: Connection timeout per host

        Returns:
            List of discovered servers
        """
        import ipaddress

        discovered = []

        try:
            network = ipaddress.ip_network(ip_range, strict=False)
            hosts = list(network.hosts())

            logger.info(f"Scanning {len(hosts)} hosts for OPC UA servers on port {port}...")

            # Scan hosts in parallel (batches of 100 for much faster scanning)
            # For a /24 network (256 IPs), this means 3 batches instead of 26
            batch_size = 100
            for i in range(0, len(hosts), batch_size):
                batch = hosts[i:i+batch_size]
                tasks = [self._check_host(str(host), port, timeout) for host in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if result and not isinstance(result, Exception):
                        discovered.extend(result)

            logger.info(f"Network scan complete: found {len(discovered)} server(s)")

        except Exception as e:
            logger.error(f"Network scan error: {e}")

        return discovered

    async def _check_host(self, host: str, port: int, timeout: float) -> List[Dict[str, Any]]:
        """Check if a host has an OPC UA server."""
        url = f"opc.tcp://{host}:{port}"

        try:
            # Try to get endpoints to verify OPC UA server
            client = Client(url=url, timeout=timeout)
            async with client:
                endpoints = await asyncio.wait_for(
                    client.get_endpoints(),
                    timeout=timeout
                )

                if endpoints:
                    # Server found - extract server info
                    first_ep = endpoints[0]
                    server_info = {
                        'application_uri': first_ep.Server.ApplicationUri if hasattr(first_ep, 'Server') else url,
                        'application_name': str(first_ep.Server.ApplicationName) if hasattr(first_ep, 'Server') and first_ep.Server.ApplicationName else f'OPC UA Server at {host}',
                        'application_type': 'Server',
                        'discovery_url': url,
                        'discovery_urls': [url],
                        'host': host,
                        'port': port,
                        'verified': True,
                        'endpoints': []
                    }

                    # Add endpoint details
                    for ep in endpoints[:5]:  # Limit to first 5 endpoints
                        server_info['endpoints'].append({
                            'endpoint_url': ep.EndpointUrl,
                            'security_mode': str(ep.SecurityMode) if hasattr(ep, 'SecurityMode') else 'None',
                            'security_policy': ep.SecurityPolicyUri.split('#')[-1] if ep.SecurityPolicyUri else 'None',
                        })

                    logger.info(f"✓ Found OPC UA server at {url}")
                    return [server_info]

        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            pass  # Host not available or not OPC UA
        except Exception as e:
            logger.debug(f"Error checking {url}: {e}")

        return []


# Global discovery instance
_discovery = None


def get_discovery() -> OPCUADiscovery:
    """Get global discovery instance."""
    global _discovery
    if _discovery is None:
        _discovery = OPCUADiscovery()
    return _discovery
