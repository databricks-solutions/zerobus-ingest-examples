"""
Modbus Device Discovery

Provides network scanning capabilities to discover Modbus TCP devices on the local network.
Scans for both standard (502) and non-standard ports.
"""

import asyncio
import logging
import socket
from typing import List, Dict, Any
import ipaddress

logger = logging.getLogger(__name__)


class ModbusDiscovery:
    """Modbus device discovery service."""

    # Common Modbus ports to scan
    COMMON_PORTS = [502, 5020, 5021, 8502]

    def __init__(self):
        """Initialize discovery service."""
        pass

    async def scan_network(
        self,
        ip_range: str = "192.168.1.0/24",
        ports: List[int] = None,
        timeout: float = 1.0
    ) -> List[Dict[str, Any]]:
        """
        Scan network for Modbus TCP devices.

        Args:
            ip_range: IP range to scan (CIDR notation)
            ports: List of ports to scan (defaults to COMMON_PORTS)
            timeout: Connection timeout per host

        Returns:
            List of discovered Modbus devices
        """
        if ports is None:
            ports = self.COMMON_PORTS

        discovered = []

        try:
            network = ipaddress.ip_network(ip_range, strict=False)
            hosts = list(network.hosts()) if network.num_addresses > 2 else [network.network_address]

            logger.info(f"Scanning {len(hosts)} hosts for Modbus devices on ports {ports}...")

            # Scan hosts in parallel (batches of 100 for much faster scanning)
            # For a /24 network (256 IPs × 4 ports = 1024 tasks), this means ~11 batches instead of 52
            batch_size = 100
            for i in range(0, len(hosts), batch_size):
                batch = hosts[i:i+batch_size]
                tasks = []
                for host in batch:
                    for port in ports:
                        tasks.append(self._check_modbus(str(host), port, timeout))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if result and not isinstance(result, Exception):
                        discovered.append(result)

            logger.info(f"Modbus scan complete: found {len(discovered)} device(s)")

        except Exception as e:
            logger.error(f"Modbus network scan error: {e}")

        return discovered

    async def _check_modbus(self, host: str, port: int, timeout: float) -> Dict[str, Any]:
        """Check if a host has a Modbus TCP server."""
        try:
            # Try to establish TCP connection
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )

            # Send Modbus Read Holding Registers request (Function Code 0x03)
            # Transaction ID: 0x0001, Protocol ID: 0x0000, Length: 6, Unit ID: 1
            # Function: 0x03, Start Address: 0x0000, Quantity: 1
            modbus_request = bytes([
                0x00, 0x01,  # Transaction ID
                0x00, 0x00,  # Protocol ID
                0x00, 0x06,  # Length
                0x01,        # Unit ID
                0x03,        # Function Code (Read Holding Registers)
                0x00, 0x00,  # Start Address
                0x00, 0x01   # Quantity
            ])

            writer.write(modbus_request)
            await writer.drain()

            # Try to read response
            response = await asyncio.wait_for(reader.read(1024), timeout=timeout)

            writer.close()
            await writer.wait_closed()

            # If we got a response, it's likely a Modbus device
            if len(response) >= 9:  # Minimum valid Modbus response
                device_info = {
                    'protocol': 'modbus',
                    'type': 'TCP',
                    'host': host,
                    'port': port,
                    'endpoint': f"{host}:{port}",
                    'response_length': len(response),
                    'accessible': True
                }

                # Check if response is valid Modbus (starts with our transaction ID)
                if response[0:2] == bytes([0x00, 0x01]):
                    device_info['verified'] = True
                    logger.info(f"✓ Found Modbus TCP device at {host}:{port}")
                else:
                    device_info['verified'] = False
                    logger.debug(f"Found responding device at {host}:{port} (not verified as Modbus)")

                return device_info

        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            pass  # Host not available or not Modbus
        except Exception as e:
            logger.debug(f"Error checking {host}:{port}: {e}")

        return None

    async def test_connection(self, host: str, port: int, unit_id: int = 1, timeout: float = 3.0) -> Dict[str, Any]:
        """
        Test connection to a specific Modbus TCP device.

        Args:
            host: Device hostname or IP
            port: Modbus TCP port
            unit_id: Modbus unit ID
            timeout: Connection timeout

        Returns:
            Test results with connection status and device info
        """
        result = {
            'success': False,
            'endpoint': f"{host}:{port}",
            'unit_id': unit_id
        }

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )

            # Read Holding Registers (Function 0x03)
            modbus_request = bytes([
                0x00, 0x01,  # Transaction ID
                0x00, 0x00,  # Protocol ID
                0x00, 0x06,  # Length
                unit_id,     # Unit ID
                0x03,        # Function Code
                0x00, 0x00,  # Start Address
                0x00, 0x0A   # Quantity (10 registers)
            ])

            writer.write(modbus_request)
            await writer.drain()

            response = await asyncio.wait_for(reader.read(1024), timeout=timeout)

            writer.close()
            await writer.wait_closed()

            if len(response) >= 9:
                result['success'] = True
                result['response_length'] = len(response)
                result['message'] = f"Successfully connected to Modbus device at {host}:{port}"

                # Parse response if valid
                if response[0:2] == bytes([0x00, 0x01]) and response[7] == 0x03:
                    byte_count = response[8]
                    result['registers_read'] = byte_count // 2
                else:
                    result['warning'] = "Device responded but response format unexpected"

        except asyncio.TimeoutError:
            result['error'] = f"Connection timeout to {host}:{port}"
        except ConnectionRefusedError:
            result['error'] = f"Connection refused by {host}:{port}"
        except Exception as e:
            result['error'] = f"Connection failed: {str(e)}"

        return result


# Global discovery instance
_discovery = None


def get_discovery() -> ModbusDiscovery:
    """Get global discovery instance."""
    global _discovery
    if _discovery is None:
        _discovery = ModbusDiscovery()
    return _discovery
