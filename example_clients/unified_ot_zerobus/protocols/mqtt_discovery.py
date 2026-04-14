"""
MQTT Broker Discovery

Provides network scanning capabilities to discover MQTT brokers on the local network.
Scans for both standard (1883, 8883) and non-standard ports.
"""

import asyncio
import logging
import socket
from typing import List, Dict, Any
import ipaddress

logger = logging.getLogger(__name__)


class MQTTDiscovery:
    """MQTT broker discovery service."""

    # Common MQTT ports to scan
    COMMON_PORTS = [1883, 8883, 1884, 8884]

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
        Scan network for MQTT brokers.

        Args:
            ip_range: IP range to scan (CIDR notation)
            ports: List of ports to scan (defaults to COMMON_PORTS)
            timeout: Connection timeout per host

        Returns:
            List of discovered MQTT brokers
        """
        if ports is None:
            ports = self.COMMON_PORTS

        discovered = []

        try:
            network = ipaddress.ip_network(ip_range, strict=False)
            hosts = list(network.hosts()) if network.num_addresses > 2 else [network.network_address]

            logger.info(f"Scanning {len(hosts)} hosts for MQTT brokers on ports {ports}...")

            # Scan hosts in parallel (batches of 100 for much faster scanning)
            # For a /24 network (256 IPs × 4 ports = 1024 tasks), this means ~11 batches instead of 52
            batch_size = 100
            for i in range(0, len(hosts), batch_size):
                batch = hosts[i:i+batch_size]
                tasks = []
                for host in batch:
                    for port in ports:
                        tasks.append(self._check_mqtt(str(host), port, timeout))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if result and not isinstance(result, Exception):
                        discovered.append(result)

            logger.info(f"MQTT scan complete: found {len(discovered)} broker(s)")

        except Exception as e:
            logger.error(f"MQTT network scan error: {e}")

        return discovered

    async def _check_mqtt(self, host: str, port: int, timeout: float) -> Dict[str, Any]:
        """Check if a host has an MQTT broker."""
        try:
            # Try to establish TCP connection
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )

            # Send MQTT CONNECT packet (MQTT 3.1.1)
            # Fixed header: 0x10 (CONNECT), Length: variable
            # Variable header: Protocol Name (MQTT), Protocol Level (4), Connect Flags, Keep Alive
            mqtt_connect = bytes([
                0x10,  # CONNECT packet type
                0x10,  # Remaining length (16 bytes)
                0x00, 0x04,  # Protocol name length
                0x4D, 0x51, 0x54, 0x54,  # "MQTT"
                0x04,  # Protocol level (MQTT 3.1.1)
                0x02,  # Connect flags (Clean Session)
                0x00, 0x3C,  # Keep alive (60 seconds)
                0x00, 0x04,  # Client ID length
                0x74, 0x65, 0x73, 0x74  # Client ID: "test"
            ])

            writer.write(mqtt_connect)
            await writer.drain()

            # Try to read CONNACK response
            response = await asyncio.wait_for(reader.read(4), timeout=timeout)

            writer.close()
            await writer.wait_closed()

            # Check if response is valid MQTT CONNACK (0x20)
            if len(response) >= 2 and response[0] == 0x20:
                broker_info = {
                    'protocol': 'mqtt',
                    'host': host,
                    'port': port,
                    'endpoint': f"{host}:{port}",
                    'verified': True,
                    'tls': port in [8883, 8884],
                    'accessible': True
                }

                # Parse CONNACK return code
                if len(response) >= 4:
                    return_code = response[3]
                    if return_code == 0x00:
                        broker_info['connection_accepted'] = True
                    elif return_code == 0x05:
                        broker_info['connection_accepted'] = False
                        broker_info['reason'] = 'Not authorized'
                    else:
                        broker_info['connection_accepted'] = False
                        broker_info['reason'] = f'Return code: {return_code}'

                logger.info(f"✓ Found MQTT broker at {host}:{port}")
                return broker_info

            # If port is listening but not MQTT, still report it
            elif len(response) > 0:
                logger.debug(f"Found responding service at {host}:{port} (not MQTT)")
                return {
                    'protocol': 'mqtt',
                    'host': host,
                    'port': port,
                    'endpoint': f"{host}:{port}",
                    'verified': False,
                    'accessible': True,
                    'warning': 'Port responding but not MQTT protocol'
                }

        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            pass  # Host not available or not MQTT
        except Exception as e:
            logger.debug(f"Error checking {host}:{port}: {e}")

        return None

    async def test_connection(
        self,
        host: str,
        port: int,
        username: str = None,
        password: str = None,
        client_id: str = "databricks-iot-test",
        timeout: float = 5.0
    ) -> Dict[str, Any]:
        """
        Test connection to a specific MQTT broker.

        Args:
            host: Broker hostname or IP
            port: MQTT port
            username: Optional username
            password: Optional password
            client_id: MQTT client ID
            timeout: Connection timeout

        Returns:
            Test results with connection status and broker info
        """
        result = {
            'success': False,
            'endpoint': f"{host}:{port}",
            'client_id': client_id
        }

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )

            # Build CONNECT packet with optional auth
            protocol_name = b'MQTT'
            protocol_level = 0x04  # MQTT 3.1.1

            # Connect flags
            connect_flags = 0x02  # Clean session
            if username:
                connect_flags |= 0x80  # Username flag
            if password:
                connect_flags |= 0x40  # Password flag

            # Payload
            payload = bytearray()

            # Client ID
            client_id_bytes = client_id.encode('utf-8')
            payload.extend(len(client_id_bytes).to_bytes(2, 'big'))
            payload.extend(client_id_bytes)

            # Username
            if username:
                username_bytes = username.encode('utf-8')
                payload.extend(len(username_bytes).to_bytes(2, 'big'))
                payload.extend(username_bytes)

            # Password
            if password:
                password_bytes = password.encode('utf-8')
                payload.extend(len(password_bytes).to_bytes(2, 'big'))
                payload.extend(password_bytes)

            # Variable header
            variable_header = bytearray()
            variable_header.extend(len(protocol_name).to_bytes(2, 'big'))
            variable_header.extend(protocol_name)
            variable_header.append(protocol_level)
            variable_header.append(connect_flags)
            variable_header.extend((60).to_bytes(2, 'big'))  # Keep alive

            # Fixed header
            remaining_length = len(variable_header) + len(payload)
            fixed_header = bytes([0x10, remaining_length])

            # Complete packet
            mqtt_connect = fixed_header + variable_header + payload

            writer.write(mqtt_connect)
            await writer.drain()

            # Read CONNACK
            response = await asyncio.wait_for(reader.read(4), timeout=timeout)

            writer.close()
            await writer.wait_closed()

            if len(response) >= 4 and response[0] == 0x20:
                return_code = response[3]

                if return_code == 0x00:
                    result['success'] = True
                    result['message'] = f"Successfully connected to MQTT broker at {host}:{port}"
                    result['session_present'] = bool(response[2] & 0x01)
                elif return_code == 0x05:
                    result['error'] = "Not authorized (check username/password)"
                elif return_code == 0x04:
                    result['error'] = "Bad username or password"
                elif return_code == 0x03:
                    result['error'] = "Server unavailable"
                else:
                    result['error'] = f"Connection refused (code: {return_code})"
            else:
                result['error'] = "Invalid CONNACK response"

        except asyncio.TimeoutError:
            result['error'] = f"Connection timeout to {host}:{port}"
        except ConnectionRefusedError:
            result['error'] = f"Connection refused by {host}:{port}"
        except Exception as e:
            result['error'] = f"Connection failed: {str(e)}"

        return result


# Global discovery instance
_discovery = None


def get_discovery() -> MQTTDiscovery:
    """Get global discovery instance."""
    global _discovery
    if _discovery is None:
        _discovery = MQTTDiscovery()
    return _discovery
