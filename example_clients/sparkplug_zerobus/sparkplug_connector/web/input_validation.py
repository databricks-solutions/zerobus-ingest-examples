"""
Input Validation and Sanitization for Web UI.

Protects against injection attacks and malicious input:
- SQL injection (though we don't use SQL directly)
- Command injection
- Path traversal
- XSS via data inputs
- Resource name validation
"""

import re
import logging
from typing import Any, Dict, Optional, List
from pathlib import Path

from sparkplug_connector.core.targets import parse_target_table
from sparkplug_connector.sparkplug.edge_node_schemas import nbirth_procedures

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when input validation fails."""
    pass


# Validation patterns
SAFE_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_\-\.]+$')
SAFE_IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*$')
IP_ADDRESS_PATTERN = re.compile(r'^(\d{1,3}\.){3}\d{1,3}$')
HOSTNAME_PATTERN = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$')
URL_PATTERN = re.compile(r'^(https?|mqtts?)://[a-zA-Z0-9\-\._~:/?#\[\]@!$&\'()*+,;=]+$')

# Dangerous characters that might indicate injection attempts
DANGEROUS_CHARS = ['<', '>', '"', "'", ';', '|', '&', '`', '$', '(', ')', '{', '}', '[', ']', '\n', '\r', '\x00']

# Maximum lengths to prevent buffer overflow attacks
MAX_NAME_LENGTH = 255
MAX_ENDPOINT_LENGTH = 1024
MAX_CONFIG_VALUE_LENGTH = 10240  # 10KB


def validate_source_name(name: str) -> str:
    """
    Validate source name for safety.

    Args:
        name: Source name to validate

    Returns:
        Validated name

    Raises:
        ValidationError: If name is invalid
    """
    if not name:
        raise ValidationError("Source name cannot be empty")

    if len(name) > MAX_NAME_LENGTH:
        raise ValidationError(f"Source name too long (max {MAX_NAME_LENGTH} characters)")

    # Check for path traversal attempts
    if '..' in name or '/' in name or '\\' in name:
        raise ValidationError("Source name cannot contain path traversal sequences")

    if not SAFE_NAME_PATTERN.match(name):
        raise ValidationError(
            "Source name can only contain letters, numbers, hyphens, underscores, and periods"
        )

    logger.debug(f"Validated source name: {name}")
    return name


def validate_protocol(protocol: str) -> str:
    """
    Validate protocol type.

    Args:
        protocol: Protocol name

    Returns:
        Validated protocol

    Raises:
        ValidationError: If protocol is invalid
    """
    valid_protocols = ['sparkplug']

    protocol = protocol.lower().strip()

    if protocol not in valid_protocols:
        raise ValidationError(f"Invalid protocol. Must be one of: {', '.join(valid_protocols)}")

    return protocol


def validate_endpoint(endpoint: str) -> str:
    """
    Validate endpoint URL.

    Args:
        endpoint: Endpoint URL

    Returns:
        Validated endpoint

    Raises:
        ValidationError: If endpoint is invalid
    """
    if not endpoint:
        raise ValidationError("Endpoint cannot be empty")

    if len(endpoint) > MAX_ENDPOINT_LENGTH:
        raise ValidationError(f"Endpoint too long (max {MAX_ENDPOINT_LENGTH} characters)")

    # Check for dangerous characters that might indicate command injection
    for char in DANGEROUS_CHARS:
        if char in endpoint:
            raise ValidationError(f"Endpoint contains dangerous character: {char}")

    # Validate URL format
    if not URL_PATTERN.match(endpoint):
        raise ValidationError("Invalid endpoint URL format")

    # Reject path traversal sequences anywhere in the URL
    # (e.g. http://host/../../../etc/passwd)
    if ".." in endpoint:
        raise ValidationError("Endpoint contains path traversal sequence")

    # Check for suspicious patterns
    suspicious_patterns = [
        r'[;&|`$]',  # Command injection
        r'\$\{',      # Variable expansion
        r'\.\./',     # Path traversal
        r'\.\.\\',    # Path traversal (windows-style)
        r'file://',   # Local file access
        r'javascript:', # XSS
    ]

    for pattern in suspicious_patterns:
        if re.search(pattern, endpoint, re.IGNORECASE):
            raise ValidationError(f"Endpoint contains suspicious pattern: {pattern}")

    logger.debug(f"Validated endpoint: {endpoint}")
    return endpoint


def validate_host(host: str) -> str:
    """
    Validate hostname or IP address.

    Args:
        host: Hostname or IP

    Returns:
        Validated host

    Raises:
        ValidationError: If host is invalid
    """
    if not host:
        raise ValidationError("Host cannot be empty")

    if len(host) > 253:  # DNS hostname max length
        raise ValidationError("Host too long")

    # Check if it's an IP address
    if IP_ADDRESS_PATTERN.match(host):
        # Validate IP octets
        octets = [int(o) for o in host.split('.')]
        if any(o > 255 for o in octets):
            raise ValidationError("Invalid IP address")
        return host

    # Check if it's a hostname
    if not HOSTNAME_PATTERN.match(host):
        raise ValidationError("Invalid hostname format")

    return host


def validate_port(port: Any) -> int:
    """
    Validate port number.

    Args:
        port: Port number (int or str)

    Returns:
        Validated port as int

    Raises:
        ValidationError: If port is invalid
    """
    try:
        port_int = int(port)
    except (ValueError, TypeError):
        raise ValidationError("Port must be a number")

    if port_int < 1 or port_int > 65535:
        raise ValidationError("Port must be between 1 and 65535")

    return port_int


def validate_config_value(value: Any, max_length: int = MAX_CONFIG_VALUE_LENGTH) -> Any:
    """
    Validate generic configuration value.

    Args:
        value: Configuration value
        max_length: Maximum string length

    Returns:
        Validated value

    Raises:
        ValidationError: If value is invalid
    """
    if value is None:
        return value

    # Validate strings
    if isinstance(value, str):
        if len(value) > max_length:
            raise ValidationError(f"Configuration value too long (max {max_length} characters)")

        # Check for null bytes (can cause issues in C libraries)
        if '\x00' in value:
            raise ValidationError("Configuration value contains null byte")

    # Validate numbers
    elif isinstance(value, (int, float)):
        # Check for reasonable ranges
        if abs(value) > 1e15:  # Arbitrary large number
            raise ValidationError("Configuration value out of reasonable range")

    # Validate booleans (no validation needed, already typed)
    elif isinstance(value, bool):
        pass

    # Validate lists
    elif isinstance(value, list):
        if len(value) > 1000:  # Arbitrary limit
            raise ValidationError("Configuration list too long")
        for item in value:
            validate_config_value(item, max_length)

    # Validate dicts
    elif isinstance(value, dict):
        if len(value) > 1000:  # Arbitrary limit
            raise ValidationError("Configuration dict too large")
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValidationError("Configuration dict keys must be strings")
            validate_config_value(k, 255)
            validate_config_value(v, max_length)

    else:
        raise ValidationError(f"Unsupported configuration value type: {type(value)}")

    return value


def validate_source_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate complete source configuration.

    Args:
        config: Source configuration dict

    Returns:
        Validated config

    Raises:
        ValidationError: If config is invalid
    """
    required_fields = ['name', 'protocol', 'endpoint']

    # Check required fields
    for field in required_fields:
        if field not in config:
            raise ValidationError(f"Missing required field: {field}")

    # Validate individual fields
    validated = {}
    validated['name'] = validate_source_name(config['name'])
    validated['protocol'] = validate_protocol(config['protocol'])
    validated['endpoint'] = validate_endpoint(config['endpoint'])

    # Validate optional fields
    # (sparkplug/README.md); the username, password, topics and TLS settings stay in the config file
    optional_fields = ['enabled', 'target_table', 'client_id', 'primary_host_id', 'state_format',
                       'offline_after_seconds', 'online_after_seconds', 'edge_node_schemas',
                       'sequence_handling', 'sequence_reordering_timeout', 'block_auto_rebirth']

    for field in optional_fields:
        if field in config:
            validated[field] = validate_config_value(config[field])

    # The source's own Zerobus table (core/targets.py); empty: the default table
    if str(validated.get('target_table') or '').strip():
        try:
            validated['target_table'] = ".".join(parse_target_table(validated['target_table']))
        except ValueError as e:
            raise ValidationError(str(e))

    # Sparkplug B NBIRTH procedures: their names go into SQL text
    ens = validated.get('edge_node_schemas')
    if isinstance(ens, dict):
        try:
            procedures = nbirth_procedures(ens)
        except ValueError as e:
            raise ValidationError(str(e))
        if ens.get('enabled') is True and not procedures:
            raise ValidationError("NBIRTH procedure: at least one catalog.schema.procedure when the call is on")
        if ens.get('enabled') is True and not str(ens.get('warehouse_id') or '').strip():
            raise ValidationError("SQL warehouse ID: needed when the NBIRTH call is on")

    # Sparkplug B primary host: refused here, not only when the source starts with it
    state_format = str(validated.get('state_format') or '').strip().lower()
    if state_format and state_format not in ('v3', 'v2'):
        raise ValidationError(f"STATE format: v3 or v2, not {validated['state_format']!r}")
    for field in ('offline_after_seconds', 'online_after_seconds'):
        value = validated.get(field)
        if value is None or value == '':
            continue
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            seconds = -1.0
        if not seconds >= 0:
            raise ValidationError(f"{field}: a number of seconds, 0 or more (not {value!r})")

    # Rows and rebirth requests (sparkplug/sequencer.py)
    handling = str(validated.get('sequence_handling') or '').strip().lower()
    if handling and handling not in ('strict', 'keep_all'):
        raise ValidationError(f"Sequence handling: strict or keep_all, not {validated['sequence_handling']!r}")
    timeout = validated.get('sequence_reordering_timeout')
    if timeout is not None and timeout != '':
        try:
            ms = int(str(timeout).strip())
        except ValueError:
            ms = 0
        if ms <= 0:
            raise ValidationError(f"sequence_reordering_timeout: a number of milliseconds, more than 0 (not {timeout!r})")
        validated['sequence_reordering_timeout'] = ms
    if 'block_auto_rebirth' in validated and not isinstance(validated['block_auto_rebirth'], bool):
        raise ValidationError(f"block_auto_rebirth: true or false, not {validated['block_auto_rebirth']!r}")
    if validated.get('block_auto_rebirth') is True and handling in ('', 'strict'):
        raise ValidationError("Block auto rebirth needs Rows: keep_all (strict drops an edge node's messages after a "
                              "gap until its rebirth)")

    # Log validation success
    logger.info(
        f"Validated source config: {validated['name']} ({validated['protocol']})",
        extra={
            'event_type': 'input_validation',
            'validation_target': 'source_config',
            'result': 'success'
        }
    )

    return validated


def sanitize_log_message(message: str) -> str:
    """
    Sanitize log message to prevent log injection.

    Args:
        message: Log message

    Returns:
        Sanitized message
    """
    if not message:
        return ""

    # Remove newlines and carriage returns (prevent log injection)
    message = message.replace('\n', ' ').replace('\r', ' ')

    # Remove null bytes
    message = message.replace('\x00', '')

    # Truncate if too long
    if len(message) > 1000:
        message = message[:997] + '...'

    return message


def validate_file_path(path: str, allowed_dirs: Optional[List[str]] = None) -> Path:
    """
    Validate file path to prevent directory traversal.

    Args:
        path: File path to validate
        allowed_dirs: Optional list of allowed directory prefixes

    Returns:
        Validated Path object

    Raises:
        ValidationError: If path is invalid or unsafe
    """
    if not path:
        raise ValidationError("File path cannot be empty")

    try:
        path_obj = Path(path).resolve()
    except Exception as e:
        raise ValidationError(f"Invalid file path: {e}")

    # Check for path traversal
    if '..' in path:
        raise ValidationError("Path contains directory traversal sequence")

    # Check against allowed directories if specified
    if allowed_dirs:
        if not any(str(path_obj).startswith(str(Path(d).resolve())) for d in allowed_dirs):
            raise ValidationError("Path not in allowed directories")

    return path_obj
