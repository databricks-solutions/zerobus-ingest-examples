"""
Structured Logging for SIEM Integration.

Provides JSON-formatted logging with security event categories for
integration with SIEM systems (Splunk, ELK, Azure Sentinel, etc.).

NIS2 Compliance: Article 21.2(b) - Incident Handling
"""

import logging
import json
import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from contextvars import ContextVar

# Context variable for correlation ID (thread-safe for async)
correlation_id_var: ContextVar[Optional[str]] = ContextVar('correlation_id', default=None)


class SecurityEventCategory:
    """Security event categories for SIEM classification."""

    # Authentication events
    AUTH_SUCCESS = "auth.success"
    AUTH_FAILURE = "auth.failure"
    AUTH_LOGOUT = "auth.logout"
    AUTH_SESSION_EXPIRED = "auth.session_expired"
    AUTH_MFA_SUCCESS = "auth.mfa.success"
    AUTH_MFA_FAILURE = "auth.mfa.failure"

    # Authorization events
    AUTHZ_GRANTED = "authz.granted"
    AUTHZ_DENIED = "authz.denied"
    AUTHZ_PRIVILEGE_ESCALATION = "authz.privilege_escalation"

    # Input validation events
    VALIDATION_FAILED = "validation.failed"
    INJECTION_ATTEMPT = "security.injection_attempt"
    PATH_TRAVERSAL_ATTEMPT = "security.path_traversal"
    XSS_ATTEMPT = "security.xss_attempt"

    # Configuration changes
    CONFIG_CHANGED = "config.changed"
    CONFIG_DELETED = "config.deleted"
    SOURCE_ADDED = "source.added"
    SOURCE_MODIFIED = "source.modified"
    SOURCE_DELETED = "source.deleted"

    # System events
    SYSTEM_START = "system.start"
    SYSTEM_STOP = "system.stop"
    SYSTEM_ERROR = "system.error"

    # Data access
    DATA_ACCESS = "data.access"
    DATA_EXPORT = "data.export"

    # Security scan events
    SECURITY_SCAN_START = "security.scan.start"
    SECURITY_SCAN_COMPLETE = "security.scan.complete"
    VULNERABILITY_DETECTED = "security.vulnerability.detected"


class StructuredLogger:
    """
    Structured logger that outputs JSON-formatted logs for SIEM integration.

    Includes:
    - Timestamp (ISO 8601)
    - Correlation ID (for request tracking)
    - Event category
    - Severity level
    - User information
    - Source IP
    - Custom fields
    """

    def __init__(self, logger: logging.Logger):
        """
        Initialize structured logger.

        Args:
            logger: Base Python logger instance
        """
        self.logger = logger

    def _format_event(
        self,
        level: str,
        message: str,
        event_category: str,
        user: Optional[str] = None,
        source_ip: Optional[str] = None,
        **extra_fields
    ) -> Dict[str, Any]:
        """
        Format log event as structured data.

        Args:
            level: Log level (INFO, WARNING, ERROR, etc.)
            message: Human-readable message
            event_category: Security event category
            user: User email/ID
            source_ip: Source IP address
            **extra_fields: Additional custom fields

        Returns:
            Structured log event dictionary
        """
        correlation_id = correlation_id_var.get()

        event = {
            # Core fields
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': level,
            'message': message,

            # Security fields
            'event_category': event_category,
            'correlation_id': correlation_id or 'none',

            # Identity fields
            'user': user or 'anonymous',
            'source_ip': source_ip or 'unknown',

            # Application context
            'application': 'unified-ot-connector',
            'component': extra_fields.pop('component', 'unknown'),
            'action': extra_fields.pop('action', 'unknown'),
            'result': extra_fields.pop('result', 'unknown'),
        }

        # Add any extra fields
        if extra_fields:
            event['details'] = extra_fields

        return event

    def log_security_event(
        self,
        level: str,
        message: str,
        event_category: str,
        user: Optional[str] = None,
        source_ip: Optional[str] = None,
        **extra_fields: Any
    ) -> None:
        """
        Log a security event in structured format.

        Args:
            level: Log level (INFO, WARNING, ERROR, CRITICAL)
            message: Human-readable message
            event_category: Security event category
            user: User email/ID
            source_ip: Source IP address
            **extra_fields: Additional custom fields
        """
        event = self._format_event(
            level, message, event_category, user, source_ip, **extra_fields
        )

        # Convert to JSON string
        json_log = json.dumps(event, default=str)

        # Log at appropriate level
        log_level = getattr(logging, level.upper(), logging.INFO)
        self.logger.log(log_level, json_log)

    def auth_success(self, user: str, source_ip: str, mfa: bool = False, **extra: Any) -> None:
        """Log successful authentication."""
        self.log_security_event(
            'INFO',
            f"User {user} authenticated successfully",
            SecurityEventCategory.AUTH_MFA_SUCCESS if mfa else SecurityEventCategory.AUTH_SUCCESS,
            user=user,
            source_ip=source_ip,
            mfa=mfa,
            **extra
        )

    def auth_failure(self, user: str, source_ip: str, reason: str, **extra: Any) -> None:
        """Log failed authentication."""
        self.log_security_event(
            'WARNING',
            f"Authentication failed for user {user}: {reason}",
            SecurityEventCategory.AUTH_FAILURE,
            user=user,
            source_ip=source_ip,
            reason=reason,
            **extra
        )

    def authz_denied(self, user: str, action: str, resource: str, required_permission: str, **extra: Any) -> None:
        """Log authorization denial."""
        self.log_security_event(
            'WARNING',
            f"Access denied: {user} attempted {action} on {resource}",
            SecurityEventCategory.AUTHZ_DENIED,
            user=user,
            action=action,
            resource=resource,
            required_permission=required_permission,
            **extra
        )

    def validation_failed(self, user: str, action: str, reason: str, input_data: str, **extra: Any) -> None:
        """Log input validation failure (potential attack)."""
        self.log_security_event(
            'WARNING',
            f"Input validation failed: {reason}",
            SecurityEventCategory.VALIDATION_FAILED,
            user=user,
            action=action,
            reason=reason,
            input_data=input_data[:100],  # Limit to 100 chars
            **extra
        )

    def injection_attempt(self, user: str, source_ip: str, payload: str, attack_type: str, **extra: Any) -> None:
        """Log injection attack attempt (critical security event)."""
        self.log_security_event(
            'CRITICAL',
            f"Injection attack detected: {attack_type}",
            SecurityEventCategory.INJECTION_ATTEMPT,
            user=user,
            source_ip=source_ip,
            payload=payload[:100],  # Limit to 100 chars
            attack_type=attack_type,
            **extra
        )

    def config_changed(self, user: str, config_type: str, changes: Dict[str, Any], **extra):
        """Log configuration change."""
        self.log_security_event(
            'INFO',
            f"Configuration changed: {config_type}",
            SecurityEventCategory.CONFIG_CHANGED,
            user=user,
            config_type=config_type,
            changes=changes,
            **extra
        )

    def source_added(self, user: str, source_name: str, protocol: str, endpoint: str, **extra):
        """Log source addition."""
        self.log_security_event(
            'INFO',
            f"Source added: {source_name}",
            SecurityEventCategory.SOURCE_ADDED,
            user=user,
            source_name=source_name,
            protocol=protocol,
            endpoint=endpoint,
            **extra
        )

    def source_deleted(self, user: str, source_name: str, **extra):
        """Log source deletion."""
        self.log_security_event(
            'WARNING',
            f"Source deleted: {source_name}",
            SecurityEventCategory.SOURCE_DELETED,
            user=user,
            source_name=source_name,
            **extra
        )

    def system_start(self, version: str, **extra):
        """Log system start."""
        self.log_security_event(
            'INFO',
            "System started",
            SecurityEventCategory.SYSTEM_START,
            version=version,
            **extra
        )

    def system_error(self, error: str, component: str, **extra):
        """Log system error."""
        self.log_security_event(
            'ERROR',
            f"System error in {component}: {error}",
            SecurityEventCategory.SYSTEM_ERROR,
            component=component,
            error=error,
            **extra
        )


def get_correlation_id() -> Optional[str]:
    """
    Get current correlation ID from context.

    Returns:
        Correlation ID or None
    """
    return correlation_id_var.get()


def set_correlation_id(correlation_id: str):
    """
    Set correlation ID for current context.

    Args:
        correlation_id: Correlation ID to set
    """
    correlation_id_var.set(correlation_id)


def generate_correlation_id() -> str:
    """
    Generate a new correlation ID.

    Returns:
        UUID-based correlation ID
    """
    return str(uuid.uuid4())


def clear_correlation_id():
    """Clear correlation ID from current context."""
    correlation_id_var.set(None)


def configure_structured_logging(log_file: str = None, log_level: str = 'INFO'):
    """
    Configure structured logging for the application.

    Args:
        log_file: Optional log file path for JSON logs
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Create JSON formatter
    class JsonFormatter(logging.Formatter):
        """Formatter that outputs JSON for all log records."""

        def format(self, record):
            # If message is already JSON, use it as-is
            try:
                json.loads(record.getMessage())
                return record.getMessage()
            except (json.JSONDecodeError, ValueError):
                # Not JSON, wrap in basic structure
                log_obj = {
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'level': record.levelname,
                    'message': record.getMessage(),
                    'logger': record.name,
                    'correlation_id': correlation_id_var.get() or 'none',
                }
                return json.dumps(log_obj, default=str)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Console handler with JSON formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(JsonFormatter())
    root_logger.addHandler(console_handler)

    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(JsonFormatter())
        root_logger.addHandler(file_handler)


# Global structured logger instance (lazy initialization)
_structured_logger = None


def get_structured_logger() -> StructuredLogger:
    """
    Get global structured logger instance.

    Returns:
        StructuredLogger instance
    """
    global _structured_logger
    if _structured_logger is None:
        logger = logging.getLogger('unified_connector.security')
        _structured_logger = StructuredLogger(logger)
    return _structured_logger
