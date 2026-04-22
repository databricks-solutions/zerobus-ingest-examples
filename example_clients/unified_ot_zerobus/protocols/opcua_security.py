"""OPC UA Security Configuration for Enterprise Connector.

Implements OPC UA 10101 compliant security for production deployments:
- Basic256Sha256 encryption (Sign & SignAndEncrypt)
- Enterprise certificate-based authentication
- Certificate validation and trust management
- Username/password authentication
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from asyncua import ua

logger = logging.getLogger(__name__)


@dataclass
class OPCUASecurityConfig:
    """OPC UA security configuration for connector (client-side)."""

    # Security mode
    enabled: bool = False
    security_policy: str = "Basic256Sha256"  # Basic256Sha256, NoSecurity
    security_mode: str = "SignAndEncrypt"  # Sign, SignAndEncrypt

    # Client certificate paths (for mutual TLS)
    client_cert_path: Optional[str] = None
    client_key_path: Optional[str] = None

    # Server certificate validation
    server_cert_path: Optional[str] = None  # Specific server cert to trust
    trust_all_certificates: bool = False  # For development only, NEVER in production

    # Username/password authentication (optional, works with or without certificates)
    username: Optional[str] = None
    password: Optional[str] = None

    # Connection settings
    timeout_s: float = 5.0
    session_timeout_ms: int = 60000  # 60 seconds


class OPCUASecurityManager:
    """Manages OPC UA client security configuration for enterprise deployments."""

    def __init__(self, config: OPCUASecurityConfig):
        """Initialize security manager.

        Args:
            config: Security configuration
        """
        self.config = config

    def get_security_string(self) -> str:
        """Get security policy string for asyncua Client.

        Returns:
            Security policy string (e.g., "Basic256Sha256,SignAndEncrypt,certificate.pem,key.pem")
        """
        if not self.config.enabled:
            return "None"

        # Build security string
        parts = [self.config.security_policy, self.config.security_mode]

        if self.config.client_cert_path:
            parts.append(str(Path(self.config.client_cert_path).resolve()))

        if self.config.client_key_path:
            parts.append(str(Path(self.config.client_key_path).resolve()))

        return ",".join(parts)

    def get_certificate_path(self) -> Optional[str]:
        """Get client certificate path.

        Returns:
            Path to certificate file or None
        """
        if not self.config.enabled or not self.config.client_cert_path:
            return None

        cert_path = Path(self.config.client_cert_path)
        if not cert_path.exists():
            logger.error(f"Client certificate not found: {cert_path}")
            return None

        logger.info(f"Using client certificate: {cert_path}")
        return str(cert_path)

    def get_private_key_path(self) -> Optional[str]:
        """Get client private key path.

        Returns:
            Path to private key file or None
        """
        if not self.config.enabled or not self.config.client_key_path:
            return None

        key_path = Path(self.config.client_key_path)
        if not key_path.exists():
            logger.error(f"Client private key not found: {key_path}")
            return None

        logger.info(f"Using client private key: {key_path}")
        return str(key_path)

    def get_server_certificate_path(self) -> Optional[str]:
        """Get server certificate path for validation.

        Returns:
            Path to server certificate file or None
        """
        if not self.config.server_cert_path:
            return None

        cert_path = Path(self.config.server_cert_path)
        if not cert_path.exists():
            logger.warning(f"Server certificate not found: {cert_path}")
            return None

        logger.info(f"Trusting server certificate: {cert_path}")
        return str(cert_path)

    def validate_configuration(self) -> bool:
        """Validate security configuration for production readiness.

        Returns:
            True if configuration is valid, False otherwise
        """
        if not self.config.enabled:
            logger.info("Security disabled - using NoSecurity mode")
            return True

        # Check trust_all_certificates flag
        if self.config.trust_all_certificates:
            logger.warning(
                "⚠️  SECURITY WARNING: trust_all_certificates=True "
                "(acceptable for development, NEVER use in production)"
            )

        # Check certificate files exist
        if self.config.client_cert_path:
            if not Path(self.config.client_cert_path).exists():
                logger.error(f"Client certificate not found: {self.config.client_cert_path}")
                return False
        else:
            logger.warning("No client certificate provided (anonymous connection)")

        if self.config.client_key_path:
            if not Path(self.config.client_key_path).exists():
                logger.error(f"Client private key not found: {self.config.client_key_path}")
                return False
        else:
            logger.warning("No client private key provided (anonymous connection)")

        # Check username/password if provided
        if self.config.username and not self.config.password:
            logger.error("Username provided but password is missing")
            return False

        if self.config.password and not self.config.username:
            logger.error("Password provided but username is missing")
            return False

        # Validate security policy
        valid_policies = ["Basic256Sha256", "NoSecurity"]
        if self.config.security_policy not in valid_policies:
            logger.error(
                f"Invalid security policy: {self.config.security_policy}. "
                f"Valid options: {valid_policies}"
            )
            return False

        # Validate security mode
        valid_modes = ["Sign", "SignAndEncrypt"]
        if self.config.security_mode not in valid_modes:
            logger.error(
                f"Invalid security mode: {self.config.security_mode}. "
                f"Valid options: {valid_modes}"
            )
            return False

        logger.info("✓ Security configuration is valid")
        return True

    def log_security_status(self):
        """Log security configuration status for troubleshooting."""
        if not self.config.enabled:
            logger.info("OPC UA Security: DISABLED (NoSecurity mode)")
            return

        logger.info("OPC UA Security Configuration:")
        logger.info(f"  - Policy: {self.config.security_policy}")
        logger.info(f"  - Mode: {self.config.security_mode}")
        logger.info(f"  - Client Certificate: {self.config.client_cert_path or 'None'}")
        logger.info(f"  - Client Key: {self.config.client_key_path or 'None'}")
        logger.info(f"  - Server Certificate: {self.config.server_cert_path or 'None'}")
        logger.info(f"  - Trust All Certs: {self.config.trust_all_certificates}")
        logger.info(f"  - Username: {self.config.username or 'None (anonymous)'}")
        logger.info(f"  - Timeout: {self.config.timeout_s}s")


def create_security_config_from_dict(config_dict: dict) -> OPCUASecurityConfig:
    """Create security configuration from dictionary.

    Args:
        config_dict: Configuration dictionary (from YAML/JSON)

    Returns:
        OPCUASecurityConfig instance
    """
    return OPCUASecurityConfig(
        enabled=config_dict.get("enabled", False),
        security_policy=config_dict.get("security_policy", "Basic256Sha256"),
        security_mode=config_dict.get("security_mode", "SignAndEncrypt"),
        client_cert_path=config_dict.get("client_cert_path"),
        client_key_path=config_dict.get("client_key_path"),
        server_cert_path=config_dict.get("server_cert_path"),
        trust_all_certificates=config_dict.get("trust_all_certificates", False),
        username=config_dict.get("username"),
        password=config_dict.get("password"),
        timeout_s=config_dict.get("timeout", 5.0),
        session_timeout_ms=config_dict.get("session_timeout_ms", 60000),
    )
