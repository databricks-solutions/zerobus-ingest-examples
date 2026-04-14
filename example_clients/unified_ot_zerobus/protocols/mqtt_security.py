"""
MQTT TLS Security Configuration for NIS2 Compliance.

Provides:
- TLS/SSL encryption for MQTT connections
- Certificate-based authentication (mutual TLS)
- Username/password authentication
- CA certificate validation
- Secure cipher configuration

NIS2 Compliance: Article 21.2(h) - Encryption (data in transit)
"""

import ssl
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class MQTTSecurityConfig:
    """MQTT TLS/SSL security configuration."""

    # TLS/SSL settings
    enabled: bool = False
    use_tls: bool = False  # Derived from mqtt:// vs mqtts:// scheme
    tls_version: str = "TLSv1_2"  # TLSv1_2, TLSv1_3, TLS (auto)

    # Certificate paths
    ca_cert_path: Optional[str] = None  # CA certificate for server validation
    client_cert_path: Optional[str] = None  # Client certificate (mutual TLS)
    client_key_path: Optional[str] = None  # Client private key

    # Certificate validation
    cert_required: bool = True  # Require server certificate validation
    check_hostname: bool = True  # Verify server hostname matches certificate
    insecure: bool = False  # Skip all validation (DEVELOPMENT ONLY)

    # Cipher configuration
    ciphers: Optional[str] = None  # Custom cipher string (None = use defaults)

    # Username/password authentication (optional, works with or without TLS)
    username: Optional[str] = None
    password: Optional[str] = None


class MQTTSecurityManager:
    """Manages MQTT TLS/SSL configuration."""

    def __init__(self, config: MQTTSecurityConfig):
        """
        Initialize MQTT security manager.

        Args:
            config: Security configuration
        """
        self.config = config

    def get_tls_params(self) -> Optional[dict]:
        """
        Get TLS parameters for aiomqtt.

        Returns:
            Dictionary with TLS parameters or None if TLS disabled
        """
        if not self.config.use_tls:
            return None

        # Build TLS context
        tls_context = self.create_ssl_context()

        # Create TLS params dict for aiomqtt
        tls_params = {
            "context": tls_context,
        }

        # Add certificate paths if provided
        if self.config.ca_cert_path:
            tls_params["ca_certs"] = str(Path(self.config.ca_cert_path).resolve())

        if self.config.client_cert_path:
            tls_params["certfile"] = str(Path(self.config.client_cert_path).resolve())

        if self.config.client_key_path:
            tls_params["keyfile"] = str(Path(self.config.client_key_path).resolve())

        return tls_params

    def create_ssl_context(self) -> ssl.SSLContext:
        """
        Create SSL context for MQTT connection.

        Returns:
            SSL context configured for secure MQTT connections

        Raises:
            ValueError: If configuration is invalid
        """
        # Select TLS version
        if self.config.tls_version == "TLSv1_2":
            protocol = ssl.PROTOCOL_TLSv1_2
        elif self.config.tls_version == "TLSv1_3":
            protocol = ssl.PROTOCOL_TLS  # TLS 1.3 uses PROTOCOL_TLS
        elif self.config.tls_version == "TLS":
            protocol = ssl.PROTOCOL_TLS  # Auto-negotiate
        else:
            logger.warning(f"Unknown TLS version: {self.config.tls_version}, using TLS 1.2")
            protocol = ssl.PROTOCOL_TLSv1_2

        # Create SSL context
        context = ssl.SSLContext(protocol)

        # Set certificate verification mode
        if self.config.insecure:
            logger.warning("⚠️  SECURITY WARNING: Certificate verification DISABLED (insecure mode)")
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        else:
            if self.config.cert_required:
                context.verify_mode = ssl.CERT_REQUIRED
            else:
                context.verify_mode = ssl.CERT_OPTIONAL

            context.check_hostname = self.config.check_hostname

        # Load CA certificate for server validation
        if self.config.ca_cert_path:
            ca_path = Path(self.config.ca_cert_path)
            if not ca_path.exists():
                raise ValueError(f"CA certificate not found: {ca_path}")

            context.load_verify_locations(cafile=str(ca_path))
            logger.info(f"✓ Loaded CA certificate: {ca_path}")
        else:
            # Load system default CA certificates
            context.load_default_certs(ssl.Purpose.SERVER_AUTH)
            logger.info("✓ Using system default CA certificates")

        # Load client certificate and key (mutual TLS)
        if self.config.client_cert_path and self.config.client_key_path:
            cert_path = Path(self.config.client_cert_path)
            key_path = Path(self.config.client_key_path)

            if not cert_path.exists():
                raise ValueError(f"Client certificate not found: {cert_path}")
            if not key_path.exists():
                raise ValueError(f"Client private key not found: {key_path}")

            context.load_cert_chain(
                certfile=str(cert_path),
                keyfile=str(key_path)
            )
            logger.info(f"✓ Loaded client certificate: {cert_path}")
            logger.info("✓ Mutual TLS (mTLS) enabled")

        # Set cipher suites
        if self.config.ciphers:
            context.set_ciphers(self.config.ciphers)
            logger.info(f"✓ Custom ciphers: {self.config.ciphers}")
        else:
            # Use modern secure ciphers (Mozilla Intermediate profile)
            context.set_ciphers(
                'ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS'
            )
            logger.info("✓ Using secure cipher suite")

        # Additional security options
        context.options |= ssl.OP_NO_SSLv2
        context.options |= ssl.OP_NO_SSLv3
        context.options |= ssl.OP_NO_TLSv1
        context.options |= ssl.OP_NO_TLSv1_1
        context.options |= ssl.OP_NO_COMPRESSION  # Disable compression (CRIME attack)
        context.options |= ssl.OP_CIPHER_SERVER_PREFERENCE

        return context

    def validate_configuration(self) -> bool:
        """
        Validate security configuration.

        Returns:
            True if configuration is valid

        Raises:
            ValueError: If configuration is invalid
        """
        if not self.config.use_tls:
            logger.info("MQTT TLS: DISABLED")
            return True

        # Check for insecure mode in production
        if self.config.insecure:
            logger.warning(
                "⚠️  SECURITY WARNING: insecure=True "
                "(acceptable for development, NEVER use in production)"
            )

        # Validate TLS version
        valid_tls_versions = ["TLSv1_2", "TLSv1_3", "TLS"]
        if self.config.tls_version not in valid_tls_versions:
            raise ValueError(
                f"Invalid TLS version: {self.config.tls_version}. "
                f"Valid options: {valid_tls_versions}"
            )

        # Check certificate files
        if self.config.ca_cert_path:
            if not Path(self.config.ca_cert_path).exists():
                raise ValueError(f"CA certificate not found: {self.config.ca_cert_path}")

        if self.config.client_cert_path:
            if not Path(self.config.client_cert_path).exists():
                raise ValueError(f"Client certificate not found: {self.config.client_cert_path}")

        if self.config.client_key_path:
            if not Path(self.config.client_key_path).exists():
                raise ValueError(f"Client key not found: {self.config.client_key_path}")

        # Validate mutual TLS configuration
        if self.config.client_cert_path and not self.config.client_key_path:
            raise ValueError("Client certificate provided but private key is missing")

        if self.config.client_key_path and not self.config.client_cert_path:
            raise ValueError("Client key provided but certificate is missing")

        # Validate username/password
        if self.config.username and not self.config.password:
            raise ValueError("Username provided but password is missing")

        if self.config.password and not self.config.username:
            raise ValueError("Password provided but username is missing")

        logger.info("✓ MQTT TLS configuration is valid")
        return True

    def log_security_status(self):
        """Log security configuration status for troubleshooting."""
        if not self.config.use_tls:
            logger.info("MQTT Security: DISABLED (unencrypted connection)")
            if self.config.username:
                logger.info(f"  - Username authentication: {self.config.username}")
            return

        logger.info("MQTT Security Configuration:")
        logger.info(f"  - TLS Version: {self.config.tls_version}")
        logger.info(f"  - CA Certificate: {self.config.ca_cert_path or 'System default'}")
        logger.info(f"  - Client Certificate: {self.config.client_cert_path or 'None (server auth only)'}")
        logger.info(f"  - Client Key: {self.config.client_key_path or 'None'}")
        logger.info(f"  - Certificate Required: {self.config.cert_required}")
        logger.info(f"  - Check Hostname: {self.config.check_hostname}")
        logger.info(f"  - Insecure Mode: {self.config.insecure}")
        logger.info(f"  - Custom Ciphers: {self.config.ciphers or 'Default secure ciphers'}")

        if self.config.username:
            logger.info(f"  - Username: {self.config.username}")

        if self.config.client_cert_path and self.config.client_key_path:
            logger.info("  - Mutual TLS (mTLS): ENABLED")
        else:
            logger.info("  - Mutual TLS (mTLS): DISABLED (server authentication only)")


def create_security_config_from_dict(config_dict: dict) -> MQTTSecurityConfig:
    """
    Create MQTT security configuration from dictionary.

    Args:
        config_dict: Configuration dictionary (from YAML/JSON)

    Returns:
        MQTTSecurityConfig instance
    """
    # Check if TLS should be enabled based on endpoint
    endpoint = config_dict.get("endpoint", "")
    use_tls = endpoint.startswith("mqtts://")

    # Get security-specific config (if present)
    security_dict = config_dict.get("security", {})

    return MQTTSecurityConfig(
        enabled=security_dict.get("enabled", use_tls),
        use_tls=use_tls or security_dict.get("use_tls", False),
        tls_version=security_dict.get("tls_version", "TLSv1_2"),
        ca_cert_path=security_dict.get("ca_cert_path"),
        client_cert_path=security_dict.get("client_cert_path"),
        client_key_path=security_dict.get("client_key_path"),
        cert_required=security_dict.get("cert_required", True),
        check_hostname=security_dict.get("check_hostname", True),
        insecure=security_dict.get("insecure", False),
        ciphers=security_dict.get("ciphers"),
        username=config_dict.get("username") or security_dict.get("username"),
        password=config_dict.get("password") or security_dict.get("password"),
    )
