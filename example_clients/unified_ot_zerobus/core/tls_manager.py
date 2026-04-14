"""
TLS/SSL Certificate Management for NIS2 Compliance.

Provides:
- Self-signed certificate generation for development
- Certificate validation and expiration checking
- TLS configuration for web server
- Certificate renewal automation

NIS2 Compliance: Article 21.2(h) - Encryption (data in transit)
"""

import os
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any
from cryptography import x509
from cryptography.x509.oid import NameOID, ExtensionOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import ssl

logger = logging.getLogger(__name__)


class TLSError(Exception):
    """Raised when TLS operations fail."""
    pass


class TLSManager:
    """
    Manages TLS certificates for HTTPS web server.

    Features:
    - Self-signed certificate generation
    - Certificate validation
    - Expiration monitoring
    - SSL context creation
    """

    def __init__(self, cert_dir: Optional[Path] = None):
        """
        Initialize TLS manager.

        Args:
            cert_dir: Directory for certificate storage
        """
        self.cert_dir = cert_dir or Path.home() / '.unified_connector' / 'certs'
        self.cert_file = self.cert_dir / 'server.crt'
        self.key_file = self.cert_dir / 'server.key'

        # Create directory if doesn't exist
        self.cert_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    def generate_self_signed_cert(
        self,
        common_name: str = "localhost",
        organization: str = "Unified OT Connector",
        validity_days: int = 365,
        key_size: int = 2048,
        san_list: Optional[list] = None
    ) -> Tuple[Path, Path]:
        """
        Generate self-signed TLS certificate.

        Args:
            common_name: Certificate common name (CN)
            organization: Organization name
            validity_days: Certificate validity in days
            key_size: RSA key size in bits (minimum 2048)
            san_list: Subject Alternative Names (DNS names and IP addresses)

        Returns:
            Tuple of (cert_file, key_file) paths

        Raises:
            TLSError: If certificate generation fails
        """
        logger.info(f"Generating self-signed certificate for {common_name}")

        try:
            # Generate private key
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=key_size,
                backend=default_backend()
            )

            # Certificate subject
            subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COUNTRY_NAME, "AU"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "NSW"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "Sydney"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, organization),
                x509.NameAttribute(NameOID.COMMON_NAME, common_name),
            ])

            # Build certificate
            cert_builder = x509.CertificateBuilder()
            cert_builder = cert_builder.subject_name(subject)
            cert_builder = cert_builder.issuer_name(issuer)
            cert_builder = cert_builder.public_key(private_key.public_key())
            cert_builder = cert_builder.serial_number(x509.random_serial_number())
            cert_builder = cert_builder.not_valid_before(datetime.utcnow())
            cert_builder = cert_builder.not_valid_after(
                datetime.utcnow() + timedelta(days=validity_days)
            )

            # Add extensions
            # Subject Alternative Names (SAN)
            if san_list is None:
                san_list = [common_name, "localhost", "127.0.0.1", "::1"]

            san_entries = []
            for san in san_list:
                # Check if IP address or DNS name
                if san.replace('.', '').replace(':', '').isdigit() or ':' in san:
                    # IP address
                    try:
                        san_entries.append(x509.IPAddress(san))
                    except:
                        # Try as DNS name if IP parsing fails
                        san_entries.append(x509.DNSName(san))
                else:
                    # DNS name
                    san_entries.append(x509.DNSName(san))

            cert_builder = cert_builder.add_extension(
                x509.SubjectAlternativeName(san_entries),
                critical=False,
            )

            # Basic constraints (CA=False)
            cert_builder = cert_builder.add_extension(
                x509.BasicConstraints(ca=False, path_length=None),
                critical=True,
            )

            # Key usage
            cert_builder = cert_builder.add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=True,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )

            # Extended key usage (server authentication)
            cert_builder = cert_builder.add_extension(
                x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]),
                critical=False,
            )

            # Sign certificate
            certificate = cert_builder.sign(private_key, hashes.SHA256(), default_backend())

            # Write private key
            with open(self.key_file, 'wb') as f:
                f.write(private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption()
                ))
            os.chmod(self.key_file, 0o600)

            # Write certificate
            with open(self.cert_file, 'wb') as f:
                f.write(certificate.public_bytes(serialization.Encoding.PEM))
            os.chmod(self.cert_file, 0o644)

            logger.info(f"Generated certificate: {self.cert_file}")
            logger.info(f"Generated private key: {self.key_file}")
            logger.info(f"Certificate valid for {validity_days} days")
            logger.info(f"Subject Alternative Names: {', '.join(san_list)}")

            return self.cert_file, self.key_file

        except Exception as e:
            raise TLSError(f"Failed to generate certificate: {e}")

    def load_certificate(self, cert_path: Optional[Path] = None) -> x509.Certificate:
        """
        Load certificate from file.

        Args:
            cert_path: Path to certificate file (default: self.cert_file)

        Returns:
            Certificate object

        Raises:
            TLSError: If certificate cannot be loaded
        """
        cert_path = cert_path or self.cert_file

        try:
            with open(cert_path, 'rb') as f:
                cert_data = f.read()

            certificate = x509.load_pem_x509_certificate(cert_data, default_backend())
            return certificate

        except Exception as e:
            raise TLSError(f"Failed to load certificate: {e}")

    def validate_certificate(self, cert_path: Optional[Path] = None) -> Dict[str, Any]:
        """
        Validate certificate and return details.

        Args:
            cert_path: Path to certificate file

        Returns:
            Dictionary with validation results
        """
        try:
            certificate = self.load_certificate(cert_path)

            now = datetime.utcnow()
            not_valid_before = certificate.not_valid_before
            not_valid_after = certificate.not_valid_after

            is_valid = not_valid_before <= now <= not_valid_after
            days_until_expiry = (not_valid_after - now).days

            # Extract subject
            subject = certificate.subject
            common_name = subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value

            # Extract SAN
            san_list = []
            try:
                san_ext = certificate.extensions.get_extension_for_oid(
                    ExtensionOID.SUBJECT_ALTERNATIVE_NAME
                )
                san_list = [str(name.value) for name in san_ext.value]
            except x509.ExtensionNotFound:
                pass

            result = {
                'valid': is_valid,
                'common_name': common_name,
                'not_valid_before': not_valid_before.isoformat(),
                'not_valid_after': not_valid_after.isoformat(),
                'days_until_expiry': days_until_expiry,
                'expired': days_until_expiry < 0,
                'expiring_soon': 0 <= days_until_expiry < 30,
                'san': san_list,
                'serial_number': certificate.serial_number,
            }

            return result

        except Exception as e:
            return {
                'valid': False,
                'error': str(e)
            }

    def check_expiration(self, warn_days: int = 30) -> bool:
        """
        Check if certificate is expired or expiring soon.

        Args:
            warn_days: Days before expiration to warn

        Returns:
            True if certificate needs renewal
        """
        if not self.cert_file.exists():
            logger.warning("Certificate file not found")
            return True

        validation = self.validate_certificate()

        if not validation.get('valid', False):
            logger.warning(f"Certificate is invalid: {validation.get('error', 'Unknown error')}")
            return True

        if validation.get('expired', False):
            logger.warning("Certificate has expired")
            return True

        if validation.get('expiring_soon', False):
            days = validation.get('days_until_expiry', 0)
            logger.warning(f"Certificate expiring in {days} days")
            return True

        return False

    def create_ssl_context(
        self,
        cert_file: Optional[Path] = None,
        key_file: Optional[Path] = None,
        require_client_cert: bool = False,
        ca_cert_file: Optional[Path] = None
    ) -> ssl.SSLContext:
        """
        Create SSL context for web server.

        Args:
            cert_file: Path to server certificate file
            key_file: Path to server private key file
            require_client_cert: Require client certificate authentication
            ca_cert_file: Path to CA certificate for verifying client certs

        Returns:
            SSL context configured for secure connections

        Raises:
            TLSError: If SSL context creation fails
        """
        cert_file = cert_file or self.cert_file
        key_file = key_file or self.key_file

        # Verify files exist
        if not cert_file.exists():
            raise TLSError(f"Certificate file not found: {cert_file}")
        if not key_file.exists():
            raise TLSError(f"Private key file not found: {key_file}")

        try:
            # Create SSL context with modern TLS settings
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

            # Load certificate and key
            context.load_cert_chain(str(cert_file), str(key_file))

            # Security settings (NIS2 compliant)
            context.minimum_version = ssl.TLSVersion.TLSv1_2  # Minimum TLS 1.2
            context.maximum_version = ssl.TLSVersion.TLSv1_3  # Prefer TLS 1.3

            # Cipher suites (strong ciphers only)
            # These are the Mozilla "Intermediate" configuration ciphers
            context.set_ciphers(
                'ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS'
            )

            # Prefer server cipher order
            context.options |= ssl.OP_CIPHER_SERVER_PREFERENCE

            # Disable compression (CRIME attack prevention)
            context.options |= ssl.OP_NO_COMPRESSION

            # Client certificate verification (optional)
            if require_client_cert:
                if ca_cert_file and ca_cert_file.exists():
                    # Load CA cert for verifying client certificates
                    context.load_verify_locations(cafile=str(ca_cert_file))
                    logger.info(f"Loaded CA certificate for client verification: {ca_cert_file}")
                else:
                    logger.warning("Client cert verification requested but no CA cert provided")

                context.verify_mode = ssl.CERT_REQUIRED
                context.check_hostname = False
                logger.info("Client certificate authentication REQUIRED")
            else:
                context.verify_mode = ssl.CERT_NONE

            logger.info(f"Created SSL context with cert: {cert_file}")
            logger.info(f"TLS version: {context.minimum_version.name} - {context.maximum_version.name}")

            return context

        except Exception as e:
            raise TLSError(f"Failed to create SSL context: {e}")

    def get_or_create_certificate(
        self,
        common_name: str = "localhost",
        force_regenerate: bool = False,
        **kwargs
    ) -> Tuple[Path, Path]:
        """
        Get existing certificate or create new one if needed.

        Args:
            common_name: Certificate common name
            force_regenerate: Force regeneration even if valid cert exists
            **kwargs: Additional arguments for generate_self_signed_cert()

        Returns:
            Tuple of (cert_file, key_file) paths
        """
        # Check if certificate exists and is valid
        if not force_regenerate and self.cert_file.exists() and self.key_file.exists():
            validation = self.validate_certificate()

            if validation.get('valid', False) and not validation.get('expiring_soon', False):
                logger.info(f"Using existing certificate: {self.cert_file}")
                days = validation.get('days_until_expiry', 0)
                logger.info(f"Certificate valid for {days} more days")
                return self.cert_file, self.key_file
            else:
                logger.info("Certificate invalid or expiring soon, regenerating")

        # Generate new certificate
        return self.generate_self_signed_cert(common_name=common_name, **kwargs)

    def print_certificate_info(self, cert_path: Optional[Path] = None):
        """
        Print certificate information in human-readable format.

        Args:
            cert_path: Path to certificate file
        """
        validation = self.validate_certificate(cert_path)

        if not validation.get('valid', False):
            print(f"❌ Certificate is INVALID: {validation.get('error', 'Unknown error')}")
            return

        print("🔒 Certificate Information")
        print("=" * 60)
        print(f"Common Name: {validation['common_name']}")
        print(f"Serial Number: {validation['serial_number']}")
        print(f"Valid From: {validation['not_valid_before']}")
        print(f"Valid Until: {validation['not_valid_after']}")
        print(f"Days Until Expiry: {validation['days_until_expiry']}")

        if validation.get('expired', False):
            print("⚠️  Status: EXPIRED")
        elif validation.get('expiring_soon', False):
            print("⚠️  Status: EXPIRING SOON (< 30 days)")
        else:
            print("✅ Status: VALID")

        if validation.get('san'):
            print(f"Subject Alternative Names:")
            for san in validation['san']:
                print(f"  - {san}")

        print("=" * 60)


def create_ssl_context_from_config(config: Dict[str, Any]) -> Optional[ssl.SSLContext]:
    """
    Create SSL context from configuration dictionary.

    Args:
        config: TLS configuration from config.yaml

    Returns:
        SSL context or None if TLS is disabled
    """
    if not config.get('enabled', False):
        return None

    tls_manager = TLSManager()

    # Check for custom certificate paths
    custom_cert = config.get('cert_file')
    custom_key = config.get('key_file')
    ca_cert = config.get('ca_cert_file')

    if custom_cert and custom_key:
        # Use custom certificate paths
        cert_file = Path(custom_cert).expanduser()
        key_file = Path(custom_key).expanduser()
        logger.info(f"Using custom TLS certificate: {cert_file}")
    else:
        # Get or create self-signed certificate
        cert_file, key_file = tls_manager.get_or_create_certificate(
            common_name=config.get('common_name', 'localhost'),
            san_list=config.get('san_list')
        )

    # Prepare CA cert path if provided
    ca_cert_file = None
    if ca_cert:
        ca_cert_file = Path(ca_cert).expanduser()

    # Create SSL context
    return tls_manager.create_ssl_context(
        cert_file=cert_file,
        key_file=key_file,
        require_client_cert=config.get('require_client_cert', False),
        ca_cert_file=ca_cert_file
    )


if __name__ == '__main__':
    """CLI for certificate management"""
    import argparse

    parser = argparse.ArgumentParser(description='TLS Certificate Manager')
    parser.add_argument('--generate', action='store_true', help='Generate new certificate')
    parser.add_argument('--validate', action='store_true', help='Validate existing certificate')
    parser.add_argument('--info', action='store_true', help='Print certificate information')
    parser.add_argument('--common-name', default='localhost', help='Certificate common name')
    parser.add_argument('--san', nargs='+', help='Subject Alternative Names')
    parser.add_argument('--days', type=int, default=365, help='Validity in days')

    args = parser.parse_args()

    tls_manager = TLSManager()

    if args.generate:
        cert_file, key_file = tls_manager.generate_self_signed_cert(
            common_name=args.common_name,
            validity_days=args.days,
            san_list=args.san
        )
        print(f"✅ Certificate generated: {cert_file}")
        print(f"✅ Private key generated: {key_file}")
        tls_manager.print_certificate_info()

    elif args.validate:
        validation = tls_manager.validate_certificate()
        if validation.get('valid', False):
            print("✅ Certificate is valid")
        else:
            print(f"❌ Certificate is invalid: {validation.get('error', 'Unknown')}")
        print(validation)

    elif args.info:
        tls_manager.print_certificate_info()

    else:
        parser.print_help()
