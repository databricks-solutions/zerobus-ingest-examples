"""Secure credential management with Fernet encryption.

Combines best practices from both iot_connector and ot_simulator:
- Fernet AES-256-CBC encryption
- PBKDF2 key derivation (480k iterations)
- Per-installation salt
- Environment variable fallback
- Secure file permissions (0o600)
"""

import base64
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logger = logging.getLogger(__name__)


class CredentialManager:
    """Secure credential storage with Fernet encryption."""

    def __init__(
        self,
        credentials_dir: Optional[Path] = None,
        master_password: Optional[str] = None
    ):
        """Initialize credential manager.

        Args:
            credentials_dir: Directory for encrypted credentials (default: ~/.unified_connector)
            master_password: Master password for encryption (or use CONNECTOR_MASTER_PASSWORD env var)
        """
        if credentials_dir is None:
            credentials_dir = Path.home() / ".unified_connector"

        self.credentials_dir = Path(credentials_dir)
        try:
            self.credentials_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError as e:
            fallback_dir = Path.cwd() / ".unified_connector"
            logger.warning(
                f"Cannot access credentials dir {self.credentials_dir} ({e}). Falling back to {fallback_dir}."
            )
            self.credentials_dir = fallback_dir
            self.credentials_dir.mkdir(parents=True, exist_ok=True)

        # Credentials file
        self.credentials_file = self.credentials_dir / "credentials.enc"

        # Get master password
        self.master_password = master_password or os.getenv('CONNECTOR_MASTER_PASSWORD')

        if not self.master_password:
            logger.warning("No master password provided. Credential encryption disabled.")
            logger.warning("Set CONNECTOR_MASTER_PASSWORD environment variable to enable.")

        self._cipher = None
        self._salt = None

        # Load or generate salt
        self._load_or_create_salt()

    def _load_or_create_salt(self) -> bytes:
        """Load existing salt or generate new one."""
        salt_file = self.credentials_dir / "salt.txt"

        try:
            if salt_file.exists():
                with open(salt_file, 'r') as f:
                    self._salt = base64.b64decode(f.read().strip())
                logger.debug("Loaded existing salt")
                return

            # Generate new random salt
            self._salt = os.urandom(16)
            with open(salt_file, 'w') as f:
                f.write(base64.b64encode(self._salt).decode('utf-8'))

            # Secure permissions
            salt_file.chmod(0o600)
            logger.info("Generated new salt for credential encryption")

        except PermissionError as e:
            fallback_dir = Path.cwd() / ".unified_connector"
            if self.credentials_dir.resolve() != fallback_dir.resolve():
                logger.warning(
                    f"Permission error accessing {salt_file} ({e}). Falling back to {fallback_dir}."
                )
                self.credentials_dir = fallback_dir
                self.credentials_dir.mkdir(parents=True, exist_ok=True)
                self.credentials_file = self.credentials_dir / "credentials.enc"
                # Retry once in fallback dir
                self._load_or_create_salt()
                return
            raise


    def _get_cipher(self) -> Optional[Fernet]:
        """Get or create Fernet cipher with PBKDF2 key derivation."""
        if self._cipher:
            return self._cipher

        if not self.master_password:
            return None

        # Derive key from master password using PBKDF2
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self._salt,
            iterations=480000,  # OWASP recommended minimum
        )
        key = base64.urlsafe_b64encode(
            kdf.derive(self.master_password.encode('utf-8'))
        )
        self._cipher = Fernet(key)

        return self._cipher

    def store_credentials(self, credentials: Dict[str, Any]) -> bool:
        """Store encrypted credentials to disk.

        Args:
            credentials: Dictionary of credentials to encrypt and store

        Returns:
            True if successful, False otherwise
        """
        cipher = self._get_cipher()
        if not cipher:
            logger.error("Cannot store credentials without master password")
            return False

        try:
            # Serialize to JSON
            json_data = json.dumps(credentials, indent=2)

            # Encrypt
            encrypted = cipher.encrypt(json_data.encode('utf-8'))

            # Write to file
            with open(self.credentials_file, 'wb') as f:
                f.write(encrypted)

            # Set restrictive permissions (owner read/write only)
            self.credentials_file.chmod(0o600)

            logger.info(f"Stored encrypted credentials to {self.credentials_file}")
            return True

        except Exception as e:
            logger.error(f"Failed to store credentials: {e}")
            return False

    def load_credentials(self) -> Optional[Dict[str, Any]]:
        """Load and decrypt credentials from disk.

        Returns:
            Dictionary of credentials, or None if not found or decryption fails
        """
        if not self.credentials_file.exists():
            logger.debug("Credentials file does not exist")
            return None

        cipher = self._get_cipher()
        if not cipher:
            logger.error("Cannot load credentials without master password")
            return None

        try:
            # Read encrypted data
            with open(self.credentials_file, 'rb') as f:
                encrypted = f.read()

            # Decrypt
            decrypted = cipher.decrypt(encrypted)

            # Parse JSON
            credentials = json.loads(decrypted.decode('utf-8'))

            logger.debug("Loaded encrypted credentials")
            return credentials

        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
            return None

    def get_credential(self, key: str, default: Any = None) -> Any:
        """Get a specific credential by key.

        Precedence:
            1. Environment variable (CONNECTOR_<KEY>)
            2. Encrypted credentials file
            3. Default value

        Args:
            key: Credential key (supports nested keys with dots, e.g., 'zerobus.client_id')
            default: Default value if credential not found

        Returns:
            Credential value or default
        """
        # Check environment variable first (best practice for containers)
        env_key = f"CONNECTOR_{key.upper().replace('.', '_')}"
        env_value = os.getenv(env_key)
        if env_value:
            return env_value

        # Check encrypted credentials
        credentials = self.load_credentials()
        if credentials:
            # Support nested keys (e.g., 'zerobus.client_id')
            parts = key.split('.')
            value = credentials
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    return default
            return value

        return default

    def update_credential(self, key: str, value: Any) -> bool:
        """Update a single credential.

        Args:
            key: Credential key (supports nested keys with dots)
            value: New credential value

        Returns:
            True if successful, False otherwise
        """
        # Load existing credentials
        credentials = self.load_credentials() or {}

        # Update nested key
        parts = key.split('.')
        current = credentials
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

        # Store updated credentials
        return self.store_credentials(credentials)

    def delete_credentials(self) -> bool:
        """Delete encrypted credentials file.

        Returns:
            True if successful, False otherwise
        """
        try:
            if self.credentials_file.exists():
                self.credentials_file.unlink()
                logger.info("Deleted credentials file")
            return True
        except Exception as e:
            logger.error(f"Failed to delete credentials: {e}")
            return False

    def list_credential_keys(self) -> list[str]:
        """List all stored credential keys.

        Returns:
            List of credential keys (flattened dot notation)
        """
        credentials = self.load_credentials()
        if not credentials:
            return []

        def flatten_keys(d: dict, prefix: str = "") -> list[str]:
            """Recursively flatten nested keys."""
            keys = []
            for k, v in d.items():
                full_key = f"{prefix}.{k}" if prefix else k
                if isinstance(v, dict):
                    keys.extend(flatten_keys(v, full_key))
                else:
                    keys.append(full_key)
            return keys

        return flatten_keys(credentials)


def generate_master_password() -> str:
    """Generate a secure random master password.

    Returns:
        Base64-encoded 32-byte random password
    """
    return base64.urlsafe_b64encode(os.urandom(32)).decode('utf-8')


# CLI helper
if __name__ == '__main__':
    import sys
    import getpass

    if len(sys.argv) < 2:
        print("Unified Connector - Credential Manager CLI")
        print("\nUsage:")
        print("  python credential_manager.py generate-password     - Generate secure master password")
        print("  python credential_manager.py store <key> <value>   - Store a credential")
        print("  python credential_manager.py get <key>             - Get a credential")
        print("  python credential_manager.py list                  - List all credential keys")
        print("  python credential_manager.py delete                - Delete all credentials")
        print("\nEnvironment Variables:")
        print("  CONNECTOR_MASTER_PASSWORD - Master password for encryption/decryption")
        sys.exit(1)

    command = sys.argv[1]

    if command == 'generate-password':
        password = generate_master_password()
        print(f"Generated master password: {password}")
        print("\nSet this as environment variable:")
        print(f"export CONNECTOR_MASTER_PASSWORD='{password}'")
        sys.exit(0)

    # Get master password
    master_password = os.getenv('CONNECTOR_MASTER_PASSWORD')
    if not master_password:
        master_password = getpass.getpass("Enter master password: ")

    manager = CredentialManager(master_password=master_password)

    if command == 'store':
        if len(sys.argv) != 4:
            print("Usage: python credential_manager.py store <key> <value>")
            sys.exit(1)

        key = sys.argv[2]
        value = sys.argv[3]

        if manager.update_credential(key, value):
            print(f"✓ Stored credential: {key}")
        else:
            print(f"✗ Failed to store credential: {key}")
            sys.exit(1)

    elif command == 'get':
        if len(sys.argv) != 3:
            print("Usage: python credential_manager.py get <key>")
            sys.exit(1)

        key = sys.argv[2]
        value = manager.get_credential(key)

        if value is not None:
            print(f"{key} = {value}")
        else:
            print(f"Credential not found: {key}")
            sys.exit(1)

    elif command == 'list':
        keys = manager.list_credential_keys()
        if keys:
            print("Stored credential keys:")
            for key in keys:
                print(f"  - {key}")
        else:
            print("No credentials stored")

    elif command == 'delete':
        confirm = input("Delete all encrypted credentials? (yes/no): ")
        if confirm.lower() == 'yes':
            if manager.delete_credentials():
                print("✓ Deleted all credentials")
            else:
                print("✗ Failed to delete credentials")
                sys.exit(1)
        else:
            print("Cancelled")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
