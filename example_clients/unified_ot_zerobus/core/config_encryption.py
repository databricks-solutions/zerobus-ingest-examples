"""
Configuration File Encryption for NIS2 Compliance.

Provides:
- Encryption of sensitive configuration fields
- Selective field encryption (encrypt only sensitive data)
- Environment variable substitution
- Backward compatibility with unencrypted configs

NIS2 Compliance: Article 21.2(h) - Encryption (data at rest)
"""

import os
import re
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import yaml

from unified_connector.core.encryption import get_encryption_manager, EncryptionError

logger = logging.getLogger(__name__)


# Fields that should be encrypted in configuration files
SENSITIVE_FIELDS = {
    'password', 'secret', 'token', 'api_key', 'private_key',
    'client_secret', 'master_password', 'session_secret_key',
    'encryption_key', 'auth_token', 'access_token', 'refresh_token',
}


class ConfigEncryption:
    """
    Handles encryption of sensitive configuration data.

    Features:
    - Automatic detection of sensitive fields
    - Selective encryption (only sensitive fields)
    - Environment variable substitution
    - Encrypted value markers for easy identification
    """

    def __init__(self, master_password: Optional[str] = None):
        """
        Initialize config encryption.

        Args:
            master_password: Master password for encryption
        """
        self.encryption_manager = get_encryption_manager()
        self.encrypted_prefix = "ENC["
        self.encrypted_suffix = "]"

    def is_encrypted(self, value: str) -> bool:
        """
        Check if a value is already encrypted.

        Args:
            value: String value to check

        Returns:
            True if value is encrypted
        """
        if not isinstance(value, str):
            return False
        return value.startswith(self.encrypted_prefix) and value.endswith(self.encrypted_suffix)

    def is_environment_variable(self, value: str) -> bool:
        """
        Check if a value is an environment variable reference.

        Args:
            value: String value to check

        Returns:
            True if value references an environment variable
        """
        if not isinstance(value, str):
            return False
        # Matches: ${env:VAR_NAME} or ${env:VAR_NAME:default_value}
        return bool(re.match(r'^\$\{env:[^}]+\}$', value))

    def is_credential_reference(self, value: str) -> bool:
        """
        Check if a value is a credential reference.

        Args:
            value: String value to check

        Returns:
            True if value references a credential
        """
        if not isinstance(value, str):
            return False
        # Matches: ${credential:key_name}
        return bool(re.match(r'^\$\{credential:[^}]+\}$', value))

    def is_sensitive_field(self, key: str) -> bool:
        """
        Check if a configuration key should be encrypted.

        Args:
            key: Configuration key name

        Returns:
            True if key contains sensitive data
        """
        key_lower = key.lower()
        return any(sensitive in key_lower for sensitive in SENSITIVE_FIELDS)

    def encrypt_value(self, value: str) -> str:
        """
        Encrypt a configuration value.

        Args:
            value: Plaintext value

        Returns:
            Encrypted value with ENC[] marker

        Raises:
            EncryptionError: If encryption fails
        """
        if not value:
            return value

        # Don't encrypt if already encrypted
        if self.is_encrypted(value):
            return value

        # Don't encrypt environment variables or credential references
        if self.is_environment_variable(value) or self.is_credential_reference(value):
            return value

        try:
            encrypted = self.encryption_manager.encrypt(value)
            return f"{self.encrypted_prefix}{encrypted}{self.encrypted_suffix}"
        except EncryptionError as e:
            logger.error(f"Failed to encrypt value: {e}")
            raise

    def decrypt_value(self, value: str) -> str:
        """
        Decrypt a configuration value.

        Args:
            value: Encrypted value with ENC[] marker

        Returns:
            Decrypted plaintext value

        Raises:
            EncryptionError: If decryption fails
        """
        if not value or not self.is_encrypted(value):
            return value

        try:
            # Remove ENC[ and ] markers
            encrypted = value[len(self.encrypted_prefix):-len(self.encrypted_suffix)]
            return self.encryption_manager.decrypt(encrypted)
        except EncryptionError as e:
            logger.error(f"Failed to decrypt value: {e}")
            raise

    def encrypt_config(
        self,
        config: Any,
        parent_key: str = "",
        sensitive_fields: Optional[Set[str]] = None
    ) -> Any:
        """
        Recursively encrypt sensitive fields in configuration.

        Args:
            config: Configuration value (dict, list, str, etc.)
            parent_key: Parent key for nested structures
            sensitive_fields: Additional sensitive field names (beyond defaults)

        Returns:
            Configuration with sensitive fields encrypted
        """
        if sensitive_fields is None:
            sensitive_fields = SENSITIVE_FIELDS
        else:
            sensitive_fields = SENSITIVE_FIELDS | sensitive_fields

        if isinstance(config, dict):
            encrypted_config = {}
            for key, value in config.items():
                # Build full key path for nested structures
                full_key = f"{parent_key}.{key}" if parent_key else key

                # Encrypt if sensitive field
                if self.is_sensitive_field(key) and isinstance(value, str):
                    if value and not self.is_environment_variable(value) and not self.is_credential_reference(value):
                        try:
                            encrypted_config[key] = self.encrypt_value(value)
                            logger.debug(f"Encrypted field: {full_key}")
                        except EncryptionError:
                            logger.warning(f"Failed to encrypt field: {full_key}, keeping plaintext")
                            encrypted_config[key] = value
                    else:
                        encrypted_config[key] = value
                else:
                    # Recursively process nested structures
                    encrypted_config[key] = self.encrypt_config(value, full_key, sensitive_fields)

            return encrypted_config

        elif isinstance(config, list):
            return [self.encrypt_config(item, parent_key, sensitive_fields) for item in config]

        else:
            return config

    def decrypt_config(self, config: Any, parent_key: str = "") -> Any:
        """
        Recursively decrypt encrypted fields in configuration.

        Args:
            config: Configuration value (dict, list, str, etc.)
            parent_key: Parent key for nested structures

        Returns:
            Configuration with encrypted fields decrypted
        """
        if isinstance(config, dict):
            decrypted_config = {}
            for key, value in config.items():
                full_key = f"{parent_key}.{key}" if parent_key else key

                if isinstance(value, str) and self.is_encrypted(value):
                    try:
                        decrypted_config[key] = self.decrypt_value(value)
                        logger.debug(f"Decrypted field: {full_key}")
                    except EncryptionError:
                        logger.error(f"Failed to decrypt field: {full_key}, keeping encrypted")
                        decrypted_config[key] = value
                else:
                    decrypted_config[key] = self.decrypt_config(value, full_key)

            return decrypted_config

        elif isinstance(config, list):
            return [self.decrypt_config(item, parent_key) for item in config]

        else:
            return config

    def load_config(self, config_path: Path, decrypt: bool = True) -> Dict[str, Any]:
        """
        Load configuration file with optional decryption.

        Args:
            config_path: Path to YAML configuration file
            decrypt: Whether to decrypt encrypted fields

        Returns:
            Configuration dictionary

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config is invalid
        """
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        # Load YAML
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        if not config:
            raise ValueError(f"Empty configuration file: {config_path}")

        # Decrypt if requested
        if decrypt:
            config = self.decrypt_config(config)

        logger.info(f"Loaded configuration from {config_path}")
        return config

    def save_config(
        self,
        config: Dict[str, Any],
        config_path: Path,
        encrypt: bool = True,
        backup: bool = True
    ) -> bool:
        """
        Save configuration file with optional encryption.

        Args:
            config: Configuration dictionary
            config_path: Path to save configuration
            encrypt: Whether to encrypt sensitive fields
            backup: Whether to create backup before saving

        Returns:
            True if successful

        Raises:
            Exception: If save fails
        """
        try:
            # Create backup if requested
            if backup and config_path.exists():
                backup_path = config_path.with_suffix('.yaml.bak')
                import shutil
                shutil.copy2(config_path, backup_path)
                logger.info(f"Created backup: {backup_path}")

            # Encrypt sensitive fields if requested
            config_to_save = config
            if encrypt:
                config_to_save = self.encrypt_config(config)
                logger.info("Encrypted sensitive fields in configuration")

            # Create parent directory if needed
            config_path.parent.mkdir(parents=True, exist_ok=True)

            # Save YAML
            with open(config_path, 'w') as f:
                yaml.safe_dump(config_to_save, f, default_flow_style=False, sort_keys=False)

            # Set secure permissions
            config_path.chmod(0o600)

            logger.info(f"Saved configuration to {config_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            raise

    def identify_sensitive_fields(self, config: Dict[str, Any]) -> List[str]:
        """
        Identify all sensitive fields in configuration.

        Args:
            config: Configuration dictionary

        Returns:
            List of sensitive field paths (e.g., ["web_ui.authentication.session.secret_key"])
        """
        sensitive_fields = []

        def traverse(obj: Any, path: str = ""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key

                    if self.is_sensitive_field(key):
                        sensitive_fields.append(current_path)

                    traverse(value, current_path)
            elif isinstance(obj, list):
                for idx, item in enumerate(obj):
                    traverse(item, f"{path}[{idx}]")

        traverse(config)
        return sensitive_fields

    def migrate_to_encrypted(
        self,
        config_path: Path,
        output_path: Optional[Path] = None
    ) -> bool:
        """
        Migrate plaintext configuration to encrypted format.

        Args:
            config_path: Path to existing plaintext config
            output_path: Path for encrypted config (default: config_path.encrypted.yaml)

        Returns:
            True if successful
        """
        if output_path is None:
            output_path = config_path.with_suffix('.encrypted.yaml')

        try:
            # Load plaintext config
            config = self.load_config(config_path, decrypt=False)

            # Identify sensitive fields
            sensitive = self.identify_sensitive_fields(config)
            logger.info(f"Found {len(sensitive)} sensitive fields to encrypt:")
            for field in sensitive:
                logger.info(f"  - {field}")

            # Save with encryption
            self.save_config(config, output_path, encrypt=True, backup=False)

            logger.info(f"Migration complete: {config_path} → {output_path}")
            logger.info("Verify the encrypted configuration works, then replace the original")
            return True

        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False


def encrypt_config_file(
    config_path: Path,
    output_path: Optional[Path] = None,
    master_password: Optional[str] = None
) -> bool:
    """
    CLI helper to encrypt a configuration file.

    Args:
        config_path: Path to plaintext configuration
        output_path: Path for encrypted output
        master_password: Master password for encryption

    Returns:
        True if successful
    """
    config_encryption = ConfigEncryption(master_password)
    return config_encryption.migrate_to_encrypted(config_path, output_path)


if __name__ == '__main__':
    """CLI for configuration encryption"""
    import argparse

    parser = argparse.ArgumentParser(description='Configuration File Encryption Tool')
    parser.add_argument('config', type=Path, help='Path to configuration file')
    parser.add_argument('--encrypt', action='store_true', help='Encrypt sensitive fields')
    parser.add_argument('--decrypt', action='store_true', help='Decrypt encrypted fields')
    parser.add_argument('--identify', action='store_true', help='Identify sensitive fields')
    parser.add_argument('--output', type=Path, help='Output file path')

    args = parser.parse_args()

    config_encryption = ConfigEncryption()

    try:
        if args.identify:
            config = config_encryption.load_config(args.config, decrypt=False)
            sensitive = config_encryption.identify_sensitive_fields(config)
            print(f"Found {len(sensitive)} sensitive fields:")
            for field in sensitive:
                print(f"  - {field}")

        elif args.encrypt:
            success = config_encryption.migrate_to_encrypted(args.config, args.output)
            if success:
                print("✅ Configuration encrypted successfully")
            else:
                print("❌ Encryption failed")
                exit(1)

        elif args.decrypt:
            config = config_encryption.load_config(args.config, decrypt=True)
            output_path = args.output or args.config.with_suffix('.decrypted.yaml')
            config_encryption.save_config(config, output_path, encrypt=False, backup=False)
            print(f"✅ Configuration decrypted: {output_path}")

        else:
            parser.print_help()

    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)
