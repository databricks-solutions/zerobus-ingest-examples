"""
Data Encryption Module for NIS2 Compliance.

Provides encryption for:
- Data at rest (credentials, configuration, sensitive data)
- Encryption key management
- Secure key derivation from master password

NIS2 Compliance: Article 21.2(h) - Encryption
                Article 21.2(j) - Cryptography
"""

import os
import base64
import hashlib
import logging
from typing import Optional, Dict, Any
from pathlib import Path
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)


class EncryptionError(Exception):
    """Raised when encryption/decryption operations fail."""
    pass


class EncryptionManager:
    """
    Manages encryption for data at rest.

    Features:
    - AES-256 encryption via Fernet (symmetric encryption)
    - Key derivation from master password using PBKDF2
    - Salt-based key strengthening
    - Secure credential storage
    """

    def __init__(self, master_password: Optional[str] = None, key_file: Optional[str] = None):
        """
        Initialize encryption manager.

        Args:
            master_password: Master password for key derivation
            key_file: Path to file containing encryption key

        Raises:
            EncryptionError: If neither password nor key file provided
        """
        self.master_password = master_password or os.getenv('CONNECTOR_MASTER_PASSWORD')
        self.key_file = Path(key_file) if key_file else Path.home() / '.unified_connector' / 'encryption.key'
        self._cipher = None

        if not self.master_password and not self.key_file.exists():
            logger.warning(
                "No master password or encryption key found. "
                "Set CONNECTOR_MASTER_PASSWORD environment variable or create key file."
            )
            # Don't raise error - allow initialization but encryption will fail if attempted

    def _get_or_create_salt(self) -> bytes:
        """
        Get or create salt for key derivation.

        Salt is stored in a file to ensure consistent key derivation
        across application restarts.

        Returns:
            32-byte salt

        Raises:
            EncryptionError: If salt file operations fail
        """
        salt_file = self.key_file.parent / 'salt.bin'

        # Create directory if doesn't exist
        try:
            salt_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        except Exception as e:
            raise EncryptionError(f"Failed to create encryption directory: {e}")

        # Read existing salt
        if salt_file.exists():
            try:
                with open(salt_file, 'rb') as f:
                    salt = f.read()
                if len(salt) == 32:
                    return salt
                logger.warning("Invalid salt file, regenerating")
            except Exception as e:
                logger.warning(f"Failed to read salt file: {e}")

        # Generate new salt
        salt = os.urandom(32)

        # Save salt
        try:
            with open(salt_file, 'wb') as f:
                f.write(salt)
            os.chmod(salt_file, 0o600)
            logger.info(f"Generated new encryption salt: {salt_file}")
        except Exception as e:
            raise EncryptionError(f"Failed to save salt: {e}")

        return salt

    def _derive_key_from_password(self, password: str) -> bytes:
        """
        Derive encryption key from master password using PBKDF2.

        PBKDF2 Parameters:
        - Algorithm: SHA-256
        - Iterations: 480,000 (OWASP recommendation 2023)
        - Salt: 32 bytes
        - Output: 32 bytes (256-bit key)

        Args:
            password: Master password

        Returns:
            32-byte encryption key

        Raises:
            EncryptionError: If key derivation fails
        """
        try:
            salt = self._get_or_create_salt()

            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=480000  # OWASP 2023 recommendation
            )

            key = kdf.derive(password.encode('utf-8'))
            return base64.urlsafe_b64encode(key)

        except Exception as e:
            raise EncryptionError(f"Key derivation failed: {e}")

    def _get_or_create_key(self) -> bytes:
        """
        Get or create encryption key.

        Priority:
        1. Derive from master password if available
        2. Read from key file if exists
        3. Generate new random key and save to file

        Returns:
            Fernet-compatible encryption key

        Raises:
            EncryptionError: If key operations fail
        """
        # Option 1: Derive from master password
        if self.master_password:
            return self._derive_key_from_password(self.master_password)

        # Option 2: Read from key file
        if self.key_file.exists():
            try:
                with open(self.key_file, 'rb') as f:
                    key = f.read()
                logger.info(f"Loaded encryption key from {self.key_file}")
                return key
            except Exception as e:
                logger.error(f"Failed to read key file: {e}")
                # Continue to generate new key

        # Option 3: Generate new key
        logger.info("Generating new encryption key")
        key = Fernet.generate_key()

        # Save key to file
        try:
            self.key_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with open(self.key_file, 'wb') as f:
                f.write(key)
            os.chmod(self.key_file, 0o600)
            logger.info(f"Saved encryption key to {self.key_file}")
        except Exception as e:
            logger.warning(f"Failed to save key file: {e}")
            # Continue with in-memory key

        return key

    def _get_cipher(self) -> Fernet:
        """
        Get Fernet cipher instance.

        Cipher is cached after first initialization.

        Returns:
            Fernet cipher instance

        Raises:
            EncryptionError: If cipher initialization fails
        """
        if self._cipher is None:
            try:
                key = self._get_or_create_key()
                self._cipher = Fernet(key)
            except Exception as e:
                raise EncryptionError(f"Failed to initialize cipher: {e}")

        return self._cipher

    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt plaintext string.

        Args:
            plaintext: String to encrypt

        Returns:
            Base64-encoded encrypted string

        Raises:
            EncryptionError: If encryption fails
        """
        if not plaintext:
            return ""

        try:
            cipher = self._get_cipher()
            encrypted_bytes = cipher.encrypt(plaintext.encode('utf-8'))
            return encrypted_bytes.decode('ascii')
        except Exception as e:
            raise EncryptionError(f"Encryption failed: {e}")

    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypt encrypted string.

        Args:
            ciphertext: Base64-encoded encrypted string

        Returns:
            Decrypted plaintext string

        Raises:
            EncryptionError: If decryption fails (wrong key, corrupted data)
        """
        if not ciphertext:
            return ""

        try:
            cipher = self._get_cipher()
            decrypted_bytes = cipher.decrypt(ciphertext.encode('ascii'))
            return decrypted_bytes.decode('utf-8')
        except InvalidToken:
            raise EncryptionError(
                "Decryption failed: Invalid key or corrupted data. "
                "Ensure CONNECTOR_MASTER_PASSWORD is correct."
            )
        except Exception as e:
            raise EncryptionError(f"Decryption failed: {e}")

    def encrypt_dict(self, data: Dict[str, Any], fields_to_encrypt: list) -> Dict[str, Any]:
        """
        Encrypt specific fields in a dictionary.

        Args:
            data: Dictionary to encrypt
            fields_to_encrypt: List of field names to encrypt

        Returns:
            Dictionary with encrypted fields

        Example:
            data = {'username': 'admin', 'password': 'secret123'}
            encrypted = encrypt_dict(data, ['password'])
            # Result: {'username': 'admin', 'password': '<encrypted>'}
        """
        encrypted_data = data.copy()

        for field in fields_to_encrypt:
            if field in encrypted_data and encrypted_data[field]:
                try:
                    encrypted_data[field] = self.encrypt(str(encrypted_data[field]))
                    logger.debug(f"Encrypted field: {field}")
                except EncryptionError as e:
                    logger.error(f"Failed to encrypt field {field}: {e}")

        return encrypted_data

    def decrypt_dict(self, data: Dict[str, Any], fields_to_decrypt: list) -> Dict[str, Any]:
        """
        Decrypt specific fields in a dictionary.

        Args:
            data: Dictionary with encrypted fields
            fields_to_decrypt: List of field names to decrypt

        Returns:
            Dictionary with decrypted fields

        Example:
            encrypted = {'username': 'admin', 'password': '<encrypted>'}
            decrypted = decrypt_dict(encrypted, ['password'])
            # Result: {'username': 'admin', 'password': 'secret123'}
        """
        decrypted_data = data.copy()

        for field in fields_to_decrypt:
            if field in decrypted_data and decrypted_data[field]:
                try:
                    decrypted_data[field] = self.decrypt(str(decrypted_data[field]))
                    logger.debug(f"Decrypted field: {field}")
                except EncryptionError as e:
                    logger.error(f"Failed to decrypt field {field}: {e}")
                    # Keep encrypted value rather than failing

        return decrypted_data

    def encrypt_file(self, input_path: Path, output_path: Optional[Path] = None) -> Path:
        """
        Encrypt a file.

        Args:
            input_path: Path to file to encrypt
            output_path: Path for encrypted file (default: input_path + .enc)

        Returns:
            Path to encrypted file

        Raises:
            EncryptionError: If file encryption fails
        """
        input_path = Path(input_path)
        output_path = Path(output_path) if output_path else input_path.with_suffix(input_path.suffix + '.enc')

        try:
            # Read plaintext
            with open(input_path, 'rb') as f:
                plaintext = f.read()

            # Encrypt
            cipher = self._get_cipher()
            ciphertext = cipher.encrypt(plaintext)

            # Write encrypted file
            with open(output_path, 'wb') as f:
                f.write(ciphertext)

            os.chmod(output_path, 0o600)
            logger.info(f"Encrypted file: {input_path} -> {output_path}")

            return output_path

        except Exception as e:
            raise EncryptionError(f"File encryption failed: {e}")

    def decrypt_file(self, input_path: Path, output_path: Optional[Path] = None) -> Path:
        """
        Decrypt a file.

        Args:
            input_path: Path to encrypted file
            output_path: Path for decrypted file (default: input_path without .enc)

        Returns:
            Path to decrypted file

        Raises:
            EncryptionError: If file decryption fails
        """
        input_path = Path(input_path)

        # Default output path: remove .enc extension
        if output_path is None:
            if input_path.suffix == '.enc':
                output_path = input_path.with_suffix('')
            else:
                output_path = input_path.with_suffix('.dec')

        output_path = Path(output_path)

        try:
            # Read encrypted file
            with open(input_path, 'rb') as f:
                ciphertext = f.read()

            # Decrypt
            cipher = self._get_cipher()
            plaintext = cipher.decrypt(ciphertext)

            # Write decrypted file
            with open(output_path, 'wb') as f:
                f.write(plaintext)

            os.chmod(output_path, 0o600)
            logger.info(f"Decrypted file: {input_path} -> {output_path}")

            return output_path

        except InvalidToken:
            raise EncryptionError(
                f"File decryption failed: Invalid key or corrupted file. "
                f"Ensure CONNECTOR_MASTER_PASSWORD is correct."
            )
        except Exception as e:
            raise EncryptionError(f"File decryption failed: {e}")

    def hash_password(self, password: str) -> str:
        """
        Hash a password using SHA-256.

        Note: For production password storage, use bcrypt or Argon2.
        This is for configuration file checksums.

        Args:
            password: Password to hash

        Returns:
            Hex-encoded hash
        """
        return hashlib.sha256(password.encode('utf-8')).hexdigest()

    def verify_password(self, password: str, password_hash: str) -> bool:
        """
        Verify password against hash.

        Args:
            password: Password to verify
            password_hash: Hash to verify against

        Returns:
            True if password matches hash
        """
        return self.hash_password(password) == password_hash


# Global encryption manager instance (lazy initialization)
_encryption_manager = None


def get_encryption_manager() -> EncryptionManager:
    """
    Get global encryption manager instance.

    Returns:
        EncryptionManager instance
    """
    global _encryption_manager
    if _encryption_manager is None:
        _encryption_manager = EncryptionManager()
    return _encryption_manager


def encrypt(plaintext: str) -> str:
    """
    Convenience function to encrypt string.

    Args:
        plaintext: String to encrypt

    Returns:
        Encrypted string
    """
    return get_encryption_manager().encrypt(plaintext)


def decrypt(ciphertext: str) -> str:
    """
    Convenience function to decrypt string.

    Args:
        ciphertext: String to decrypt

    Returns:
        Decrypted string
    """
    return get_encryption_manager().decrypt(ciphertext)
