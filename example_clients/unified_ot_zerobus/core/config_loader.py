"""Configuration loader with credential injection support.

Loads configuration from YAML files and injects credentials from secure storage.
Supports encrypted configuration fields for NIS2 compliance.
"""

import logging
import os
import re
import yaml
from pathlib import Path
from typing import Any, Dict, Optional

from unified_connector.core.credential_manager import CredentialManager
from unified_connector.core.config_encryption import ConfigEncryption

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Configuration loader with credential injection."""

    def __init__(
        self,
        config_path: Optional[Path] = None,
        credential_manager: Optional[CredentialManager] = None,
        enable_encryption: bool = True
    ):
        """Initialize config loader.

        Args:
            config_path: Path to config.yaml (default: unified_connector/config/config.yaml)
            credential_manager: Credential manager instance (default: create new)
            enable_encryption: Enable config file encryption (default: True)
        """
        if config_path is None:
            # Default to config.yaml in package
            config_path = Path(__file__).parent.parent / "config" / "config.yaml"

        self.config_path = Path(config_path)

        if credential_manager is None:
            credential_manager = CredentialManager()

        self.credential_manager = credential_manager
        self.config_encryption = ConfigEncryption() if enable_encryption else None

    def load(self, inject_credentials: bool = True, decrypt_config: bool = True) -> Dict[str, Any]:
        """Load configuration from YAML file (optionally inject credentials and decrypt).

        Args:
            inject_credentials: Inject credentials from credential manager
            decrypt_config: Decrypt encrypted configuration fields

        Returns:
            Configuration dictionary with credentials injected and fields decrypted
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        # Load YAML
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)

        if not config:
            raise ValueError(f"Empty configuration file: {self.config_path}")

        # Decrypt encrypted fields (NIS2 compliance - Article 21.2(h))
        if decrypt_config and self.config_encryption:
            config = self.config_encryption.decrypt_config(config)

        # Substitute environment variables
        config = self._substitute_environment_variables(config)

        # Inject credentials (optional). IMPORTANT: callers that plan to save the config
        # should load with inject_credentials=False to avoid persisting secrets.
        if inject_credentials:
            config = self._inject_credentials(config)

        logger.info(f"Loaded configuration from {self.config_path}")
        return config

    def _substitute_environment_variables(self, config: Any) -> Any:
        """
        Recursively substitute environment variables.

        Replaces placeholders like '${env:VAR_NAME}' or '${env:VAR_NAME:default}' with actual values.

        Args:
            config: Configuration value (dict, list, str, etc.)

        Returns:
            Configuration with environment variables substituted
        """
        if isinstance(config, dict):
            return {
                k: self._substitute_environment_variables(v)
                for k, v in config.items()
            }
        elif isinstance(config, list):
            return [self._substitute_environment_variables(item) for item in config]
        elif isinstance(config, str):
            # Match ${env:VAR_NAME} or ${env:VAR_NAME:default_value}
            match = re.match(r'^\$\{env:([^:}]+)(?::([^}]*))?\}$', config)
            if match:
                var_name = match.group(1)
                default_value = match.group(2) if match.group(2) is not None else ""

                # Get environment variable
                value = os.getenv(var_name, default_value)

                if not value and not default_value:
                    logger.warning(f"Environment variable not found: {var_name}")

                return value
            return config
        else:
            return config

    def _inject_credentials(self, config: Any) -> Any:
        """Recursively inject credentials from secure storage.

        Replaces placeholders like '${credential:zerobus.client_secret}' with actual values.

        Args:
            config: Configuration value (dict, list, str, etc.)

        Returns:
            Configuration with credentials injected
        """
        if isinstance(config, dict):
            return {
                k: self._inject_credentials(v)
                for k, v in config.items()
            }
        elif isinstance(config, list):
            return [self._inject_credentials(item) for item in config]
        elif isinstance(config, str) and config.startswith('${credential:') and config.endswith('}'):
            # Extract credential key
            key = config[13:-1]  # Remove '${credential:' and '}'

            # Get credential
            value = self.credential_manager.get_credential(key)

            if value is None:
                logger.warning(f"Credential not found: {key}")
                return ""  # Return empty string if not found

            return value
        else:
            return config

    def save(self, config: Dict[str, Any], save_path: Optional[Path] = None) -> bool:
        """Save configuration to YAML file.

        Args:
            config: Configuration dictionary
            save_path: Path to save (default: self.config_path)

        Returns:
            True if successful, False otherwise
        """
        if save_path is None:
            save_path = self.config_path

        try:
            # Create parent directory if it doesn't exist
            save_path.parent.mkdir(parents=True, exist_ok=True)

            # Save YAML
            with open(save_path, 'w') as f:
                yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

            logger.info(f"Saved configuration to {save_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            return False

    def update_sources(self, sources: list[Dict[str, Any]]) -> bool:
        """Update sources in configuration.

        Args:
            sources: List of source configurations

        Returns:
            True if successful, False otherwise
        """
        try:
            config = self.load(inject_credentials=False)
            config['sources'] = sources
            return self.save(config)
        except Exception as e:
            logger.error(f"Failed to update sources: {e}")
            return False

    def update_zerobus_config(self, zerobus_config: Dict[str, Any]) -> bool:
        """Update ZeroBus configuration.

        Args:
            zerobus_config: ZeroBus configuration dict

        Returns:
            True if successful, False otherwise
        """
        try:
            config = self.load(inject_credentials=False)
            config['zerobus'] = {**config.get('zerobus', {}), **zerobus_config}
            return self.save(config)
        except Exception as e:
            logger.error(f"Failed to update ZeroBus config: {e}")
            return False
