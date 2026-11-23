"""Zerobus Ingest mock server: a test double for the REST and gRPC write APIs.

It is not the Zerobus service. See the README ("Differences from the real service").
"""

from .config import ConfigError, MockConfig, from_dict, load
from .runner import MockZerobus

__all__ = ["MockZerobus", "MockConfig", "ConfigError", "from_dict", "load"]
__version__ = "0.1.0"
