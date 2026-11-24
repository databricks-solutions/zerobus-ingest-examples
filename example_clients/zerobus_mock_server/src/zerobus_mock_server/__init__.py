"""Zerobus Ingest mock server: a test double for the REST and gRPC write APIs (not the Zerobus service).

UNOFFICIAL: a community test tool. Not affiliated with Databricks.
"""

import json
from pathlib import Path

from .config import ConfigError, MockConfig, from_dict, load
from .runner import MockZerobus

__version__ = "0.1.0"
# The contract this server was built against: its copy of the shared scenarios.json.
CONTRACT_VERSION = json.loads(Path(__file__).with_name("scenarios.json").read_text(encoding="utf-8"))["contract_version"]

__all__ = ["MockZerobus", "MockConfig", "ConfigError", "from_dict", "load", "__version__", "CONTRACT_VERSION"]
