"""Command line: ``python -m zerobus_mock_server --config config.yaml``."""

import argparse
import sys

from .config import ConfigError, load
from .runner import MockZerobus

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="zerobus_mock_server", description=__doc__)
    parser.add_argument("--config", required=True, help="path to the YAML config file")
    try:
        cfg = load(parser.parse_args().config)
    except ConfigError as exc:
        sys.exit(f"config error: {exc}")
    MockZerobus(cfg).run_forever()
