"""Command line entry point: ``python -m zerobus_mock_server --config config.yaml``."""

from __future__ import annotations

import argparse
import logging
import sys

from . import config as config_mod
from .runner import MockZerobus


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="zerobus_mock_server", description=__doc__)
    parser.add_argument("--config", required=True, help="path to the YAML config file")
    parser.add_argument("--check-config", action="store_true", help="validate the config and exit")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        cfg = config_mod.load(args.config)
    except config_mod.ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    if args.check_config:
        print(f"config ok: {len(cfg.tables)} table(s), {len(cfg.faults)} startup fault(s)")
        return 0
    MockZerobus(cfg).run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
