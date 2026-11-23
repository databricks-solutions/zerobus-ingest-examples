#!/usr/bin/env python3
"""Contract check: each package's declared contract_version must match mock_lib/scenarios.json.

UNOFFICIAL: a community test tool. Not affiliated with Databricks.

Standard library only, so it runs without installing anything:

    python3 -I mock_lib/check_contract_version.py            # the server folder is the parent of mock_lib/

It checks that the two scenarios.json copies are byte-identical, that the server reads its copy's
contract_version, and that each library declares the same contract_version. Exit 0 when all match, 1 otherwise.
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent  # the server folder
SHARED = ROOT / "mock_lib" / "scenarios.json"
SERVER_COPY = ROOT / "src" / "zerobus_mock_server" / "scenarios.json"

# Each artifact declares the contract it embeds. The pattern pulls the declared integer out of the file.
DECLARATIONS = [
    ("python lib", "mock_lib/python/zerobus_mock_lib/__init__.py", r"^CONTRACT_VERSION\s*=\s*(\d+)", re.M),
    ("typescript lib", "mock_lib/typescript/package.json", r'"contract_version"\s*:\s*(\d+)', 0),
    ("rust lib", "mock_lib/rust/Cargo.toml", r"^contract_version\s*=\s*(\d+)", re.M),
    ("java lib", "mock_lib/java/pom.xml", r"<zerobus\.mock\.contract_version>\s*(\d+)\s*<", 0),
    ("go lib", "mock_lib/go/zerobusmock.go", r"^const ContractVersion\s*=\s*(\d+)", re.M),
]


def main():
    failures = []

    contract = json.loads(SHARED.read_text(encoding="utf-8")).get("contract_version")
    if not isinstance(contract, int):
        print("FAIL scenarios.json has no integer contract_version")
        return 1
    print("contract_version in mock_lib/scenarios.json: %d" % contract)

    if not SERVER_COPY.exists():
        failures.append("server copy missing: %s" % SERVER_COPY)
    elif SERVER_COPY.read_bytes() != SHARED.read_bytes():
        failures.append("server scenarios.json differs from mock_lib/scenarios.json")
    else:
        print("ok   server scenarios.json is byte-identical to the shared copy")

    init = ROOT / "src" / "zerobus_mock_server" / "__init__.py"
    init_text = init.read_text(encoding="utf-8") if init.exists() else ""
    if re.search(r"^CONTRACT_VERSION\s*=.*scenarios\.json", init_text, re.M) and "contract_version" in init_text:
        print("ok   server reads CONTRACT_VERSION from its scenarios.json copy")
    else:
        failures.append("server __init__.py does not read CONTRACT_VERSION from scenarios.json")

    for label, rel, pattern, flags in DECLARATIONS:
        path = ROOT / rel
        if not path.exists():
            failures.append("%s: file missing (%s)" % (label, rel))
            continue
        found = re.findall(pattern, path.read_text(encoding="utf-8"), flags)
        if len(found) != 1:
            failures.append("%s: expected one contract_version declaration in %s, found %d" % (label, rel, len(found)))
            continue
        declared = int(found[0])
        if declared != contract:
            failures.append("%s: contract_version %d in %s, scenarios.json has %d" % (label, declared, rel, contract))
        else:
            print("ok   %s declares contract_version %d" % (label, declared))

    for line in failures:
        print("FAIL " + line)
    if failures:
        return 1
    print("PASS all declared contract versions match scenarios.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
