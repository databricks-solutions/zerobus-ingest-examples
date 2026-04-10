"""
OPC UA Server Simulator — exposes synthetic sensor data matching KEPServerEX tag
structure for the demo pipeline.

Reads tag definitions from tags_config.yaml (same file the client uses).
Each tag is registered with a string-based NodeId identical to what KEPServerEX
would expose, e.g.  ns=2;s=Channel1.Device1.LineA.Temperature

Synthetic data generators vary by declared type:
  float  → sinusoidal drift + noise
  string → stable identifiers that rotate occasionally
  bool   → random toggle every few cycles
"""

import asyncio
import logging
import os
import random
import string
from math import sin, pi
from pathlib import Path

import yaml
from asyncua import Server, ua
from dotenv import load_dotenv

load_dotenv()

OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4840/freeopcua/server/")
UPDATE_INTERVAL = float(os.getenv("OPCUA_POLL_INTERVAL", "5"))
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_DIR = _SCRIPT_DIR.parent
_tags_raw = os.getenv("TAGS_CONFIG_FILE", "tags_config.yaml")
TAGS_CONFIG = str(_PROJECT_DIR / _tags_raw) if not os.path.isabs(_tags_raw) else _tags_raw

NAMESPACE_URI = "http://example.com/opcua/zerobus-demo"

logger = logging.getLogger(__name__)


def load_tags_config(path: str) -> dict:
    """Load tag definitions from YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


_STRING_POOL = {
    "Status": ["Idle", "Running", "Warming", "Cooling", "Maintenance", "Stopped"],
}


def synthetic_float(tick: int, phase: float, tag_name: str) -> float:
    """Generate a realistic float value based on tag semantics."""
    n = tag_name.lower()
    if "temperature" in n or "temp" in n:
        base = 65.0 + 15.0 * sin(2 * pi * tick / 120 + phase)
        return round(base + random.uniform(-0.5, 0.5), 2)
    if "humidity" in n:
        base = 45.0 + 10.0 * sin(2 * pi * tick / 150 + phase)
        return round(max(0, min(100, base + random.uniform(-1, 1))), 2)
    if "pressure" in n:
        base = 1013.25 + 5.0 * sin(2 * pi * tick / 200 + phase)
        return round(base + random.uniform(-0.5, 0.5), 2)
    base = 50.0 + 10.0 * sin(2 * pi * tick / 130 + phase)
    return round(base + random.uniform(-1, 1), 2)


def synthetic_string(tick: int, tag_name: str) -> str:
    """Generate a string value, rotating through a pool if available."""
    pool = _STRING_POOL.get(tag_name)
    if pool:
        idx = (tick // 60) % len(pool)
        return pool[idx]
    return f"{tag_name}-{''.join(random.choices(string.ascii_uppercase, k=4))}"


def synthetic_bool(tick: int, phase: float) -> bool:
    """Generate a boolean value based on sine wave threshold."""
    return sin(2 * pi * tick / 40 + phase) > 0.2


def initial_value(tag_type: str):
    """Return initial value for a tag based on its type."""
    if tag_type == "float":
        return 0.0
    if tag_type == "bool":
        return False
    return ""


async def main():
    config = load_tags_config(TAGS_CONFIG)
    opcua_cfg = config["opcua"]
    device_path = opcua_cfg["device_path"]

    server = Server()
    await server.init()
    server.set_endpoint(OPCUA_URL)
    server.set_server_name("OPC UA Demo Server (Zerobus Example)")

    idx = await server.register_namespace(NAMESPACE_URI)

    tag_vars = []
    tag_index = 0
    for line_name, line_cfg in config["lines"].items():
        for tag_def in line_cfg["tags"]:
            tag_name = tag_def["name"]
            tag_type = tag_def["type"]
            node_str = f"{device_path}.{line_name}.{tag_name}"

            var = await server.nodes.objects.add_variable(
                ua.NodeId(node_str, idx),
                node_str,
                initial_value(tag_type),
            )
            await var.set_writable()

            phase = tag_index * (2 * pi / 59)
            tag_vars.append((line_name, tag_name, tag_type, var, phase))
            tag_index += 1

    total = len(tag_vars)
    lines = sorted({t[0] for t in tag_vars})
    logger.info(
        "Starting OPC UA server on %s  (%d tags across lines: %s, update every %ds)",
        OPCUA_URL,
        total,
        ", ".join(lines),
        UPDATE_INTERVAL,
    )

    tick = 0
    async with server:
        while True:
            for line_name, tag_name, tag_type, var, phase in tag_vars:
                if tag_type == "float":
                    val = synthetic_float(tick, phase, tag_name)
                elif tag_type == "bool":
                    val = synthetic_bool(tick, phase)
                else:
                    val = synthetic_string(tick, tag_name)
                await var.write_value(val)

            if tick % 10 == 0:
                logger.info("Tick %d — updated %d tags", tick, total)

            tick += 1
            await asyncio.sleep(UPDATE_INTERVAL)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )
    asyncio.run(main())
