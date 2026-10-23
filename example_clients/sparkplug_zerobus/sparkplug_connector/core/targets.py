"""A source's Zerobus target table.

A source's table: its own ``target_table`` (``catalog.schema.table``), else ``zerobus.source_targets.<name>``
(merged over the default), else ``zerobus.default_target``.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Tuple

_PART = re.compile(r"[^.\s`]+")


def parse_target_table(text: Any) -> Tuple[str, str, str]:
    """``catalog.schema.table`` as its three parts; raises ``ValueError`` otherwise."""
    parts = str(text).strip().split(".")
    if len(parts) != 3 or not all(_PART.fullmatch(p) for p in parts):
        raise ValueError(f"target table must be catalog.schema.table: {text}")
    return parts[0], parts[1], parts[2]


def target_config(source: Dict[str, Any], zerobus_config: Dict[str, Any]) -> Dict[str, Any]:
    """The target (workspace, catalog, schema, table) a source writes to."""
    default = zerobus_config.get("default_target", {}) or {}
    own = str(source.get("target_table") or "").strip()
    if own:
        catalog, schema, table = parse_target_table(own)
        return {**default, "catalog": catalog, "schema": schema, "table": table}
    override = (zerobus_config.get("source_targets") or {}).get(source.get("name"))
    return {**default, **override} if override else default


def table_name(target: Dict[str, Any]) -> str:
    return f"{target.get('catalog', 'main')}.{target.get('schema', 'iot_data')}.{target.get('table', 'sensor_readings')}"
