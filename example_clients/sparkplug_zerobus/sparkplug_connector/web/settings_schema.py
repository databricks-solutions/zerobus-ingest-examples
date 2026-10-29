"""The settings the web page's Settings page edits (``GET/PUT /api/settings``): the connector-wide ones that have no
page of their own, each a dotted path in the configuration file. They are read when the connector starts, so a
change takes effect when the bridge restarts (``POST /api/bridge/restart``, the page's Save and restart). Sources
and Zerobus have their own pages (and APIs).
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any

from sparkplug_connector.core.cert_store import DEFAULT_DIR


@dataclass(frozen=True)
class Setting:
    key: str                                 # dotted path in the configuration
    section: str
    label: str
    type: str                                # BOOLEAN, STRING, INTEGER, NUMBER, ENUM
    defaultValue: Any = None
    help: str = ""
    unit: str = ""
    min: float | None = None
    options: list[str] = field(default_factory=list)


DIAGNOSTICS, QUEUE, WAL, WEB = "Diagnostics", "Queue and writes", "Write-ahead log", "Web page"
SETTINGS = [
    Setting("diagnostics.enabled", DIAGNOSTICS, "Diagnostics", "BOOLEAN", False,
            "Write the connector's own edge nodes (bridge, edge nodes, MQTT clients) into the default table."),
    Setting("diagnostics.group_id", DIAGNOSTICS, "Group ID", "STRING", "BRIDGE",
            "Their Sparkplug group: the NBIRTH procedure's schemas are <prefix>_<group>_<edge node>."),
    Setting("diagnostics.bridge_model", DIAGNOSTICS, "Bridge model", "STRING", "BRIDGE",
            "The bridge's template and device, its instance Core/<model> and its metric \"<model> Version\"."),
    Setting("diagnostics.bridge_info_edge_node_id", DIAGNOSTICS, "Bridge info edge node", "STRING", "Bridge Info"),
    Setting("diagnostics.edge_node_info_edge_node_id", DIAGNOSTICS, "Edge node info edge node", "STRING", "Edge Node Info"),
    Setting("diagnostics.mqtt_client_info_edge_node_id", DIAGNOSTICS, "MQTT client info edge node", "STRING",
            "MQTT Client Info"),
    Setting("backpressure.memory_queue.max_size", QUEUE, "Queue size", "INTEGER", 10000,
            "Rows queued per table; a primary host goes OFFLINE at 80% full.", "rows", 1),
    Setting("backpressure.memory_queue.drop_policy", QUEUE, "When the queue is full", "ENUM", "oldest",
            "Drop the oldest row, the newest row, or refuse it.", options=["oldest", "newest", "reject"]),
    Setting("zerobus.batch.max_records", QUEUE, "Batch size", "INTEGER", 500, "Rows per Zerobus write.", "rows", 1),
    Setting("zerobus.batch.linger_seconds", QUEUE, "Batch linger", "NUMBER", 0.2,
            "How long a batch waits to fill.", "s", 0),
    Setting("zerobus.drain_timeout_seconds", QUEUE, "Drain on stop", "NUMBER", 15,
            "On shutdown, how long the queued rows may take to be written.", "s", 0),
    Setting("zerobus.wal.enabled", WAL, "Write-ahead log", "BOOLEAN", False,
            "Rows on disk before they are queued: they survive a crash or a cut-short stop."),
    Setting("zerobus.wal.path", WAL, "Directory", "STRING", "wal", "One subdirectory per table."),
    Setting("zerobus.wal.max_size_mb", WAL, "Largest size", "NUMBER", 1024, "Per table.", "MB", 1),
    Setting("zerobus.wal.fsync_interval_seconds", WAL, "Fsync interval", "NUMBER", 0.2,
            "A power loss loses at most this much.", "s", 0),
    Setting("web_ui.certs_dir", WEB, "Certificates directory", "STRING", DEFAULT_DIR,
            "Where the Certificates page keeps uploads; a source's TLS file given by its bare name is read from here."),
]
BY_KEY = {s.key: s for s in SETTINGS}


def schema() -> dict[str, Any]:
    sections: dict[str, list[dict]] = {}
    for s in SETTINGS:
        sections.setdefault(s.section, []).append(asdict(s))
    return {"sections": [{"name": name, "fields": fields} for name, fields in sections.items()]}


def _get(config: dict[str, Any], key: str) -> Any:
    node: Any = config
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def values(config: dict[str, Any]) -> dict[str, Any]:
    """Each setting's value as written in the file (None: not set, the default applies)."""
    return {s.key: _get(config, s.key) for s in SETTINGS}


def _parse(s: Setting, value: Any) -> Any:
    """The value to store (None: remove it); ValueError with the message for the field."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, str) and value.strip().startswith("${"):
        return value.strip()                 # a placeholder (${env:...}): kept as written
    if s.type == "BOOLEAN":
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in ("true", "false"):
            return text == "true"
        raise ValueError("true or false")
    if s.type in ("INTEGER", "NUMBER"):
        try:
            number = float(value)
        except (TypeError, ValueError):
            raise ValueError("a number") from None
        if not math.isfinite(number):            # float() takes "inf" and "nan"
            raise ValueError("a number")
        if s.type == "INTEGER":
            if number != int(number):
                raise ValueError("a whole number")
            number = int(number)
        if s.min is not None and number < s.min:
            raise ValueError(f"at least {s.min:g}")
        return number
    if s.type == "ENUM":
        text = str(value).strip()
        if text not in s.options:
            raise ValueError("one of " + ", ".join(s.options))
        return text
    return str(value).strip()


def apply(config: dict[str, Any], changes: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """``config`` (the file's, placeholders kept) with ``changes`` (key -> value; blank: back to the default) applied,
    and the errors by key (nothing applied when there is one)."""
    errors: dict[str, str] = {}
    parsed: dict[str, Any] = {}
    for key, value in (changes or {}).items():
        s = BY_KEY.get(key)
        if s is None:
            errors[key] = "not a setting"
            continue
        try:
            parsed[key] = _parse(s, value)
        except ValueError as e:
            errors[key] = f"Must be {e}"
    if errors:
        return config, errors
    for key, value in parsed.items():
        parts = key.split(".")
        node = config
        for part in parts[:-1]:
            if not isinstance(node.get(part), dict):
                node[part] = {}
            node = node[part]
        if value is None:
            node.pop(parts[-1], None)
        else:
            node[parts[-1]] = value
    return config, {}
