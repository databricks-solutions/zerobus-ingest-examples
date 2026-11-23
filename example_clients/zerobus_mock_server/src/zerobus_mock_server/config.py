"""Configuration loading and validation (YAML or a Python dict)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from .schema import Column, SchemaError, make_column
from .tables import TableConfig

DEFAULT_MAX_RECORD_BYTES = 10_485_760  # 10 MB record size maximum (docs: quotas, record size)
DEFAULT_HEADER_BYTES = 19  # headers that count against the 10 MB message (docs)
NAME_RE = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


class ConfigError(ValueError):
    pass


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    rest_port: int = 8443
    grpc_port: int = 50051
    public_hosts: List[str] = field(default_factory=lambda: ["localhost"])
    workspace_id: Optional[str] = None  # when set, token requests must name this workspace


@dataclass
class AuthConfig:
    token_ttl_seconds: int = 3600
    require_known_clients: bool = False
    clients: List[Tuple[str, str]] = field(default_factory=list)


@dataclass
class LimitsConfig:
    max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES
    message_header_bytes: int = DEFAULT_HEADER_BYTES

    @property
    def max_payload_bytes(self) -> int:
        return self.max_record_bytes - self.message_header_bytes


@dataclass
class MockConfig:
    server: ServerConfig
    auth: AuthConfig
    limits: LimitsConfig
    tables: List[TableConfig]
    faults: List[Dict[str, Any]]
    fault_seed: int = 0
    cert_dir: Optional[Path] = None
    event_log: Optional[Path] = None


def _get(d: Dict[str, Any], key: str, default: Any = None, *, where: str = "") -> Any:
    if not isinstance(d, dict):
        raise ConfigError(f"{where or 'config'} must be a mapping")
    return d.get(key, default)


def _table(raw: Dict[str, Any]) -> TableConfig:
    name = raw.get("name")
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise ConfigError(f"table name {name!r} must be catalog.schema.table")
    cols_raw = raw.get("columns")
    if not isinstance(cols_raw, list) or not cols_raw:
        raise ConfigError(f"{name}: columns must be a non-empty list")
    columns: List[Column] = []
    try:
        for c in cols_raw:
            columns.append(make_column(c["name"], c["type"], c.get("nullable", True)))
    except (KeyError, SchemaError, TypeError) as exc:
        raise ConfigError(f"{name}: bad column {c!r}: {exc}")
    names = [c.name for c in columns]
    if len(set(names)) != len(names):
        raise ConfigError(f"{name}: duplicate column names")
    # docs: column names must stay unique once underscores are removed (error 4044)
    squashed = [re.sub(r"_([a-z])", lambda m: m.group(1).upper(), n) for n in names]
    if len(set(squashed)) != len(squashed):
        raise ConfigError(f"{name}: two column names collide after underscore folding (docs error 4044)")
    rescue = raw.get("rescue_column")
    if rescue is not None:
        col = next((c for c in columns if c.name == rescue), None)
        if col is None:
            raise ConfigError(f"{name}: rescue_column {rescue!r} is not a column")
        if not col.nullable:
            raise ConfigError(f"{name}: the rescue column must be nullable")
        if col.type.kind not in ("VARIANT", "STRING"):
            raise ConfigError(f"{name}: the rescue column must be VARIANT")
    return TableConfig(name=name, columns=tuple(columns), rescue_column=rescue)


def from_dict(raw: Dict[str, Any], base_dir: Optional[Path] = None) -> MockConfig:
    base = base_dir or Path.cwd()

    def resolve(p: Optional[str], default: str) -> Path:
        path = Path(p or default)
        return path if path.is_absolute() else (base / path).resolve()

    srv = _get(raw, "server", {}, where="server") or {}
    server = ServerConfig(
        host=srv.get("host", "127.0.0.1"),
        rest_port=int(srv.get("rest_port", 8443)),
        grpc_port=int(srv.get("grpc_port", 50051)),
        public_hosts=list(srv.get("public_hosts", ["localhost"])),
        workspace_id=srv.get("workspace_id"),
    )
    if not server.public_hosts:
        raise ConfigError("server.public_hosts needs at least one host name for the certificate")

    au = _get(raw, "auth", {}, where="auth") or {}
    clients = []
    for c in au.get("clients", []) or []:
        clients.append((str(c["client_id"]), str(c["client_secret"])))
    auth = AuthConfig(
        token_ttl_seconds=int(au.get("token_ttl_seconds", 3600)),
        require_known_clients=bool(au.get("require_known_clients", False)),
        clients=clients,
    )
    if auth.require_known_clients and not clients:
        raise ConfigError("auth.require_known_clients is true but no auth.clients are listed")

    lim = _get(raw, "limits", {}, where="limits") or {}
    limits = LimitsConfig(
        max_record_bytes=int(lim.get("max_record_bytes", DEFAULT_MAX_RECORD_BYTES)),
        message_header_bytes=int(lim.get("message_header_bytes", DEFAULT_HEADER_BYTES)),
    )
    if limits.max_record_bytes <= limits.message_header_bytes:
        raise ConfigError("limits.max_record_bytes must exceed message_header_bytes")

    tables_raw = raw.get("tables") or []
    if not isinstance(tables_raw, list) or not tables_raw:
        raise ConfigError("tables must list at least one table")
    tables = [_table(t) for t in tables_raw]
    if len({t.name for t in tables}) != len(tables):
        raise ConfigError("table names must be unique")

    faults = raw.get("faults") or []
    if not isinstance(faults, list):
        raise ConfigError("faults must be a list")

    tls = _get(raw, "tls", {}, where="tls") or {}
    cert_dir = resolve(tls.get("cert_dir"), "./data/tls")
    event_log_raw = raw.get("event_log")
    event_log = resolve(event_log_raw, "./data/events.jsonl") if event_log_raw else None

    return MockConfig(
        server=server,
        auth=auth,
        limits=limits,
        tables=tables,
        faults=faults,
        fault_seed=int(raw.get("fault_seed", 0)),
        cert_dir=cert_dir,
        event_log=event_log,
    )


def load(path: Path) -> MockConfig:
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"cannot read {path}: {exc}")
    return from_dict(raw, base_dir=path.parent)
