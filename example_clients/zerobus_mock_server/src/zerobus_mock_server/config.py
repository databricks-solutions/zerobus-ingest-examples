"""Configuration: ports, TLS directory, token lifetime, the record limit and the tables (YAML or dict)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .schema import make_column

DEFAULT_MAX_RECORD_BYTES = 10_485_760  # 10 MB record size maximum (docs: quotas, record size)
MESSAGE_HEADER_BYTES = 19  # header bytes that count against that maximum (docs)
TABLE_RE = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class TableConfig:
    name: str
    columns: tuple  # of Column


@dataclass
class MockConfig:
    host: str = "127.0.0.1"
    rest_port: int = 8443
    grpc_port: int = 50051
    public_hosts: List[str] = field(default_factory=lambda: ["localhost"])
    cert_dir: Path = Path("./data/tls")
    max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES
    tables: List[TableConfig] = field(default_factory=list)

    @property
    def max_payload_bytes(self) -> int:
        return self.max_record_bytes - MESSAGE_HEADER_BYTES


def _table(raw: Dict[str, Any]) -> TableConfig:
    name, cols = raw.get("name"), raw.get("columns")
    if not isinstance(name, str) or not TABLE_RE.match(name):
        raise ConfigError(f"table name {name!r} must be catalog.schema.table")
    if not isinstance(cols, list) or not cols:
        raise ConfigError(f"{name}: columns must be a non-empty list")
    try:
        columns = tuple(make_column(c["name"], c["type"], c.get("nullable", True)) for c in cols)
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigError(f"{name}: bad column: {exc}")
    if len({c.name for c in columns}) != len(columns):
        raise ConfigError(f"{name}: duplicate column names")
    return TableConfig(name, columns)


def from_dict(raw: Optional[Dict[str, Any]], base_dir: Optional[Path] = None) -> MockConfig:
    raw = raw or {}
    base = base_dir or Path.cwd()
    srv, tls = raw.get("server") or {}, raw.get("tls") or {}
    tables = [_table(t) for t in raw.get("tables") or []]
    if not tables or len({t.name for t in tables}) != len(tables):
        raise ConfigError("tables must list at least one table, with unique names")
    hosts = list(srv.get("public_hosts", ["localhost"]))
    if not hosts:
        raise ConfigError("server.public_hosts needs at least one host name for the certificate")
    cert_dir = Path(tls.get("cert_dir", "./data/tls"))
    cfg = MockConfig(
        host=srv.get("host", "127.0.0.1"),
        rest_port=int(srv.get("rest_port", 8443)),
        grpc_port=int(srv.get("grpc_port", 50051)),
        public_hosts=hosts,
        cert_dir=cert_dir if cert_dir.is_absolute() else (base / cert_dir).resolve(),
        max_record_bytes=int((raw.get("limits") or {}).get("max_record_bytes", DEFAULT_MAX_RECORD_BYTES)),
        tables=tables,
    )
    if cfg.max_record_bytes <= MESSAGE_HEADER_BYTES:
        raise ConfigError("limits.max_record_bytes must exceed the message header size")
    return cfg


def load(path: Path) -> MockConfig:
    path = Path(path)
    return from_dict(yaml.safe_load(path.read_text(encoding="utf-8")), base_dir=path.parent)
