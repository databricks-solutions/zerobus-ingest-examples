"""Shared state for the REST and gRPC listeners: tables, tokens, faults, TLS."""

from __future__ import annotations

import itertools
import threading
from typing import Any, Dict, Iterable, List, Optional

from .auth import TokenStore
from .certs import TlsFiles, ensure_tls
from .config import MockConfig
from .faults import FaultRegistry, Firing
from .schema import Column, SchemaError, SchemaSnapshot, make_column
from .tables import TableStore


class MockState:
    def __init__(self, cfg: MockConfig):
        self.cfg = cfg
        self.tables = TableStore(cfg.tables)
        self.registry = FaultRegistry(
            seed=cfg.fault_seed,
            event_log_path=cfg.event_log,
        )
        self.tokens = TokenStore(cfg.auth.token_ttl_seconds)
        self._stream_ids = itertools.count(1)
        self.persistent: Dict[str, Dict[str, Any]] = {}  # persistent stream id -> committed state
        self._lock = threading.Lock()
        self.tls: TlsFiles = ensure_tls(cfg.cert_dir, cfg.server.public_hosts)
        for spec in cfg.faults:
            self.registry.add(spec)

    # ------------------------------------------------------------ helpers

    def next_stream_id(self) -> str:
        with self._lock:
            return f"mock-stream-{next(self._stream_ids)}"

    def known_client(self, client_id: str, secret: str) -> bool:
        if not self.cfg.auth.require_known_clients:
            return bool(client_id) and bool(secret)
        return (client_id, secret) in self.cfg.auth.clients

    def apply_schema_change(self, table: str, specs: Iterable[Dict[str, Any]]) -> SchemaSnapshot:
        columns: List[Column] = []
        for spec in specs:
            if not isinstance(spec, dict):
                raise SchemaError("add_columns entries must be objects")
            columns.append(make_column(spec.get("name", ""), spec.get("type", ""), spec.get("nullable", True)))
        return self.tables.add_columns(table, columns)

    def log_firing(self, firing: Firing, action: str, detail: str = "") -> None:
        self.registry.log(firing, action, detail)

    def reset(self, tables: Optional[List[str]] = None, *, faults: bool = True, schema: bool = False) -> None:
        self.tables.reset(tables, schema=schema)
        if faults:
            self.registry.clear()
            self.registry.clear_events()
        self.tokens.clear()
        with self._lock:
            self.persistent.clear()
