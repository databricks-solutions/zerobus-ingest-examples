"""The mock's state: tables, tokens, scenarios and TLS files. Both listeners read and write it."""

from __future__ import annotations

import itertools
from typing import Iterable, Optional

from .auth import TokenStore
from .certs import ensure_tls
from .config import MockConfig
from .scenarios import SCHEMA_CHANGE, ScenarioManager
from .tables import TableStore


class MockState:
    def __init__(self, cfg: MockConfig) -> None:
        self.cfg = cfg
        self.tables = TableStore(cfg.tables)
        self.tokens = TokenStore()
        self.scenarios = ScenarioManager(self.tokens)
        self.tls = ensure_tls(cfg.cert_dir, cfg.public_hosts)
        self._stream_ids = itertools.count(1)

    def next_stream_id(self) -> str:
        return f"mock-stream-{next(self._stream_ids)}"

    def add_schema_change(self, table: str) -> None:
        self.tables.add_columns(table, SCHEMA_CHANGE)

    def reset(self, tables: Optional[Iterable[str]] = None, *, schema: bool = False) -> None:
        self.tables.reset(tables, schema=schema)
        self.scenarios.reset()
        self.tokens.clear()
