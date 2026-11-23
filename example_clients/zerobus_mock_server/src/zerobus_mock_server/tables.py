"""In-memory table store.

Each configured table keeps an ordered list of stored records. A stored record
holds the values, the stream and offset that wrote it, a receive timestamp, and
the schema version it was validated against. Rows are never persisted; a
restart starts empty (see README, differences from the real service).

Schema changes follow the docs. A stream keeps the schema snapshot it was
created with. When a stream writes after the table changed:

* if the change is only added nullable columns, the row is stored with NULL in
  the new columns (the docs' non-breaking case);
* if a non-nullable column was added, the stream's rows no longer fit the
  table. They go to the fallback bucket (the in-memory counterpart of the docs'
  ``_zerobus/table_rejected_parquets/`` location) and are not in the table.
"""

from __future__ import annotations

import datetime as _dt
import threading
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .schema import Column, SchemaSnapshot, to_json_value


def _utc_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class TableConfig:
    name: str
    columns: Sequence[Column]
    rescue_column: Optional[str] = None


@dataclass(frozen=True)
class StoredRecord:
    seq: int
    destination: str  # "table" or "fallback"
    stream_id: Optional[str]
    protocol: str  # "rest" or "grpc"
    offset: Optional[int]
    received_at: str
    schema_version: int
    values: Dict[str, Any]
    reason: Optional[str] = None  # fallback reason

    def as_json(self) -> Dict[str, Any]:
        return {
            "seq": self.seq,
            "destination": self.destination,
            "stream_id": self.stream_id,
            "protocol": self.protocol,
            "offset": self.offset,
            "received_at": self.received_at,
            "schema_version": self.schema_version,
            "reason": self.reason,
            "values": {k: to_json_value(v) for k, v in self.values.items()},
        }


@dataclass
class _TableState:
    config: TableConfig
    columns: tuple
    version: int = 1
    rescue_column: Optional[str] = None
    records: List[StoredRecord] = field(default_factory=list)
    rejections: Counter = field(default_factory=Counter)



class TableHandle:
    """Python handle for one table, for in-process tests: ``mock.tables["cat.sch.t"].rows``."""

    def __init__(self, store: "TableStore", name: str):
        self._store = store
        self.name = name

    @property
    def rows(self) -> List[Dict[str, Any]]:
        """Values of the rows in the table, in write order (Python-native types)."""
        return self._store.values(self.name, "table")

    @property
    def fallback(self) -> List[Dict[str, Any]]:
        """Values of rows routed to the fallback bucket (breaking schema change)."""
        return self._store.values(self.name, "fallback")

    @property
    def records(self) -> List[StoredRecord]:
        """Every stored record with its stream, offset and receive time."""
        return self._store.read(self.name, destination="all", limit=10**9)

    def stats(self, key: Optional[str] = None) -> Dict[str, Any]:
        return self._store.stats(self.name, key=key)

    def reset(self, *, schema: bool = False) -> None:
        self._store.reset([self.name], schema=schema)

    def __len__(self) -> int:
        return len(self.rows)


class TableStore:
    def __init__(self, configs: Iterable[TableConfig]):
        self._lock = threading.RLock()
        self._seq = 0
        self._tables: Dict[str, _TableState] = {}
        for cfg in configs:
            self._add(cfg)

    def __getitem__(self, name: str) -> "TableHandle":
        if name not in self._tables:
            raise KeyError(name)
        return TableHandle(self, name)

    def _add(self, cfg: TableConfig) -> None:
        if cfg.name in self._tables:
            raise ValueError(f"table {cfg.name} is configured twice")
        self._tables[cfg.name] = _TableState(
            config=cfg,
            columns=tuple(cfg.columns),
            rescue_column=cfg.rescue_column,
        )

    # ------------------------------------------------------------ schema

    def names(self) -> List[str]:
        return sorted(self._tables)

    def has(self, name: str) -> bool:
        return name in self._tables

    def snapshot(self, name: str) -> SchemaSnapshot:
        with self._lock:
            st = self._tables[name]
            return SchemaSnapshot(name, st.version, st.columns, st.rescue_column)

    def add_columns(self, name: str, columns: Sequence[Column]) -> SchemaSnapshot:
        """Add columns to a table. Existing streams keep their snapshot."""
        with self._lock:
            st = self._tables[name]
            existing = {c.name for c in st.columns}
            for col in columns:
                if col.name in existing:
                    raise ValueError(f"column {col.name} already exists in {name}")
                existing.add(col.name)
            st.columns = tuple(st.columns) + tuple(columns)
            st.version += 1
            return self.snapshot(name)

    def describe(self, name: str) -> Dict[str, Any]:
        with self._lock:
            st = self._tables[name]
            return {
                "table": name,
                "schema_version": st.version,
                "rescue_column": st.rescue_column,
                "columns": [
                    {"name": c.name, "type": c.type.describe(), "nullable": c.nullable}
                    for c in st.columns
                ],
                "rows": sum(1 for r in st.records if r.destination == "table"),
                "fallback_rows": sum(1 for r in st.records if r.destination == "fallback"),
            }

    # ------------------------------------------------------------ writes

    def append(
        self,
        name: str,
        snap: SchemaSnapshot,
        rows: Sequence[Dict[str, Any]],
        *,
        protocol: str,
        stream_id: Optional[str],
        offsets: Optional[Sequence[int]] = None,
    ) -> List[StoredRecord]:
        """Store validated rows that were written under ``snap``, in order."""
        stored: List[StoredRecord] = []
        with self._lock:
            st = self._tables[name]
            now = _utc_now()
            breaking = self._breaking_reason(st, snap)
            for i, row in enumerate(rows):
                self._seq += 1
                offset = offsets[i] if offsets is not None else None
                if breaking:
                    rec = StoredRecord(
                        self._seq, "fallback", stream_id, protocol, offset, now,
                        snap.version, dict(row), breaking,
                    )
                else:
                    values = {c.name: row.get(c.name) for c in st.columns}
                    rec = StoredRecord(
                        self._seq, "table", stream_id, protocol, offset, now,
                        snap.version, values,
                    )
                st.records.append(rec)
                stored.append(rec)
        return stored

    @staticmethod
    def _breaking_reason(st: _TableState, snap: SchemaSnapshot) -> Optional[str]:
        if snap.version == st.version:
            return None
        snap_names = {c.name for c in snap.columns}
        for col in st.columns:
            if col.name not in snap_names and not col.nullable:
                return (
                    f"table schema v{st.version} added non-nullable column {col.name}; "
                    f"stream schema v{snap.version} can't supply it"
                )
        return None

    def record_rejection(self, name: str, tokens: Iterable[str]) -> None:
        with self._lock:
            if name in self._tables:
                for tok in tokens:
                    self._tables[name].rejections[tok] += 1

    # ------------------------------------------------------------ reads

    def read(
        self,
        name: str,
        *,
        stream_id: Optional[str] = None,
        offset_min: Optional[int] = None,
        offset_max: Optional[int] = None,
        destination: str = "table",
        since_seq: int = 0,
        limit: int = 1000,
    ) -> List[StoredRecord]:
        with self._lock:
            st = self._tables[name]
            out = []
            for rec in st.records:
                if rec.seq <= since_seq:
                    continue
                if destination != "all" and rec.destination != destination:
                    continue
                if stream_id is not None and rec.stream_id != stream_id:
                    continue
                if offset_min is not None and (rec.offset is None or rec.offset < offset_min):
                    continue
                if offset_max is not None and (rec.offset is None or rec.offset > offset_max):
                    continue
                out.append(rec)
                if len(out) >= limit:
                    break
            return out

    def values(self, name: str, destination: str = "table") -> List[Dict[str, Any]]:
        """Python-native values of every stored record, in write order (test handle)."""
        with self._lock:
            return [
                dict(r.values) for r in self._tables[name].records
                if destination == "all" or r.destination == destination
            ]

    def stats(self, name: str, key: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            st = self._tables[name]
            table_recs = [r for r in st.records if r.destination == "table"]
            fallback = [r for r in st.records if r.destination == "fallback"]
            by_stream = Counter(r.stream_id or "rest" for r in table_recs)
            by_protocol = Counter(r.protocol for r in table_recs)
            out: Dict[str, Any] = {
                "table": name,
                "schema_version": st.version,
                "rows": len(table_recs),
                "fallback_rows": len(fallback),
                "rows_by_stream": dict(by_stream),
                "rows_by_protocol": dict(by_protocol),
                "natural_rejections": dict(st.rejections),
            }
            if key is not None:
                values = [r.values.get(key) for r in table_recs]
                counts = Counter(to_json_value(v) if not isinstance(v, (dict, list)) else repr(v) for v in values)
                distinct = len(counts)
                out["key"] = key
                out["distinct_keys"] = distinct
                out["duplicate_rows"] = len(table_recs) - distinct
                out["duplicate_keys"] = sorted(str(k) for k, n in counts.items() if n > 1)[:100]
            return out

    # ------------------------------------------------------------ reset

    def reset(self, names: Optional[Iterable[str]] = None, *, schema: bool = False) -> None:
        with self._lock:
            targets = list(names) if names else list(self._tables)
            for name in targets:
                st = self._tables[name]
                st.records.clear()
                st.rejections.clear()
                if schema:
                    st.columns = tuple(st.config.columns)
                    st.version = 1
