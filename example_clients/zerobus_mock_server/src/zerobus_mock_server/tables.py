"""In-memory tables. A stream keeps its schema; a non-nullable column added later sends that stream's rows to the fallback bucket."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .config import TableConfig
from .schema import Column, SchemaError, jsonable


@dataclass(frozen=True)
class Snapshot:
    version: int
    columns: tuple


@dataclass(frozen=True)
class Stored:
    seq: int
    destination: str  # "table" or "fallback"
    stream_id: Optional[str]  # None for REST
    protocol: str  # "rest", "grpc" (JSON, protobuf, Avro) or "arrow"
    offset: Optional[int]
    values: Dict[str, Any]

    def as_json(self) -> Dict[str, Any]:
        return {"seq": self.seq, "destination": self.destination, "stream_id": self.stream_id,
                "protocol": self.protocol, "offset": self.offset, "values": jsonable(self.values)}


def _seq_of(value: Any) -> Optional[int]:
    """A row's key as a number: an integer as it is, a string by its last run of digits."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    found = re.findall(r"\d+", value) if isinstance(value, str) else []
    return int(found[-1]) if found else None


class TableStore:
    def __init__(self, configs: Iterable[TableConfig]) -> None:
        self._config = {t.name: tuple(t.columns) for t in configs}
        self._columns = dict(self._config)
        self._version = {name: 1 for name in self._config}
        self._records: Dict[str, List[Stored]] = {name: [] for name in self._config}
        self._seq = 0

    def names(self) -> List[str]:
        return sorted(self._config)

    def has(self, name: str) -> bool:
        return name in self._config

    def snapshot(self, name: str) -> Snapshot:
        return Snapshot(self._version[name], self._columns[name])

    def add_columns(self, name: str, columns: Sequence[Column]) -> bool:
        """Adds the columns the table doesn't have yet. Returns True when it changed."""
        existing = {c.name for c in self._columns[name]}
        new = tuple(c for c in columns if c.name not in existing)
        if new:
            self._columns[name] = self._columns[name] + new
            self._version[name] += 1
        return bool(new)

    def append(self, name: str, snap: Snapshot, rows: Sequence[Dict[str, Any]], *,
               protocol: str, stream_id: Optional[str], offset: Optional[int] = None) -> None:
        """Stores validated rows that were written under ``snap``."""
        current = self._columns[name]
        snap_names = {c.name for c in snap.columns}
        breaking = snap.version != self._version[name] and any(
            not c.nullable and c.name not in snap_names for c in current)
        for row in rows:
            self._seq += 1
            values = dict(row) if breaking else {c.name: row.get(c.name) for c in current}
            self._records[name].append(Stored(self._seq, "fallback" if breaking else "table",
                                              stream_id, protocol, offset, values))

    def read(self, name: str, destination: str = "table", limit: int = 1000) -> List[Stored]:
        recs = [r for r in self._records[name] if destination == "all" or r.destination == destination]
        return recs[:limit]

    def rows(self, name: str, destination: str = "table") -> List[Dict[str, Any]]:
        """The stored values, in write order (Python-native types)."""
        return [dict(r.values) for r in self.read(name, destination, limit=10**9)]

    def coverage(self, name: str, expected: int, key: str) -> Dict[str, Any]:
        """Missing and duplicate counts for the keys 0..expected-1, read from column ``key``."""
        if key not in {c.name for c in self._columns[name]}:
            raise SchemaError(f"{name} has no column {key}")
        recs = self.read(name, "table", limit=10**9)
        seqs = [_seq_of(r.values.get(key)) for r in recs]
        counts = Counter(s for s in seqs if s is not None and 0 <= s < expected)
        missing = [i for i in range(expected) if not counts.get(i)]
        return {"table": name, "key": key, "expected": expected, "rows": len(recs),
                "fallback_rows": len(self.read(name, "fallback", limit=10**9)), "present": len(counts),
                "missing": len(missing), "missing_keys": missing[:100],
                "duplicate_rows": sum(n - 1 for n in counts.values() if n > 1),
                "unexpected_rows": sum(1 for s in seqs if s is None or not 0 <= s < expected)}

    def reset(self, names: Optional[Iterable[str]] = None, *, schema: bool = False) -> None:
        for name in list(names) if names else self.names():
            self._records[name] = []
            if schema:
                self._columns[name] = self._config[name]
                self._version[name] = 1
