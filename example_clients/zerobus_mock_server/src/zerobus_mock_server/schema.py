"""Columns and the record check. A column kind is the base of its type; values are checked by basic type only."""

from __future__ import annotations

import base64
import datetime as dt
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, List, Sequence, Tuple

KINDS = {"STRING", "BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "BINARY", "DECIMAL",
         "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "VARIANT", "ARRAY", "MAP", "STRUCT"}
INTEGRAL = {"BYTE", "SHORT", "INT", "LONG"}
FLOATING = {"FLOAT", "DOUBLE"}
PY_TYPES = {"STRING": (str,), "BOOLEAN": (bool,), "BINARY": (bytes, str), "DECIMAL": (str, int, float, Decimal),
            "DATE": (int, str, dt.date), "TIMESTAMP": (int, str, dt.datetime),
            "TIMESTAMP_NTZ": (int, str, dt.datetime), "ARRAY": (list,), "MAP": (dict,), "STRUCT": (dict,)}
NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

FIELD_NOT_IN_TABLE = "FIELD_NOT_IN_TABLE"  # the SDK's cause tokens
TYPE_INCOMPATIBLE = "TYPE_INCOMPATIBLE"
MISSING_REQUIRED_COLUMN = "MISSING_REQUIRED_COLUMN"


class SchemaError(ValueError):
    pass


@dataclass(frozen=True)
class Column:
    name: str
    kind: str
    nullable: bool = True


@dataclass(frozen=True)
class Cause:
    token: str
    message: str


class RecordRejected(Exception):
    def __init__(self, causes: List[Cause]):
        self.causes = causes
        super().__init__("; ".join(f"{c.message} [{c.token}]" for c in causes))


def make_column(name: str, type_text: str, nullable: bool = True) -> Column:
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise SchemaError(f"column name {name!r} must be a plain identifier")
    m = re.match(r"\s*([A-Za-z_]+)", str(type_text or ""))
    word = m.group(1).upper() if m else ""
    if word not in KINDS:
        raise SchemaError(f"unknown type {type_text!r} for column {name}")
    return Column(name, word, bool(nullable))


def fits(kind: str, value: Any) -> bool:
    """True when a non-null value has the basic type of the kind."""
    if value is None or kind == "VARIANT":
        return True
    if kind in INTEGRAL | FLOATING:
        return isinstance(value, (int, float) if kind in FLOATING else int) and not isinstance(value, bool)
    return isinstance(value, PY_TYPES.get(kind, ()))


def _missing(columns: Sequence[Column], present) -> List[Cause]:
    return [Cause(MISSING_REQUIRED_COLUMN, f"field '{c.name}' is required (non-nullable) but missing")
            for c in columns if not c.nullable and c.name not in present]


def check_fields(columns: Sequence[Column], table: str, fields: Iterable[Tuple[str, str]]) -> List[Cause]:
    """Schema check for a stream or batch that names its fields up front (protobuf, Avro, Arrow)."""
    by_name = {c.name: c for c in columns}
    causes: List[Cause] = []
    seen = set()
    for name, kind in fields:
        seen.add(name)
        col = by_name.get(name)
        if col is None:
            causes.append(Cause(FIELD_NOT_IN_TABLE, f"field '{name}' is not a column of {table}"))
        elif kind != col.kind:
            causes.append(Cause(TYPE_INCOMPATIBLE, f"field '{name}' is {kind} but the column is {col.kind}"))
    return causes + _missing(columns, seen)


def validate_record(columns: Sequence[Column], table: str, obj: dict) -> dict:
    """The row to store for one record, keyed by column name. A null counts as an absent field."""
    by_name = {c.name: c for c in columns}
    causes: List[Cause] = []
    for key, value in obj.items():
        if value is None:
            continue
        col = by_name.get(key)
        if col is None:
            causes.append(Cause(FIELD_NOT_IN_TABLE, f"field '{key}' is not a column of {table}"))
        elif not fits(col.kind, value):
            causes.append(Cause(TYPE_INCOMPATIBLE, f"field '{key}' is {type(value).__name__}, the column is {col.kind}"))
    causes += _missing(columns, {c.name for c in columns if obj.get(c.name) is not None})
    if causes:
        raise RecordRejected(causes)
    return {c.name: obj.get(c.name) for c in columns}


def jsonable(value: Any) -> Any:
    """A stored value as plain JSON, for the control API."""
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return value
