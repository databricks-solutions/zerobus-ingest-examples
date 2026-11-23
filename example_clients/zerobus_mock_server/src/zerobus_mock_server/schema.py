"""Table schema model: type-string parsing, value coercion, and JSON rendering.

Type names follow the Zerobus docs (Delta type names: STRING, INT, LONG, SHORT,
BYTE, FLOAT, DOUBLE, BOOLEAN, BINARY, DECIMAL(p,s), DATE, TIMESTAMP,
TIMESTAMP_NTZ, VARIANT, ARRAY<T>, MAP<K,V>, STRUCT<name: T, ...>).

Coercion decides whether one value fits a column. It returns the Python value
to store, or raises ``CoerceError`` with a cause token that matches the SDK's
``SchemaValidationCause`` wire tokens (TYPE_INCOMPATIBLE, FIELD_NOT_IN_TABLE,
MISSING_REQUIRED_COLUMN).
"""

from __future__ import annotations

import base64
import binascii
import datetime as _dt
import json
import re
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Optional, Tuple

TYPE_INCOMPATIBLE = "TYPE_INCOMPATIBLE"
FIELD_NOT_IN_TABLE = "FIELD_NOT_IN_TABLE"
MISSING_REQUIRED_COLUMN = "MISSING_REQUIRED_COLUMN"
FIELD_OUT_OF_ORDER = "FIELD_OUT_OF_ORDER"

_INT_RANGES = {
    "BYTE": (-(2**7), 2**7 - 1),
    "SHORT": (-(2**15), 2**15 - 1),
    "INT": (-(2**31), 2**31 - 1),
    "LONG": (-(2**63), 2**63 - 1),
}

_ALIASES = {
    "STRING": "STRING",
    "VARCHAR": "STRING",
    "INT": "INT",
    "INTEGER": "INT",
    "LONG": "LONG",
    "BIGINT": "LONG",
    "SHORT": "SHORT",
    "SMALLINT": "SHORT",
    "BYTE": "BYTE",
    "TINYINT": "BYTE",
    "FLOAT": "FLOAT",
    "REAL": "FLOAT",
    "DOUBLE": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "BINARY": "BINARY",
    "DECIMAL": "DECIMAL",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
    "TIMESTAMPNTZ": "TIMESTAMP_NTZ",
    "VARIANT": "VARIANT",
    "ARRAY": "ARRAY",
    "MAP": "MAP",
    "STRUCT": "STRUCT",
}


class SchemaError(ValueError):
    """Raised for an invalid table definition in the configuration."""


class CoerceError(Exception):
    """A value does not fit its column. ``token`` is the SDK cause token."""

    def __init__(self, token: str, message: str):
        super().__init__(message)
        self.token = token
        self.message = message


@dataclass(frozen=True)
class DataType:
    kind: str
    precision: int = 0
    scale: int = 0
    key: Optional["DataType"] = None  # MAP key
    value: Optional["DataType"] = None  # ARRAY element, MAP value
    fields: Tuple["StructField", ...] = ()  # STRUCT children

    def describe(self) -> str:
        if self.kind == "DECIMAL":
            return f"DECIMAL({self.precision},{self.scale})"
        if self.kind == "ARRAY":
            return f"ARRAY<{self.value.describe()}>"
        if self.kind == "MAP":
            return f"MAP<{self.key.describe()},{self.value.describe()}>"
        if self.kind == "STRUCT":
            inner = ", ".join(
                f"{f.name}: {f.type.describe()}{'' if f.nullable else ' NOT NULL'}"
                for f in self.fields
            )
            return f"STRUCT<{inner}>"
        return self.kind


@dataclass(frozen=True)
class StructField:
    name: str
    type: DataType
    nullable: bool = True


@dataclass(frozen=True)
class Column:
    name: str
    type: DataType
    nullable: bool = True

    def describe(self) -> str:
        return f"{self.name} {self.type.describe()}{' NOT NULL' if not self.nullable else ''}"


# ---------------------------------------------------------------- parsing

_TOKEN = re.compile(r"\s*([A-Za-z_][A-Za-z0-9_]*|\d+|[<>(),:])")


def _tokenize(text: str) -> list[str]:
    pos = 0
    out: list[str] = []
    text = text.strip()
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise SchemaError(f"unexpected character in type {text!r} at {pos}")
        out.append(m.group(1))
        pos = m.end()
    return out


class _Parser:
    def __init__(self, text: str):
        self.text = text
        self.toks = _tokenize(text)
        self.i = 0

    def _peek(self) -> Optional[str]:
        return self.toks[self.i] if self.i < len(self.toks) else None

    def _take(self, expected: Optional[str] = None) -> str:
        tok = self._peek()
        if tok is None:
            raise SchemaError(f"unexpected end of type {self.text!r}")
        if expected is not None and tok.upper() != expected:
            raise SchemaError(f"expected {expected} in type {self.text!r}, got {tok!r}")
        self.i += 1
        return tok

    def parse(self) -> DataType:
        t = self._type()
        if self.i != len(self.toks):
            raise SchemaError(f"trailing text in type {self.text!r}")
        return t

    def _type(self) -> DataType:
        name = self._take().upper()
        kind = _ALIASES.get(name)
        if kind is None:
            raise SchemaError(f"unknown type {name!r} in {self.text!r}")
        if kind == "DECIMAL":
            precision, scale = 10, 0
            if self._peek() == "(":
                self._take("(")
                precision = int(self._take())
                self._take(",")
                scale = int(self._take())
                self._take(")")
            if not (0 < precision <= 38 and 0 <= scale <= precision):
                raise SchemaError(f"invalid DECIMAL({precision},{scale})")
            return DataType("DECIMAL", precision=precision, scale=scale)
        if kind == "ARRAY":
            self._take("<")
            elem = self._type()
            self._take(">")
            return DataType("ARRAY", value=elem)
        if kind == "MAP":
            self._take("<")
            key = self._type()
            self._take(",")
            value = self._type()
            self._take(">")
            return DataType("MAP", key=key, value=value)
        if kind == "STRUCT":
            self._take("<")
            fields = [self._struct_field()]
            while self._peek() == ",":
                self._take(",")
                fields.append(self._struct_field())
            self._take(">")
            names = [f.name for f in fields]
            if len(set(names)) != len(names):
                raise SchemaError(f"duplicate struct field in {self.text!r}")
            return DataType("STRUCT", fields=tuple(fields))
        return DataType(kind)

    def _struct_field(self) -> StructField:
        name = self._take()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise SchemaError(f"invalid struct field name {name!r}")
        self._take(":")
        t = self._type()
        nullable = True
        if self._peek() and self._peek().upper() == "NOT":
            self._take("NOT")
            self._take("NULL")
            nullable = False
        return StructField(name, t, nullable)


def parse_type(text: str) -> DataType:
    """Parse a type string such as ``INT``, ``DECIMAL(10,2)`` or ``STRUCT<a: STRING>``."""
    if not isinstance(text, str) or not text.strip():
        raise SchemaError("type must be a non-empty string")
    return _Parser(text).parse()


def make_column(name: str, type_text: str, nullable: bool = True) -> Column:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name or ""):
        raise SchemaError(
            f"column name {name!r} must use ASCII letters, digits and underscores"
        )
    return Column(name, parse_type(type_text), bool(nullable))


@dataclass(frozen=True)
class SchemaSnapshot:
    """The table schema as one stream (or one REST request) saw it.

    A stream keeps the snapshot it was created with until it closes, which is
    how an open stream keeps its schema after the table changes.
    """

    table: str
    version: int
    columns: Tuple[Column, ...]
    rescue_column: Optional[str] = None

    def column(self, name: str) -> Optional[Column]:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def data_columns(self) -> Tuple[Column, ...]:
        """Columns a producer may write to directly (the rescue column excluded)."""
        return tuple(c for c in self.columns if c.name != self.rescue_column)


# ---------------------------------------------------------------- coercion

_EPOCH_DATE = _dt.date(1970, 1, 1)
_EPOCH_TS = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
_EPOCH_NTZ = _dt.datetime(1970, 1, 1)


def _mismatch(path: str, expected: str, got: Any) -> CoerceError:
    return CoerceError(
        TYPE_INCOMPATIBLE,
        f"{path} expected {expected}, got {type(got).__name__}",
    )


def coerce(dtp: DataType, value: Any, path: str = "value") -> Any:
    """Return the stored Python value for ``value`` under ``dtp``.

    ``None`` is handled by the caller for top-level columns. Inside arrays and
    maps a ``None`` element is stored as ``None``.
    """
    k = dtp.kind
    if k == "STRING":
        if isinstance(value, str):
            return value
        raise _mismatch(path, "STRING", value)
    if k in _INT_RANGES:
        if isinstance(value, bool) or not isinstance(value, int):
            raise _mismatch(path, k, value)
        lo, hi = _INT_RANGES[k]
        if not lo <= value <= hi:
            raise CoerceError(TYPE_INCOMPATIBLE, f"{path} is out of range for {k}")
        return value
    if k in ("FLOAT", "DOUBLE"):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise _mismatch(path, k, value)
        return float(value)
    if k == "BOOLEAN":
        if isinstance(value, bool):
            return value
        raise _mismatch(path, "BOOLEAN", value)
    if k == "BINARY":
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        if isinstance(value, str):
            try:
                return base64.b64decode(value, validate=True)
            except (binascii.Error, ValueError):
                raise CoerceError(TYPE_INCOMPATIBLE, f"{path} is not valid base64 for BINARY")
        raise _mismatch(path, "BINARY (base64 string)", value)
    if k == "DECIMAL":
        if not isinstance(value, str):
            raise _mismatch(path, "DECIMAL (decimal text)", value)
        try:
            d = Decimal(value)
        except InvalidOperation:
            raise CoerceError(TYPE_INCOMPATIBLE, f"{path} is not decimal text")
        if not d.is_finite():
            raise CoerceError(TYPE_INCOMPATIBLE, f"{path} is not a finite decimal")
        q = d.quantize(Decimal(1).scaleb(-dtp.scale), rounding=ROUND_HALF_UP)
        if len(q.as_tuple().digits) > dtp.precision:
            raise CoerceError(
                TYPE_INCOMPATIBLE, f"{path} exceeds {dtp.describe()} precision"
            )
        return q
    if k == "DATE":
        if isinstance(value, bool) or not isinstance(value, int):
            raise _mismatch(path, "DATE (days since epoch)", value)
        try:
            return _EPOCH_DATE + _dt.timedelta(days=value)
        except OverflowError:
            raise CoerceError(TYPE_INCOMPATIBLE, f"{path} is out of range for DATE")
    if k in ("TIMESTAMP", "TIMESTAMP_NTZ"):
        if isinstance(value, bool) or not isinstance(value, int):
            raise _mismatch(path, f"{k} (microseconds since epoch)", value)
        try:
            if k == "TIMESTAMP":
                return _EPOCH_TS + _dt.timedelta(microseconds=value)
            return _EPOCH_NTZ + _dt.timedelta(microseconds=value)
        except OverflowError:
            raise CoerceError(TYPE_INCOMPATIBLE, f"{path} is out of range for {k}")
    if k == "VARIANT":
        if not isinstance(value, str):
            raise _mismatch(path, "VARIANT (JSON-encoded string)", value)
        try:
            json.loads(value)
        except ValueError:
            raise CoerceError(TYPE_INCOMPATIBLE, f"{path} is not a JSON-encoded string")
        return value
    if k == "ARRAY":
        if not isinstance(value, list):
            raise _mismatch(path, "ARRAY", value)
        return [
            None if item is None else coerce(dtp.value, item, f"{path}[{i}]")
            for i, item in enumerate(value)
        ]
    if k == "MAP":
        if not isinstance(value, dict):
            raise _mismatch(path, "MAP (object)", value)
        out = {}
        for key, item in value.items():
            pkey = coerce(dtp.key, _map_key(dtp.key, key, path), f"{path} key")
            pval = None if item is None else coerce(dtp.value, item, f"{path}[{key}]")
            out[pkey] = pval
        return out
    if k == "STRUCT":
        if not isinstance(value, dict):
            raise _mismatch(path, "STRUCT (object)", value)
        known = {f.name for f in dtp.fields}
        for name in value:
            if name not in known:
                raise CoerceError(
                    FIELD_NOT_IN_TABLE, f"{path}.{name} is not a field of the struct"
                )
        out = {}
        for f in dtp.fields:
            item = value.get(f.name)
            if item is None:
                if not f.nullable:
                    raise CoerceError(
                        MISSING_REQUIRED_COLUMN, f"{path}.{f.name} is required"
                    )
                out[f.name] = None
            else:
                out[f.name] = coerce(f.type, item, f"{path}.{f.name}")
        return out
    raise SchemaError(f"unsupported type kind {k}")


def _map_key(key_type: DataType, key: str, path: str) -> Any:
    if key_type.kind in _INT_RANGES:
        try:
            return int(key)
        except ValueError:
            raise CoerceError(TYPE_INCOMPATIBLE, f"{path} key {key!r} is not an integer")
    return key


# ---------------------------------------------------------------- rendering


def to_json_value(value: Any) -> Any:
    """Render a stored Python value as plain JSON for the read API."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, _dt.datetime):
        return value.isoformat()
    if isinstance(value, _dt.date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): to_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_json_value(v) for v in value]
    if isinstance(value, tuple) and len(value) == 2:
        return [to_json_value(value[0]), to_json_value(value[1])]
    return str(value)


def normalize_native(dtp: DataType, value: Any) -> Any:
    """Convert a value from a binary decoder (pyarrow, fastavro) to the JSON-like form ``coerce`` accepts.

    Dates become days since the epoch, timestamps become microseconds since the epoch,
    decimals become text, maps become dicts, and unknown struct keys are kept so that
    ``coerce`` can reject them.
    """
    if value is None:
        return None
    k = dtp.kind
    if k == "DATE" and isinstance(value, _dt.date) and not isinstance(value, _dt.datetime):
        return (value - _EPOCH_DATE).days
    if k in ("TIMESTAMP", "TIMESTAMP_NTZ") and isinstance(value, _dt.datetime):
        base = _EPOCH_NTZ if value.tzinfo is None else _EPOCH_TS
        delta = value - base
        return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    if k == "DECIMAL" and isinstance(value, Decimal):
        return format(value, "f")
    if k == "BINARY" and isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if k == "ARRAY" and isinstance(value, (list, tuple)):
        return [normalize_native(dtp.value, v) for v in value]
    if k == "MAP":
        items = value.items() if isinstance(value, dict) else value
        return {normalize_native(dtp.key, kk): normalize_native(dtp.value, vv) for kk, vv in items}
    if k == "STRUCT" and isinstance(value, dict):
        out = dict(value)
        for f in dtp.fields:
            if f.name in value:
                out[f.name] = normalize_native(f.type, value[f.name])
        return out
    return value
