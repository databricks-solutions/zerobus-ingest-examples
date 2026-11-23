"""Avro record format for gRPC streams: writer-schema checks and per-datum decoding.

Following the docs (zerobus-message-types.md#avro, zerobus-concepts.md#avro-types):

* the writer schema is sent once, in ``create_stream.avro_schema_json``, and every
  record on the stream is decoded against it;
* records are raw Avro binary datums, with no container header, no schema-registry
  prefix and no fingerprint;
* fields pair to columns by name; every non-nullable column must be present;
* named-type references are not supported; declare each type inline.

Framed and container Avro are rejected: a container file starts with the ``Obj\\x01``
magic, and a framed (schema-registry) record starts with a zero byte and a 4-byte
schema ID. A datum that has trailing bytes after it decodes is rejected too.
"""

from __future__ import annotations

import datetime as _dt
import io
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import fastavro

from .schema import (
    FIELD_NOT_IN_TABLE,
    MISSING_REQUIRED_COLUMN,
    TYPE_INCOMPATIBLE,
    DataType,
    SchemaSnapshot,
    StructField,
    normalize_native,
)
from .validation import Cause

FIELD_NOT_IN_TABLE_CODE = "4044"
PRIMITIVES = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}
PRIM_TYPES = {
    "boolean": DataType("BOOLEAN"),
    "int": DataType("INT"),
    "long": DataType("LONG"),
    "float": DataType("FLOAT"),
    "double": DataType("DOUBLE"),
    "bytes": DataType("BINARY"),
    "string": DataType("STRING"),
}
CONTAINER_MAGIC = b"Obj\x01"


class AvroSchemaError(ValueError):
    """The writer schema can't be used for this stream."""


class AvroDecodeError(ValueError):
    """A record is not a single raw Avro datum for the writer schema."""


@dataclass
class AvroStream:
    raw: Dict[str, Any]
    parsed: Any
    fields: Dict[str, Tuple[DataType, bool]]  # field name -> (type, nullable)


def _unsupported(msg: str) -> AvroSchemaError:
    return AvroSchemaError(msg)


def _type_of(node: Any) -> Tuple[DataType, bool]:
    """Return (type, nullable) for an Avro type node."""
    if isinstance(node, str):
        if node in PRIMITIVES:
            if node == "null":
                raise _unsupported("a bare 'null' type is not a column type")
            return PRIM_TYPES[node], False
        raise _unsupported(f"named type reference {node!r} is not supported; declare the type inline")
    if isinstance(node, list):
        members = list(node)
        non_null = [m for m in members if m != "null"]
        nullable = len(non_null) != len(members)
        if len(non_null) == 1:
            t, inner_null = _type_of(non_null[0])
            return t, nullable or inner_null
        names = [m if isinstance(m, str) else m.get("type") for m in non_null]
        if set(names) == {"int", "long"}:
            return DataType("LONG"), nullable
        if set(names) == {"float", "double"}:
            return DataType("DOUBLE"), nullable
        raise _unsupported("this union is not supported (only [\"null\", T] and int/long or float/double unions)")
    if isinstance(node, dict):
        kind = node.get("type")
        logical = node.get("logicalType")
        if kind == "record":
            fields = []
            for f in node.get("fields", []):
                ft, fn = _type_of(f["type"])
                fields.append(StructField(f["name"], ft, fn))
            return DataType("STRUCT", fields=tuple(fields)), False
        if kind == "enum":
            return DataType("STRING"), False
        if kind == "array":
            item, inull = _type_of(node["items"])
            return DataType("ARRAY", value=item), False
        if kind == "map":
            val, _ = _type_of(node["values"])
            return DataType("MAP", key=DataType("STRING"), value=val), False
        if kind == "fixed":
            if logical == "uuid" and node.get("size") == 16:
                return DataType("STRING"), False
            if logical == "decimal":
                return DataType("DECIMAL", precision=int(node["precision"]), scale=int(node.get("scale", 0))), False
            return DataType("BINARY"), False
        if kind in PRIMITIVES:
            if kind == "int" and logical == "date":
                return DataType("DATE"), False
            if kind == "long" and logical in ("timestamp-millis", "timestamp-micros"):
                return DataType("TIMESTAMP"), False
            if kind == "bytes" and logical == "decimal":
                return DataType("DECIMAL", precision=int(node["precision"]), scale=int(node.get("scale", 0))), False
            return _type_of(kind)
        raise _unsupported(f"Avro type {kind!r} is not supported")
    raise _unsupported("unreadable Avro type node")


def _compatible(avro: DataType, col: DataType) -> bool:
    if avro.kind != col.kind:
        return False
    if avro.kind == "DECIMAL":
        return avro.precision == col.precision and avro.scale == col.scale
    if avro.kind == "ARRAY":
        return _compatible(avro.value, col.value)
    if avro.kind == "MAP":
        return _compatible(avro.key, col.key) and _compatible(avro.value, col.value)
    if avro.kind == "STRUCT":
        cols = {f.name: f for f in col.fields}
        for f in avro.fields:
            if f.name not in cols or not _compatible(f.type, cols[f.name].type):
                return False
        return True
    return True


def load_writer_schema(text: str) -> AvroStream:
    """Parse and check a writer schema from ``create_stream.avro_schema_json``."""
    try:
        raw = json.loads(text)
    except (TypeError, ValueError) as exc:
        raise _unsupported(f"avro_schema_json is not JSON: {exc}")
    if not (isinstance(raw, dict) and raw.get("type") == "record"):
        raise _unsupported("the Avro writer schema must be a record")
    try:
        parsed = fastavro.parse_schema(raw)
    except Exception as exc:  # fastavro raises several types
        raise _unsupported(f"the Avro writer schema does not parse: {exc}")
    fields = {}
    for f in raw.get("fields", []):
        fields[f["name"]] = _type_of(f["type"])
    return AvroStream(raw=raw, parsed=parsed, fields=fields)


def check_against_table(snap: SchemaSnapshot, stream: AvroStream) -> List[Cause]:
    causes: List[Cause] = []
    for name, (atype, anull) in stream.fields.items():
        col = snap.column(name)
        if col is None or name == snap.rescue_column:
            causes.append(Cause(FIELD_NOT_IN_TABLE, name, f"field '{name}' is not a column of {snap.table}", FIELD_NOT_IN_TABLE_CODE))
            continue
        if not _compatible(atype, col.type):
            causes.append(Cause(TYPE_INCOMPATIBLE, name, f"Avro type of '{name}' does not fit {col.type.describe()}"))
        elif anull and not col.nullable:
            causes.append(Cause(TYPE_INCOMPATIBLE, name, f"field '{name}' is nullable in Avro but the column is NOT NULL"))
    for col in snap.columns:
        if col.name == snap.rescue_column or col.nullable:
            continue
        if col.name not in stream.fields:
            causes.append(Cause(MISSING_REQUIRED_COLUMN, col.name, f"field '{col.name}' is required (non-nullable) but missing from the writer schema"))
    return causes


def decode_datum(stream: AvroStream, data: bytes) -> Dict[str, Any]:
    """Decode one raw Avro datum. Rejects container files, framed records and trailing bytes."""
    if data[:4] == CONTAINER_MAGIC:
        raise AvroDecodeError("this is an Avro container file; send raw Avro binary datums")
    buf = io.BytesIO(data)
    try:
        value = fastavro.schemaless_reader(buf, stream.parsed)
    except Exception as exc:
        if data[:1] == b"\x00":
            raise AvroDecodeError("looks like a framed (schema-registry) record; send the raw datum")
        raise AvroDecodeError(f"record does not decode against the writer schema: {exc}")
    if buf.tell() != len(data):
        if data[:1] == b"\x00":
            raise AvroDecodeError("looks like a framed (schema-registry) record; send the raw datum")
        raise AvroDecodeError("trailing bytes after the datum; this is not a single raw datum for the writer schema")
    if not isinstance(value, dict):
        raise AvroDecodeError("the datum is not a record")
    return normalize_native(DataType("STRUCT", fields=tuple(
        StructField(n, t, nl) for n, (t, nl) in stream.fields.items())), value)
