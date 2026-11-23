"""Avro records on gRPC streams: the writer schema from create_stream, and raw datum decoding (no container or registry framing)."""

from __future__ import annotations

import io
import json
from typing import Any, Dict, List, Tuple

import fastavro

from .validation import FormatError

PRIMITIVE_KINDS = {"boolean": "BOOLEAN", "int": "INT", "long": "LONG", "float": "FLOAT",
                   "double": "DOUBLE", "bytes": "BINARY", "string": "STRING"}


def avro_kind(node: Any) -> str:
    """The basic column kind an Avro type node maps to ("UNKNOWN" when it has none)."""
    if isinstance(node, str):
        return PRIMITIVE_KINDS.get(node, "UNKNOWN")
    if isinstance(node, list):  # a union: nullable when it holds null and one other type
        non_null = [m for m in node if m != "null"]
        return avro_kind(non_null[0]) if len(non_null) == 1 else "UNKNOWN"
    kind, logical = node.get("type"), node.get("logicalType")
    if kind in ("record", "array", "map", "enum", "fixed"):
        return {"record": "STRUCT", "array": "ARRAY", "map": "MAP", "enum": "STRING", "fixed": "BINARY"}[kind]
    if logical in ("date", "decimal") or (logical or "").startswith("timestamp"):
        return {"date": "DATE", "decimal": "DECIMAL"}.get(logical, "TIMESTAMP")
    return avro_kind(kind) if isinstance(kind, str) else "UNKNOWN"


class AvroStream:
    def __init__(self, schema_json: str) -> None:
        try:
            raw = json.loads(schema_json)
        except (TypeError, ValueError) as exc:
            raise FormatError(f"avro_schema_json is not JSON: {exc}")
        if not isinstance(raw, dict) or raw.get("type") != "record":
            raise FormatError("the Avro writer schema must be a record")
        try:
            self._parsed = fastavro.parse_schema(raw)
        except Exception as exc:  # fastavro raises several types
            raise FormatError(f"the Avro writer schema does not parse: {exc}")
        self.fields: List[Tuple[str, str]] = [(f["name"], avro_kind(f["type"])) for f in raw.get("fields", [])]

    def decode(self, data: bytes) -> Dict[str, Any]:
        buf = io.BytesIO(bytes(data))
        try:
            value = fastavro.schemaless_reader(buf, self._parsed)
        except Exception as exc:
            raise FormatError(f"record does not decode against the writer schema: {exc}")
        if buf.tell() != len(data):
            raise FormatError("trailing bytes after the datum; send one raw Avro datum per record")
        return value
