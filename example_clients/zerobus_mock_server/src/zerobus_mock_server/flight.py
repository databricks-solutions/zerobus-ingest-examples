"""Arrow Flight support for the mock: the DoPut messages, Arrow IPC framing and schema checks.

The SDKs ingest Arrow over the same gRPC connection as the Zerobus stream, through
Flight's ``DoPut`` method. The Rust SDK (``rust/sdk/src/stream/arrow``) sends:

* gRPC metadata ``authorization: Bearer <token>`` and
  ``x-databricks-zerobus-table-name: <catalog.schema.table>``. No FlightDescriptor
  is set; the table and the token travel in the headers.
* ``FlightData`` messages. The first one is the Arrow schema (``data_header`` holds the
  IPC schema message, no body). Each record batch is one ``FlightData`` whose
  ``app_metadata`` is JSON ``{"offset_id": n}``, with offsets 0, 1, 2, ... Dictionary
  batches carry no offset.
* The server first sends a "ready" ``PutResult`` with ``app_metadata``
  ``{"ack_up_to_offset": -1, "ack_up_to_records": 0}`` once setup succeeds. Later
  acks are ``PutResult`` messages with the same JSON shape; ``close_stream_duration_ms``
  adds a close signal.

The Flight message classes are built from descriptors here, so the mock needs no
Flight ``.proto`` file. Only the fields above are modelled.
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.ipc  # noqa: F401  (registers ipc readers)
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from .schema import (
    FIELD_NOT_IN_TABLE,
    FIELD_OUT_OF_ORDER,
    MISSING_REQUIRED_COLUMN,
    TYPE_INCOMPATIBLE,
    DataType,
    SchemaSnapshot,
)
from .validation import Cause

FLIGHT_SERVICE = "arrow.flight.protocol.FlightService"
DO_PUT_PATH = f"/{FLIGHT_SERVICE}/DoPut"
PKG = "arrow.flight.protocol"
FIELD_NOT_IN_TABLE_CODE = "4044"

_EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"
_CONTINUATION = b"\xff\xff\xff\xff"


def _build_messages():
    FD = descriptor_pb2.FieldDescriptorProto
    fdp = descriptor_pb2.FileDescriptorProto(name="flight_subset.proto", package=PKG, syntax="proto3")
    desc = fdp.message_type.add(name="FlightDescriptor")
    dtype = desc.enum_type.add(name="DescriptorType")
    for name, number in (("UNKNOWN", 0), ("PATH", 1), ("CMD", 2)):
        dtype.value.add(name=name, number=number)
    desc.field.add(name="type", number=1, type=FD.TYPE_ENUM, label=FD.LABEL_OPTIONAL,
                   type_name=f".{PKG}.FlightDescriptor.DescriptorType")
    desc.field.add(name="cmd", number=2, type=FD.TYPE_BYTES, label=FD.LABEL_OPTIONAL)
    desc.field.add(name="path", number=3, type=FD.TYPE_STRING, label=FD.LABEL_REPEATED)
    data = fdp.message_type.add(name="FlightData")
    data.field.add(name="flight_descriptor", number=1, type=FD.TYPE_MESSAGE, label=FD.LABEL_OPTIONAL,
                   type_name=f".{PKG}.FlightDescriptor")
    data.field.add(name="data_header", number=2, type=FD.TYPE_BYTES, label=FD.LABEL_OPTIONAL)
    data.field.add(name="app_metadata", number=3, type=FD.TYPE_BYTES, label=FD.LABEL_OPTIONAL)
    data.field.add(name="data_body", number=1000, type=FD.TYPE_BYTES, label=FD.LABEL_OPTIONAL)
    put = fdp.message_type.add(name="PutResult")
    put.field.add(name="app_metadata", number=1, type=FD.TYPE_BYTES, label=FD.LABEL_OPTIONAL)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    return (
        message_factory.GetMessageClass(pool.FindMessageTypeByName(f"{PKG}.FlightData")),
        message_factory.GetMessageClass(pool.FindMessageTypeByName(f"{PKG}.PutResult")),
    )


FlightData, PutResult = _build_messages()


class FlightError(Exception):
    """A DoPut message can't be decoded or doesn't fit the stream."""


def ipc_frame(header: bytes, body: bytes = b"") -> bytes:
    """One Arrow IPC stream message: continuation marker, length, padded metadata, body."""
    pad = (8 - len(header) % 8) % 8
    return _CONTINUATION + struct.pack("<i", len(header) + pad) + header + b"\x00" * pad + body


def ready_result() -> Any:
    return PutResult(app_metadata=json.dumps({"ack_up_to_offset": -1, "ack_up_to_records": 0}).encode())


def ack_result(upto: int, records: int, close_ms: Optional[int] = None) -> Any:
    payload = {"ack_up_to_offset": upto, "ack_up_to_records": records}
    if close_ms is not None:
        payload["close_stream_duration_ms"] = close_ms
    return PutResult(app_metadata=json.dumps(payload).encode())


def batch_offset(app_metadata: bytes) -> int:
    if not app_metadata:
        raise FlightError("FlightData has no app_metadata; a record batch needs {\"offset_id\": n}")
    try:
        meta = json.loads(app_metadata)
        return int(meta["offset_id"])
    except (ValueError, KeyError, TypeError):
        raise FlightError("app_metadata is not {\"offset_id\": n}")


def schema_from_header(header: bytes) -> pa.Schema:
    try:
        reader = pa.ipc.open_stream(ipc_frame(header) + _EOS)
        return reader.schema
    except (pa.ArrowInvalid, OSError) as exc:
        raise FlightError(f"the first FlightData is not an Arrow schema message: {exc}")


def batch_from_frames(schema_header: bytes, frames: List[bytes]) -> pa.RecordBatch:
    """Decode one record batch from its IPC frames (dictionary frames first, then the batch)."""
    stream = ipc_frame(schema_header) + b"".join(frames) + _EOS
    try:
        reader = pa.ipc.open_stream(stream)
        batch = reader.read_next_batch()
    except (pa.ArrowInvalid, StopIteration) as exc:
        raise FlightError(f"the record batch does not decode against the schema: {exc}")
    return batch


# ---------------------------------------------------------------- schema check

def _fits(dtp: DataType, at: pa.DataType) -> bool:
    k = dtp.kind
    if k == "STRING":
        return pa.types.is_string(at) or pa.types.is_large_string(at)
    if k == "BINARY":
        return pa.types.is_binary(at) or pa.types.is_large_binary(at)
    simple = {"INT": pa.int32(), "SHORT": pa.int16(), "BYTE": pa.int8(), "LONG": pa.int64(),
              "FLOAT": pa.float32(), "DOUBLE": pa.float64(), "BOOLEAN": pa.bool_()}
    if k in simple:
        return at == simple[k]
    if k == "DECIMAL":
        return pa.types.is_decimal(at) and at.precision == dtp.precision and at.scale == dtp.scale
    if k == "DATE":
        return pa.types.is_date32(at)
    if k == "TIMESTAMP":
        return pa.types.is_timestamp(at) and at.unit == "us" and at.tz is not None
    if k == "TIMESTAMP_NTZ":
        return pa.types.is_timestamp(at) and at.unit == "us" and at.tz is None
    if k == "ARRAY":
        return (pa.types.is_list(at) or pa.types.is_large_list(at)) and _fits(dtp.value, at.value_type)
    if k == "MAP":
        return pa.types.is_map(at) and _fits(dtp.key, at.key_type) and _fits(dtp.value, at.item_type)
    if k == "STRUCT":
        if not pa.types.is_struct(at):
            return False
        names = {f.name for f in dtp.fields}
        got = {at[i].name for i in range(at.num_fields)}
        if names != got:
            return False
        for sf in dtp.fields:
            if not _fits(sf.type, at[sf.name].type):
                return False
        return True
    return False  # VARIANT and anything else: not accepted over Arrow by the mock


def check_arrow_schema(snap: SchemaSnapshot, schema: pa.Schema) -> List[Cause]:
    """Causes that stop an Arrow stream from being created against ``snap`` (docs: schema matching)."""
    causes: List[Cause] = []
    seen: List[str] = []
    for f in schema:
        col = snap.column(f.name)
        if col is None or f.name == snap.rescue_column:
            causes.append(Cause(FIELD_NOT_IN_TABLE, f.name, f"field '{f.name}' is not a column of {snap.table}", FIELD_NOT_IN_TABLE_CODE))
            continue
        seen.append(f.name)
        if f.nullable != col.nullable:
            causes.append(Cause(TYPE_INCOMPATIBLE, f.name, f"field '{f.name}' nullability differs from the table column"))
        if not _fits(col.type, f.type):
            causes.append(Cause(TYPE_INCOMPATIBLE, f.name, f"field '{f.name}' type {f.type} does not fit {col.type.describe()}"))
    table_order = [c.name for c in snap.columns if c.name in seen]
    if seen != table_order:
        causes.append(Cause(FIELD_OUT_OF_ORDER, "schema", "fields must follow the table's column order"))
    for col in snap.columns:
        if col.name == snap.rescue_column or col.nullable:
            continue
        if col.name not in seen:
            causes.append(Cause(MISSING_REQUIRED_COLUMN, col.name, f"field '{col.name}' is required (non-nullable) but missing"))
    return causes


def batch_rows(snap: SchemaSnapshot, batch: pa.RecordBatch) -> List[dict]:
    """Rows of a batch in JSON-like form (see schema.normalize_native), keyed by column name."""
    from .schema import normalize_native

    cols = {c.name: c for c in snap.columns}
    out = []
    for raw in batch.to_pylist():
        row = {}
        for name, value in raw.items():
            col = cols.get(name)
            row[name] = normalize_native(col.type, value) if col is not None else value
        out.append(row)
    return out
