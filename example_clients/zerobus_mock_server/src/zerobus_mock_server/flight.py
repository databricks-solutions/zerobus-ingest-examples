"""Arrow Flight DoPut: message classes, Arrow IPC framing and batch decoding. Table and token travel in gRPC metadata."""

from __future__ import annotations

import json
import struct
from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.ipc  # noqa: F401  (registers the IPC readers)
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

FLIGHT_SERVICE = "arrow.flight.protocol.FlightService"
_PKG = "arrow.flight.protocol"
_EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"


def _build_messages():
    """FlightData and PutResult, with the fields this mock reads and writes (other fields are skipped)."""
    FD = descriptor_pb2.FieldDescriptorProto
    fdp = descriptor_pb2.FileDescriptorProto(name="flight_subset.proto", package=_PKG, syntax="proto3")
    data = fdp.message_type.add(name="FlightData")
    for name, number in (("data_header", 2), ("app_metadata", 3), ("data_body", 1000)):
        data.field.add(name=name, number=number, type=FD.TYPE_BYTES, label=FD.LABEL_OPTIONAL)
    fdp.message_type.add(name="PutResult").field.add(name="app_metadata", number=1, type=FD.TYPE_BYTES,
                                                     label=FD.LABEL_OPTIONAL)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    return (message_factory.GetMessageClass(pool.FindMessageTypeByName(f"{_PKG}.FlightData")),
            message_factory.GetMessageClass(pool.FindMessageTypeByName(f"{_PKG}.PutResult")))


FlightData, PutResult = _build_messages()


class FlightError(ValueError):
    """A DoPut message that can't be decoded or doesn't fit the stream."""


def ipc_frame(header: bytes, body: bytes = b"") -> bytes:
    """One Arrow IPC message: continuation marker, length, padded metadata, body."""
    pad = (8 - len(header) % 8) % 8
    return b"\xff\xff\xff\xff" + struct.pack("<i", len(header) + pad) + header + b"\x00" * pad + body


def ack_result(upto: int, records: int, close_ms: Optional[int] = None) -> Any:
    payload = {"ack_up_to_offset": upto, "ack_up_to_records": records}
    if close_ms is not None:
        payload["close_stream_duration_ms"] = close_ms
    return PutResult(app_metadata=json.dumps(payload).encode())


def ready_result() -> Any:
    return ack_result(-1, 0)


def batch_offset(app_metadata: bytes) -> int:
    try:
        return int(json.loads(app_metadata)["offset_id"])
    except (ValueError, KeyError, TypeError):
        raise FlightError('app_metadata must be {"offset_id": n} on every record batch')


def schema_from_header(header: bytes) -> pa.Schema:
    try:
        return pa.ipc.open_stream(ipc_frame(header) + _EOS).schema
    except (pa.ArrowInvalid, OSError) as exc:
        raise FlightError(f"the first FlightData is not an Arrow schema message: {exc}")


def batch_from_frames(schema_header: bytes, frames: List[bytes]) -> pa.RecordBatch:
    """One record batch, from its IPC frames (dictionary frames first)."""
    try:
        return pa.ipc.open_stream(ipc_frame(schema_header) + b"".join(frames) + _EOS).read_next_batch()
    except (pa.ArrowInvalid, StopIteration) as exc:
        raise FlightError(f"the record batch does not decode against the schema: {exc}")


_SIMPLE = [(pa.types.is_boolean, "BOOLEAN"), (pa.types.is_int8, "BYTE"), (pa.types.is_int16, "SHORT"),
           (pa.types.is_int32, "INT"), (pa.types.is_int64, "LONG"), (pa.types.is_float32, "FLOAT"),
           (pa.types.is_float64, "DOUBLE"), (pa.types.is_decimal, "DECIMAL"), (pa.types.is_date, "DATE"),
           (pa.types.is_map, "MAP"), (pa.types.is_struct, "STRUCT")]


def arrow_kind(t: pa.DataType) -> str:
    """The basic column kind an Arrow type maps to ("UNKNOWN" when it has none)."""
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "STRING"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "BINARY"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP" if t.tz else "TIMESTAMP_NTZ"
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return "ARRAY"
    return next((kind for test, kind in _SIMPLE if test(t)), "UNKNOWN")
