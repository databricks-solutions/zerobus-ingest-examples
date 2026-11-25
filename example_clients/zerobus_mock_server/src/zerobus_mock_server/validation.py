"""Protobuf streams: load the descriptor from create_stream and decode records with it. Nested messages match by simple name."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.descriptor import FieldDescriptor as FD
from google.protobuf.message import DecodeError


class FormatError(ValueError):
    """A descriptor, writer schema or record that the mock can't use."""


PROTO_KINDS = {
    FD.TYPE_STRING: "STRING", FD.TYPE_BOOL: "BOOLEAN", FD.TYPE_BYTES: "BINARY",
    FD.TYPE_FLOAT: "FLOAT", FD.TYPE_DOUBLE: "DOUBLE",
    FD.TYPE_INT32: "INT", FD.TYPE_SINT32: "INT", FD.TYPE_SFIXED32: "INT", FD.TYPE_UINT32: "INT",
    FD.TYPE_INT64: "LONG", FD.TYPE_SINT64: "LONG", FD.TYPE_SFIXED64: "LONG", FD.TYPE_UINT64: "LONG",
}


def _is_repeated(fd) -> bool:
    return fd.is_repeated if hasattr(fd, "is_repeated") else fd.label == FD.LABEL_REPEATED


def _field_kind(fd) -> str:
    if _is_repeated(fd):
        return "ARRAY"
    return "STRUCT" if fd.type == FD.TYPE_MESSAGE else PROTO_KINDS.get(fd.type, "UNKNOWN")


def _resolve_nested(msg: descriptor_pb2.DescriptorProto) -> None:
    """Points each message-typed field at its nested type's full name, matched by simple name."""
    found: Dict[str, List[str]] = {}

    def walk(m, path: str) -> None:
        for n in m.nested_type:
            found.setdefault(n.name, []).append(f"{path}.{n.name}")
            walk(n, f"{path}.{n.name}")

    walk(msg, msg.name)
    for f in msg.field:
        candidates = found.get(f.type_name.rsplit(".", 1)[-1], []) if f.type_name else []
        if len(candidates) == 1:
            f.type_name = "." + candidates[0]


def to_dict(msg) -> Dict[str, Any]:
    """A decoded protobuf message as a dict of its set fields."""
    conv = lambda fd, v: to_dict(v) if fd.type == FD.TYPE_MESSAGE else v  # noqa: E731
    return {fd.name: [conv(fd, v) for v in value] if _is_repeated(fd) else conv(fd, value)
            for fd, value in msg.ListFields()}


class ProtoStream:
    def __init__(self, descriptor_bytes: bytes) -> None:
        dp = descriptor_pb2.DescriptorProto()
        try:
            dp.ParseFromString(descriptor_bytes)
        except Exception as exc:
            raise FormatError(f"descriptor_proto does not parse: {exc}")
        dp.name = dp.name or "ZerobusRecord"
        _resolve_nested(dp)
        pool = descriptor_pool.DescriptorPool()
        try:
            pool.Add(descriptor_pb2.FileDescriptorProto(name="zerobus_record.proto", message_type=[dp]))
            desc = pool.FindMessageTypeByName(dp.name)
        except Exception as exc:
            raise FormatError(f"descriptor_proto can't be loaded: {exc}")
        self._cls = message_factory.GetMessageClass(desc)
        self.fields: List[Tuple[str, str]] = [(f.name, _field_kind(f)) for f in desc.fields]

    def decode(self, raw: bytes) -> Dict[str, Any]:
        msg = self._cls()
        try:
            msg.ParseFromString(bytes(raw))
        except DecodeError:
            raise FormatError("record does not decode as the descriptor's message")
        return to_dict(msg)
