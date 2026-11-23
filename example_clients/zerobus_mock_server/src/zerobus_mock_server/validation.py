"""Record validation against a schema snapshot.

Rules follow the Zerobus schema-management and rescue-column docs:

* every non-nullable column must be present, otherwise the record is rejected;
* a field that is not a column is rejected, unless a rescue column is set, in
  which case the field goes into the rescue column as a JSON object;
* a value whose type does not fit a nullable column goes to the rescue column
  (when one is set); otherwise the record is rejected;
* a JSON ``null`` is the same as an absent field;
* the rescue column can't be set by the record (a null is ignored).

Protobuf streams fix their fields when the stream is created, so the checks
for them run once against the descriptor (``check_descriptor``). Records are
then converted to dicts (``message_to_dict``) and go through the same rules,
with the rescue column disabled, because the docs limit the rescue column to
JSON.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.descriptor import FieldDescriptor

from .schema import (
    FIELD_NOT_IN_TABLE,
    MISSING_REQUIRED_COLUMN,
    TYPE_INCOMPATIBLE,
    CoerceError,
    DataType,
    SchemaSnapshot,
    coerce,
)

RESCUE_RESERVED = "RESCUE_COLUMN_RESERVED"
# Error code the public docs use for column-name collisions (error 4044); the
# testing against the service showed the same code for an unrecognized field.
FIELD_NOT_IN_TABLE_CODE = "4044"


@dataclass(frozen=True)
class Cause:
    token: str
    field: str
    message: str
    error_code: Optional[str] = None


class RecordRejected(Exception):
    """The record does not fit the table. ``causes`` lists every problem found."""

    def __init__(self, causes: List[Cause], where: str = ""):
        self.causes = causes
        self.where = where
        super().__init__(self.render())

    def render(self) -> str:
        prefix = f"{self.where}: " if self.where else ""
        parts = [f"{c.message} [{c.token}]" for c in self.causes]
        return prefix + "; ".join(parts)


class DescriptorError(Exception):
    """The protobuf descriptor can't be loaded."""


def validate_object(
    snap: SchemaSnapshot,
    obj: Dict[str, Any],
    *,
    rescue_allowed: bool = True,
) -> Dict[str, Any]:
    """Validate one record object. Returns the row to store, keyed by column name."""
    causes: List[Cause] = []
    row: Dict[str, Any] = {c.name: None for c in snap.columns}
    failed: set = set()
    rescue: Dict[str, Any] = {}
    rescue_col = snap.rescue_column if rescue_allowed else None

    for key, value in obj.items():
        col = snap.column(key)
        if col is not None and key != snap.rescue_column:
            if value is None:
                continue
            try:
                row[key] = coerce(col.type, value, key)
            except CoerceError as exc:
                if rescue_col and col.nullable:
                    rescue[key] = value
                else:
                    causes.append(Cause(exc.token, key, exc.message))
                    failed.add(key)
            continue
        if key == snap.rescue_column:
            if value is not None:
                causes.append(
                    Cause(RESCUE_RESERVED, key, f"field '{key}' is the rescue column and can't be set by a record")
                )
            continue
        if value is None:
            continue
        if rescue_col:
            rescue[key] = value
        else:
            causes.append(
                Cause(
                    FIELD_NOT_IN_TABLE,
                    key,
                    f"field '{key}' is not a column of {snap.table}",
                    FIELD_NOT_IN_TABLE_CODE,
                )
            )

    for col in snap.columns:
        if col.name == snap.rescue_column or col.nullable:
            continue
        if row[col.name] is None and col.name not in failed:
            causes.append(Cause(MISSING_REQUIRED_COLUMN, col.name, f"field '{col.name}' is required (non-nullable) but missing"))

    if rescue_col and rescue:
        row[rescue_col] = json.dumps(rescue, separators=(",", ":"))
    if causes:
        raise RecordRejected(causes)
    return row


# ---------------------------------------------------------------- protobuf

_SCALAR_NAMES = {
    FieldDescriptor.TYPE_STRING: "string",
    FieldDescriptor.TYPE_INT32: "int32",
    FieldDescriptor.TYPE_INT64: "int64",
    FieldDescriptor.TYPE_FLOAT: "float",
    FieldDescriptor.TYPE_DOUBLE: "double",
    FieldDescriptor.TYPE_BOOL: "bool",
    FieldDescriptor.TYPE_BYTES: "bytes",
}

# Protobuf scalar types each table type maps to (proto column of the type-support table).
_PROTO_FOR_TYPE = {
    "STRING": {"string"},
    "VARIANT": {"string"},
    "DECIMAL": {"string"},
    "INT": {"int32"},
    "SHORT": {"int32"},
    "BYTE": {"int32"},
    "DATE": {"int32"},
    "LONG": {"int64"},
    "TIMESTAMP": {"int64"},
    "TIMESTAMP_NTZ": {"int64"},
    "FLOAT": {"float"},
    "DOUBLE": {"double"},
    "BOOLEAN": {"bool"},
    "BINARY": {"bytes"},
}


def _is_repeated(fd: FieldDescriptor) -> bool:
    flag = getattr(fd, "is_repeated", None)
    if flag is not None:
        return bool(flag)
    return fd.label == FieldDescriptor.LABEL_REPEATED  # pragma: no cover - old protobuf


def _is_map(fd: FieldDescriptor) -> bool:
    return fd.message_type is not None and fd.message_type.GetOptions().map_entry


def _type_causes(dtp: DataType, fd: FieldDescriptor, path: str) -> List[Cause]:
    """Causes for proto field ``fd`` (singular) against table type ``dtp``."""
    if fd.type == FieldDescriptor.TYPE_MESSAGE:
        if dtp.kind != "STRUCT":
            return [Cause(TYPE_INCOMPATIBLE, path, f"field '{path}' is a message but the column is {dtp.describe()}")]
        return _struct_causes(dtp, fd.message_type, path)
    if fd.type == FieldDescriptor.TYPE_ENUM:
        return [Cause(TYPE_INCOMPATIBLE, path, f"field '{path}' is an enum, which the table types don't accept")]
    name = _SCALAR_NAMES.get(fd.type)
    allowed = _PROTO_FOR_TYPE.get(dtp.kind, set())
    if name is None or name not in allowed:
        return [Cause(TYPE_INCOMPATIBLE, path, f"field '{path}' proto type {fd.type} does not fit {dtp.describe()}")]
    return []


def _struct_causes(dtp: DataType, msg: descriptor_pb2.DescriptorProto | Any, path: str) -> List[Cause]:
    causes: List[Cause] = []
    known = {f.name: f for f in dtp.fields}
    seen = set()
    for sub in msg.fields:
        seen.add(sub.name)
        sf = known.get(sub.name)
        if sf is None:
            causes.append(Cause(FIELD_NOT_IN_TABLE, f"{path}.{sub.name}", f"field '{path}.{sub.name}' is not a field of the struct", FIELD_NOT_IN_TABLE_CODE))
            continue
        causes.extend(_field_type_causes(sf.type, sub, f"{path}.{sub.name}"))
    for sf in dtp.fields:
        if not sf.nullable and sf.name not in seen:
            causes.append(Cause(MISSING_REQUIRED_COLUMN, f"{path}.{sf.name}", f"field '{path}.{sf.name}' is required but missing from the message"))
    return causes


def _field_type_causes(dtp: DataType, fd: FieldDescriptor, path: str) -> List[Cause]:
    if _is_map(fd):
        if dtp.kind != "MAP":
            return [Cause(TYPE_INCOMPATIBLE, path, f"field '{path}' is a map but the column is {dtp.describe()}")]
        key_fd = fd.message_type.fields_by_name["key"]
        val_fd = fd.message_type.fields_by_name["value"]
        return _type_causes(dtp.key, key_fd, f"{path} key") + _type_causes(dtp.value, val_fd, f"{path} value")
    if _is_repeated(fd):
        if dtp.kind != "ARRAY":
            return [Cause(TYPE_INCOMPATIBLE, path, f"field '{path}' is repeated but the column is {dtp.describe()}")]
        return _type_causes(dtp.value, fd, f"{path}[]")
    return _type_causes(dtp, fd, path)


def check_descriptor(snap: SchemaSnapshot, msg_desc) -> List[Cause]:
    """Causes that stop a proto stream from being created against ``snap``."""
    causes: List[Cause] = []
    names = set()
    for fd in msg_desc.fields:
        names.add(fd.name)
        col = snap.column(fd.name)
        if col is None or fd.name == snap.rescue_column:
            causes.append(
                Cause(FIELD_NOT_IN_TABLE, fd.name, f"field '{fd.name}' is not a column of {snap.table}", FIELD_NOT_IN_TABLE_CODE)
            )
            continue
        causes.extend(_field_type_causes(col.type, fd, fd.name))
    for col in snap.columns:
        if col.name == snap.rescue_column or col.nullable:
            continue
        if col.name not in names:
            causes.append(Cause(MISSING_REQUIRED_COLUMN, col.name, f"field '{col.name}' is required (non-nullable) but missing from the descriptor"))
    return causes


@dataclass
class ProtoSchema:
    message_class: Any
    descriptor: Any
    message_name: str


def _all_message_names(fdp: descriptor_pb2.FileDescriptorProto) -> Dict[str, str]:
    """Map each message's simple name to its fully qualified name (nested ones included)."""
    found: Dict[str, List[str]] = {}

    def walk(prefix: str, messages) -> None:
        for m in messages:
            full = f"{prefix}.{m.name}" if prefix else m.name
            found.setdefault(m.name, []).append(full)
            walk(full, m.nested_type)

    walk(fdp.package, fdp.message_type)
    return {k: v[0] for k, v in found.items() if len(v) == 1}


def _qualify_type_names(fdp: descriptor_pb2.FileDescriptorProto) -> None:
    """Resolve field type names that don't match the synthetic file's names by their last part.

    The SDK's descriptor is a single message. Its nested message types may be
    referenced with a package-qualified name that the mock doesn't have, so a
    reference is matched by its simple name when that name is unique.
    """
    known = _all_message_names(fdp)
    known_full = {v for v in known.values()}

    def fix(messages) -> None:
        for m in messages:
            for f in m.field:
                if f.type in (FieldDescriptor.TYPE_MESSAGE, FieldDescriptor.TYPE_ENUM) and f.type_name:
                    tn = f.type_name.lstrip(".")
                    if tn in known_full:
                        f.type_name = "." + tn
                        continue
                    simple = tn.rsplit(".", 1)[-1]
                    if simple in known:
                        f.type_name = "." + known[simple]
            fix(m.nested_type)

    fix(fdp.message_type)


def load_proto(descriptor_bytes: bytes) -> ProtoSchema:
    """Build a message class from the ``descriptor_proto`` of a create_stream request.

    The Python SDK sends a serialized ``DescriptorProto`` (the message, with its
    nested types), even when it is given a FileDescriptorProto or a generated
    Descriptor (checked with SDK 1.11.0). A serialized FileDescriptorProto is
    also accepted: its first top-level message is used.
    """
    if not descriptor_bytes:
        raise DescriptorError("descriptor_proto is empty")
    file_fdp = descriptor_pb2.FileDescriptorProto()
    try:
        file_fdp.ParseFromString(descriptor_bytes)
    except Exception:  # not a file descriptor; try the message form below
        file_fdp = None
    if file_fdp is not None and file_fdp.message_type:
        fdp = file_fdp
        message = fdp.message_type[0]
        package = fdp.package
    else:
        dp = descriptor_pb2.DescriptorProto()
        try:
            dp.ParseFromString(descriptor_bytes)
        except Exception as exc:
            raise DescriptorError(f"descriptor_proto does not parse: {exc}")
        if not dp.name and not dp.field:
            raise DescriptorError("descriptor_proto has no message")
        if not dp.name:
            dp.name = "ZerobusDynamicMessage"
        fdp = descriptor_pb2.FileDescriptorProto(name="zerobus_dynamic.proto", message_type=[dp])
        message = dp
        package = ""
    _qualify_type_names(fdp)
    pool = descriptor_pool.DescriptorPool()
    default = descriptor_pool.Default()
    for dep in fdp.dependency:
        try:
            dep_fdp = descriptor_pb2.FileDescriptorProto()
            dep_fdp.ParseFromString(default.FindFileByName(dep).serialized_pb)
            pool.Add(dep_fdp)
        except KeyError:
            raise DescriptorError(f"descriptor imports unknown file {dep!r}")
    try:
        pool.Add(fdp)
    except Exception as exc:
        raise DescriptorError(f"descriptor can't be added: {exc}")
    full = f"{package}.{message.name}" if package else message.name
    try:
        desc = pool.FindMessageTypeByName(full)
    except KeyError:
        raise DescriptorError(f"message {full} not found after loading the descriptor")
    cls = message_factory.GetMessageClass(desc)
    return ProtoSchema(cls, desc, full)


def message_to_dict(msg) -> Dict[str, Any]:
    """Convert a protobuf message to a dict of its set fields."""
    out: Dict[str, Any] = {}
    for fd, value in msg.ListFields():
        if _is_map(fd):
            vfd = fd.message_type.fields_by_name["value"]
            out[fd.name] = {
                k: _proto_value(vfd, v) for k, v in value.items()
            }
        elif _is_repeated(fd):
            out[fd.name] = [_proto_value(fd, v) for v in value]
        else:
            out[fd.name] = _proto_value(fd, value)
    return out


def _proto_value(fd: FieldDescriptor, value: Any) -> Any:
    if fd.type == FieldDescriptor.TYPE_MESSAGE:
        return message_to_dict(value)
    if fd.type == FieldDescriptor.TYPE_BYTES:
        return bytes(value)
    return value
