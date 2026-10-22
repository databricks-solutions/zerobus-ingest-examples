# Copyright (c) 2014-2022 Cirrus Link Solutions and others
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0.
#
# SPDX-License-Identifier: EPL-2.0
#
# A Python translation of Eclipse Tahu 1.0.21 (https://github.com/eclipse-tahu/tahu, tag v1.0.21):
# org.eclipse.tahu.message.SparkplugBPayloadDecoder, the JSON shape of the
# org.eclipse.tahu.message.model classes (Jackson annotations and defaults),
# org.eclipse.tahu.json.FileSerializer and org.eclipse.tahu.util.PayloadUtil.decompress.
"""Sparkplug B payload bytes -> JSON identical to Eclipse Tahu's ``PayloadUtil.toJsonString``.

This is the JSON the connector writes to ``sparkplug_raw.msg``. Tahu's behaviour is copied as is, including its quirks
(each marked ``Tahu quirk``); the one deliberate difference is marked ``Deviation``.

    to_tahu_json(payload: bytes) -> str     # raises on a payload Tahu can't decode or serialize
"""
from __future__ import annotations

import base64
import logging
import math
import struct
import zlib
from typing import Any

from google.protobuf.message import DecodeError

from sparkplug_connector.sparkplug import sparkplug_b_decode_pb2 as pb   # strings as bytes: see _utf8
from sparkplug_connector.sparkplug.java_format import (
    java_hashmap_order,
    java_utf8_decode,
    json_double,
    json_float,
    json_string,
)

__all__ = ["to_tahu_json", "TahuDecodeError", "UUID_COMPRESSED"]

logger = logging.getLogger(__name__)

UUID_COMPRESSED = "SPBV1.0_COMPRESSED"
_METRIC_ALGORITHM = "algorithm"
# Deviation (Java Tahu has no limit): a compressed body may expand to at most this many bytes, else the payload is
# undecodable. Its JSON row would be at least as large, and Zerobus refuses a record over 10,420,224 bytes, so
# nothing storable is refused; without a limit a small payload from any publisher could expand to gigabytes.
MAX_DECOMPRESSED_BYTES = 16 * 1024 * 1024
# java.util.prefs.Preferences.MAX_VALUE_LENGTH: FileSerializer goes through Paho's Base64, which stores the text in a Preferences node
_FILE_BASE64_MAX_LENGTH = 8 * 1024
_LONG_MIN = -(1 << 63)


class TahuDecodeError(ValueError):
    """The payload can't be decoded or serialized (Tahu would throw)."""


# Type codes and names (MetricDataType, DataSetDataType, ParameterDataType, PropertyDataType)
_SCALAR_NAMES = {1: "Int8", 2: "Int16", 3: "Int32", 4: "Int64", 5: "UInt8", 6: "UInt16", 7: "UInt32", 8: "UInt64",
                 9: "Float", 10: "Double", 11: "Boolean", 12: "String", 13: "DateTime", 14: "Text"}
_METRIC_NAMES = {**_SCALAR_NAMES, 15: "UUID", 16: "DataSet", 17: "Bytes", 18: "File", 19: "Template",
                 22: "Int8Array", 23: "Int16Array", 24: "Int32Array", 25: "Int64Array", 26: "UInt8Array",
                 27: "UInt16Array", 28: "UInt32Array", 29: "UInt64Array", 30: "FloatArray", 31: "DoubleArray",
                 32: "BooleanArray", 33: "StringArray", 34: "DateTimeArray"}
_PROPERTY_NAMES = {**_SCALAR_NAMES, 20: "PropertySet", 21: "PropertySetList"}


# ----------------------------------------------------------------------------- Java values

class _Float:
    """A Java ``float`` (written with ``Float.toString``)."""
    __slots__ = ("value",)

    def __init__(self, value: float):
        self.value = value


class _Bytes:
    """A Java ``byte[]`` (Jackson writes it as base64)."""
    __slots__ = ("value",)

    def __init__(self, value: bytes):
        self.value = value


class _File:
    """A Tahu ``File`` (written by ``FileSerializer``)."""
    __slots__ = ("value",)

    def __init__(self, value: bytes):
        self.value = value


class _PropertyMap:
    """A Tahu ``PropertySet`` (a ``java.util.HashMap``, written in its iteration order)."""
    __slots__ = ("entries",)

    def __init__(self, entries: list[tuple[str, Any]]):
        self.entries = entries


def _utf8(raw: bytes) -> str:
    """A protobuf string as Java reads it: invalid UTF-8 becomes U+FFFD (Python's protobuf would raise)."""
    return java_utf8_decode(raw)


def _s8(v: int) -> int:
    return ((v & 0xFF) ^ 0x80) - 0x80


def _s16(v: int) -> int:
    return ((v & 0xFFFF) ^ 0x8000) - 0x8000


def _s32(v: int) -> int:
    """A protobuf uint32 as Java reads it (``int``)."""
    return ((v & 0xFFFFFFFF) ^ 0x80000000) - 0x80000000


def _s64(v: int) -> int:
    """A protobuf uint64 as Java reads it (``long``)."""
    return ((v & 0xFFFFFFFFFFFFFFFF) ^ 0x8000000000000000) - 0x8000000000000000


# ----------------------------------------------------------------------------- decoding

def _decode_payload(raw: bytes) -> dict[str, Any]:
    """SparkplugBPayloadDecoder.buildFromByteArray (no MetricDataTypeMap); keys in Jackson's order."""
    try:
        payload = pb.Payload.FromString(raw)
    except DecodeError as e:
        raise TahuDecodeError(f"not a Sparkplug B protobuf payload: {e}") from e
    out: dict[str, Any] = {}
    if payload.HasField("timestamp"):
        out["timestamp"] = _s64(payload.timestamp)
    metrics = []
    for metric in payload.metrics:
        try:
            metrics.append(_metric(metric))
        except Exception as e:  # noqa: BLE001 - Tahu quirk: a metric that fails to decode is left out, the rest is kept
            logger.warning("Failed to handle metric name=%s with alias=%s - excluding from decoded payload: %s",
                           _utf8(metric.name), metric.alias, e)
    out["metrics"] = metrics
    if payload.HasField("seq"):
        out["seq"] = _s64(payload.seq)
    if payload.HasField("uuid"):
        out["uuid"] = _utf8(payload.uuid)
    if payload.HasField("body"):
        out["body"] = _Bytes(payload.body)
    return out


def _metric(m: pb.Payload.Metric) -> dict[str, Any] | None:
    type_code = _s32(m.datatype)
    type_name = _METRIC_NAMES.get(type_code)
    if type_name is None:
        # Tahu quirk: an unknown data type (0, 20, 21, ...) decodes to null, kept as null in the list
        logger.error("Failed to decode the payload on metric datatype: %s", _utf8(m.name))
        return None
    value = _metric_value(m, type_code)
    out: dict[str, Any] = {}
    if m.HasField("name"):
        out["name"] = _utf8(m.name)
    if m.HasField("alias"):
        out["alias"] = _s64(m.alias)
    if m.HasField("timestamp"):
        out["timestamp"] = _s64(m.timestamp)
    out["dataType"] = type_name
    if m.HasField("is_historical"):
        out["isHistorical"] = m.is_historical
    if m.HasField("is_transient"):
        out["isTransient"] = m.is_transient
    if m.HasField("metadata"):
        out["metaData"] = _metadata(m.metadata)
    if m.HasField("properties"):
        properties = _properties(m.properties)
        if properties.entries:                      # @JsonInclude(NON_EMPTY)
            out["properties"] = properties
    out["value"] = value                            # @JsonInclude(ALWAYS): null too
    return out


def _metadata(md: pb.Payload.MetaData) -> dict[str, Any]:
    # Tahu quirk: every field is copied with its protobuf default when unset ("", 0, false)
    return {
        "contentType": _utf8(md.content_type),
        "size": _s64(md.size),
        "seq": _s64(md.seq),
        "fileName": _utf8(md.file_name),
        "fileType": _utf8(md.file_type),
        "md5": _utf8(md.md5),
        "description": _utf8(md.description),
        "multiPart": md.is_multi_part,              # Jackson's name for isMultiPart(), written last
    }


def _metric_value(m: pb.Payload.Metric, t: int) -> Any:
    """SparkplugBPayloadDecoder.getMetricValue: unset values read as protobuf defaults (0, "", false)."""
    if m.is_null:
        return None
    if t == 11:
        return m.boolean_value
    if t == 13:
        return _s64(m.long_value)                   # Date -> epoch ms
    if t == 18:
        return _File(m.bytes_value)
    if t == 9:
        return _Float(m.float_value)
    if t == 10:
        return m.double_value
    if t == 1:
        return _s8(m.int_value)
    if t == 2:
        return _s16(m.int_value)
    if t == 3:
        return _s32(m.int_value)
    if t == 4:
        return _s64(m.long_value)
    if t == 5:
        return _s16(m.int_value)                    # Tahu quirk: (short) cast, no masking to 8 bits
    if t == 6:
        return _s32(m.int_value)
    if t == 7:
        if m.HasField("int_value"):
            return m.int_value
        if m.HasField("long_value"):
            return _s64(m.long_value)
        raise TahuDecodeError("Failed to decode: UInt32 MetricDataType without an int or long value")
    if t == 8:
        return m.long_value
    if t in (12, 14, 15):
        return _utf8(m.string_value)
    if t == 17:
        return _Bytes(m.bytes_value)
    if t == 16:
        return _dataset(m.dataset_value)
    if t == 19:
        return _template(m.template_value)
    if 22 <= t <= 34:
        return _array(t, m.bytes_value)
    raise TahuDecodeError(f"Failed to decode: Unknown MetricDataType {t}")


def _template(t: pb.Payload.Template) -> dict[str, Any]:
    parameters = [_parameter(p) for p in t.parameters]
    # Tahu quirk: a member that fails to decode fails the whole template metric (no catch here)
    metrics = [_metric(m) for m in t.metrics]
    out: dict[str, Any] = {
        "version": _utf8(t.version),                       # Tahu quirk: "" when unset (read without has-check)
        "reference": _utf8(t.template_ref),
        "isDefinition": t.is_definition,
        "metrics": metrics,
    }
    if parameters:                                  # @JsonInclude(NON_EMPTY)
        out["parameters"] = parameters
    return out


def _parameter(p: pb.Payload.Template.Parameter) -> dict[str, Any]:
    type_code = _s32(p.type)
    value = _parameter_value(p, type_code)
    out: dict[str, Any] = {"name": _utf8(p.name), "type": _SCALAR_NAMES.get(type_code, "Unknown")}
    if value is not None and value != "":           # @JsonInclude(NON_EMPTY): null and "" left out
        out["value"] = value
    return out


def _parameter_value(p: pb.Payload.Template.Parameter, t: int) -> Any:
    if t == 11:
        return p.boolean_value
    if t == 13:
        return _s64(p.long_value)
    if t == 9:
        return _Float(p.float_value)
    if t == 10:
        return p.double_value
    if t == 1:
        return _s8(p.int_value)
    if t == 2:
        return _s16(p.int_value)
    if t == 3:
        return _s32(p.int_value)
    if t == 4:
        return _s64(p.long_value)
    if t == 5:
        return _s16(p.int_value)
    if t == 6:
        return _s32(p.int_value)
    if t == 7:
        if p.HasField("int_value"):
            return p.int_value
        if p.HasField("long_value"):
            return _s64(p.long_value)
        raise TahuDecodeError("Failed to decode: UInt32 Parameter Type without an int or long value")
    if t == 8:
        return p.long_value
    if t in (12, 14):
        return _utf8(p.string_value)
    raise TahuDecodeError(f"Failed to decode: Unknown Parameter Type {t}")


def _properties(ps: pb.Payload.PropertySet) -> _PropertyMap:
    entries = []
    values = ps.values
    for i, key in enumerate(ps.keys):
        if i >= len(values):
            raise TahuDecodeError("PropertySet has fewer values than keys")
        pv = values[i]
        type_code = _s32(pv.type)
        entries.append((_utf8(key), {"type": _PROPERTY_NAMES.get(type_code, "Unknown"), "value": _property_value(pv, type_code)}))
    return _PropertyMap(java_hashmap_order(entries))


def _property_value(pv: pb.Payload.PropertyValue, t: int) -> Any:
    if pv.is_null:
        return None
    if t == 11:
        return pv.boolean_value
    if t == 13:
        return _s64(pv.long_value)
    if t == 10:
        return pv.double_value
    if t == 9:
        return _Float(pv.float_value)
    if t == 1:
        return _s8(pv.int_value)
    if t == 2:
        return _s16(pv.int_value)
    if t == 3:
        return _s32(pv.int_value)
    if t == 4:
        return _s64(pv.long_value)
    if t == 5:
        return _s16(pv.int_value)
    if t == 6:
        return _s32(pv.int_value)
    if t == 7:
        if pv.HasField("int_value"):
            return pv.int_value
        if pv.HasField("long_value"):
            return _s64(pv.long_value)
        raise TahuDecodeError("Failed to decode: Invalid value for UInt32 datatype")
    if t == 8:
        return pv.long_value
    if t in (12, 14):
        return _utf8(pv.string_value)
    if t == 20:
        return _properties(pv.propertyset_value)
    if t == 21:
        return [_properties(s) for s in pv.propertysets_value.propertyset]
    raise TahuDecodeError(f"Failed to decode: Unknown PropertyDataType {t}")


def _dataset(ds: pb.Payload.DataSet) -> dict[str, Any]:
    columns = _s64(ds.num_of_columns)
    names = [_utf8(c) for c in ds.columns]
    type_codes = [_s32(t) for t in ds.types]
    rows = []
    for row in ds.rows:
        values = []
        for i, element in enumerate(row.elements):
            if i >= len(type_codes):
                raise TahuDecodeError("DataSet row has more values than types")
            values.append(_dataset_value(type_codes[i], element))
        rows.append(values)
    # DataSetBuilder.validate
    if len(names) != columns:
        raise TahuDecodeError(f"Invalid number of columns in data set column names: {len(names)} vs expected {columns}")
    if len(type_codes) != columns:
        raise TahuDecodeError(f"Invalid number of columns in data set types: {len(type_codes)} vs expected: {columns}")
    if type_codes:
        for values in rows:
            if len(values) != columns:
                raise TahuDecodeError(f"Invalid number of columns in data set row: {len(values)} vs expected: {columns}")
    return {
        "rows": rows,
        "numberOfColumns": columns,
        "columnNames": names,
        "types": [_SCALAR_NAMES.get(c, "Unknown") for c in type_codes],
    }


def _dataset_value(t: int, v: pb.Payload.DataSet.DataSetValue) -> Any:
    """convertDataSetValue: a cell without a value of its type is null."""
    if t == 11:
        return v.boolean_value if v.HasField("boolean_value") else None
    if t == 13:
        if not v.HasField("long_value"):
            return None
        epoch_ms = _s64(v.long_value)
        return None if epoch_ms == _LONG_MIN else epoch_ms     # Tahu quirk: Long.MIN_VALUE means null
    if t == 9:
        return _Float(v.float_value) if v.HasField("float_value") else None
    if t == 10:
        return v.double_value if v.HasField("double_value") else None
    if t == 1:
        return _s8(v.int_value) if v.HasField("int_value") else None
    if t == 2:
        return _s16(v.int_value) if v.HasField("int_value") else None
    if t == 3:
        return _s32(v.int_value) if v.HasField("int_value") else None
    if t == 4:
        return _s64(v.long_value) if v.HasField("long_value") else None
    if t == 5:
        return _s16(v.int_value) if v.HasField("int_value") else None
    if t == 6:
        return _s32(v.int_value) if v.HasField("int_value") else None
    if t == 7:
        if v.HasField("int_value"):
            return v.int_value
        return _s64(v.long_value) if v.HasField("long_value") else None
    if t == 8:
        return v.long_value if v.HasField("long_value") else None
    if t in (12, 14):
        if not v.HasField("string_value"):
            return None
        text = _utf8(v.string_value)
        return None if text == "null" else text   # Tahu quirk: the string "null" is null
    raise TahuDecodeError(f"Failed to decode DataSetDataType: {t}")


_ARRAY_FORMATS = {22: "b", 23: "h", 24: "i", 25: "q", 26: "b", 27: "H", 28: "I", 29: "Q", 30: "f", 31: "d", 34: "q"}


def _array(t: int, data: bytes) -> list[Any]:
    if t == 32:
        return _boolean_array(data)
    if t == 33:
        return _string_array(data)
    fmt = _ARRAY_FORMATS[t]
    size = struct.calcsize(fmt)
    if len(data) % size:
        raise TahuDecodeError("BufferUnderflowException: array bytes are not a whole number of values")
    values = list(struct.unpack(f"<{len(data) // size}{fmt}", data))
    # Tahu quirk (a bug): UInt8Array is read as signed bytes; (short) (0x10000 + v) wraps back to v, so 200 -> -56
    if t == 30:
        return [_Float(v) for v in values]
    return values


def _boolean_array(data: bytes) -> list[bool]:
    if len(data) < 4:
        raise TahuDecodeError("BufferUnderflowException: BooleanArray without its 4-byte count")
    count = int.from_bytes(data[:4], "little", signed=True)
    byte_count = math.ceil(count / 8) if count > 0 else 0
    if 4 + byte_count > len(data):
        raise TahuDecodeError("BufferUnderflowException: BooleanArray shorter than its count")
    values = []
    for i in range(byte_count):
        byte = data[4 + i]
        for j in range(8):
            if i * 8 + j < count:
                values.append(bool(byte & (1 << (7 - j))))
    return values


def _string_array(data: bytes) -> list[str]:
    # Each 0 byte ends a string; bytes after the last 0 are dropped. Java decodes each string followed by
    # zero padding (a buffer of the whole array's size) and then removes the NULs: one 0 byte has the same effect
    values, start = [], 0
    for i, b in enumerate(data):
        if b == 0:
            values.append(java_utf8_decode(data[start:i] + b"\x00")[:-1])
            start = i + 1
    return values


# ----------------------------------------------------------------------------- decompression

def _decompress(payload: dict[str, Any]) -> dict[str, Any]:
    """PayloadUtil.decompress: the body, compressed with the "algorithm" metric's method (DEFLATE by default)."""
    algorithm = "DEFLATE"
    for metric in payload["metrics"]:
        if metric is None or "name" not in metric:
            raise TahuDecodeError("NullPointerException: compressed payload with a metric without a name")
        if metric["name"] == _METRIC_ALGORITHM:
            value = metric["value"]
            if not isinstance(value, str) or value not in ("GZIP", "DEFLATE"):
                raise TahuDecodeError(f"Unknown or unsupported algorithm {value!r}")
            algorithm = value
    body = payload.get("body")
    if body is None:
        raise TahuDecodeError("NullPointerException: compressed payload without a body")
    return _decode_payload(_gunzip(body.value) if algorithm == "GZIP" else _inflate(body.value))


def _too_large(size: int) -> None:
    if size > MAX_DECOMPRESSED_BYTES:
        raise TahuDecodeError(f"compressed body expands to more than {MAX_DECOMPRESSED_BYTES} bytes")


def _gunzip(data: bytes) -> bytes:
    """GZIPInputStream: one or more members; bytes after the last member that aren't a member are ignored."""
    out, rest, size = [], data, 0
    while True:
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            out.append(d.decompress(rest, MAX_DECOMPRESSED_BYTES - size + 1))
        except zlib.error as e:
            if out:
                break
            raise TahuDecodeError(f"GZIP: {e}") from e
        size += len(out[-1])
        _too_large(size)                            # across all members
        if not d.eof:
            raise TahuDecodeError("GZIP: Unexpected end of ZLIB input stream")
        rest = d.unused_data
        if len(rest) < 2 or rest[:2] != b"\x1f\x8b":
            break
    return b"".join(out)


def _inflate(data: bytes) -> bytes:
    d = zlib.decompressobj()
    try:
        out = d.decompress(data, MAX_DECOMPRESSED_BYTES + 1)
    except zlib.error as e:
        raise TahuDecodeError(f"DataFormatException: {e}") from e
    _too_large(len(out))
    if not d.eof:
        # Deviation: Tahu's inflateBytes loops forever here (Inflater not finished and no more input)
        raise TahuDecodeError("DEFLATE: truncated body (Java Tahu hangs on this payload)")
    return out


# ----------------------------------------------------------------------------- JSON

def _write(value: Any, out: list[str]) -> None:
    if value is None:
        out.append("null")
    elif value is True:
        out.append("true")
    elif value is False:
        out.append("false")
    elif isinstance(value, int):
        out.append(str(value))
    elif isinstance(value, str):
        out.append(json_string(value))
    elif isinstance(value, float):
        out.append(json_double(value))
    elif isinstance(value, _Float):
        out.append(json_float(value.value))
    elif isinstance(value, dict):
        _write_object(value.items(), out)
    elif isinstance(value, _PropertyMap):
        _write_object(value.entries, out)
    elif isinstance(value, list):
        out.append("[")
        for i, item in enumerate(value):
            if i:
                out.append(",")
            _write(item, out)
        out.append("]")
    elif isinstance(value, _Bytes):
        out.append('"' + base64.b64encode(value.value).decode("ascii") + '"')
    elif isinstance(value, _File):
        text = base64.b64encode(value.value).decode("ascii")
        if len(text) > _FILE_BASE64_MAX_LENGTH:
            # Tahu quirk: FileSerializer fails for files over 6 KB (Preferences value too long)
            raise TahuDecodeError(f"File value too long to serialize: {len(text)} base64 characters")
        out.append('"' + text + '"')
    else:
        raise TypeError(f"unexpected value {value!r}")


def _write_object(items, out: list[str]) -> None:
    out.append("{")
    for i, (key, item) in enumerate(items):
        if i:
            out.append(",")
        out.append(json_string(key))
        out.append(":")
        _write(item, out)
    out.append("}")


def to_tahu_json(payload: bytes) -> str:
    """The JSON Tahu writes for a Sparkplug B payload (decompressed first if compressed)."""
    decoded = _decode_payload(payload)
    if decoded.get("uuid") == UUID_COMPRESSED:
        decoded = _decompress(decoded)
    out: list[str] = []
    _write(decoded, out)
    return "".join(out)
