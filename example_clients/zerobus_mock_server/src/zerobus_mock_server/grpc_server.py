"""gRPC listener: EphemeralStream (JSON, protobuf, Avro) and Arrow Flight DoPut.

Offsets run 0, 1, 2 per stream; acks are cumulative; a failed submission ends the stream after
the earlier ones are acked; every message is checked against the token.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Tuple

import grpc
from google.protobuf import any_pb2, duration_pb2
from google.rpc import error_details_pb2, status_pb2
from grpc_status import rpc_status

from . import flight as fl
from .avro import AvroStream
from .pb import zerobus_service_pb2 as pb
from .pb import zerobus_service_pb2_grpc as pb_grpc
from .schema import FIELD_NOT_IN_TABLE, Cause, RecordRejected, check_fields, validate_record
from .tables import Snapshot
from .validation import FormatError, ProtoStream

END = object()
INVALID, NOT_FOUND = grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.NOT_FOUND
UNAUTH, DENIED = grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED
UNAVAILABLE, OUT_OF_RANGE = grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.OUT_OF_RANGE


class StreamFailure(Exception):
    def __init__(self, code: grpc.StatusCode, message: str, causes: Tuple[str, ...] = ()):
        super().__init__(f"{code.name}: {message}")
        self.code, self.message, self.causes = code, message, list(causes)  # causes: ErrorInfo tokens


@dataclass
class Submission:
    offset: int
    rows: List[dict] = field(default_factory=list)
    error: Optional[StreamFailure] = None


@dataclass
class Session:
    stream_id: str
    table: str
    snap: Snapshot
    label: str  # "grpc" or "arrow", stored with each row
    token: Optional[str]
    expected: int = 0
    acked: int = -1
    stored: int = 0
    dict_frames: List[bytes] = field(default_factory=list)


async def _fail(context, f: StreamFailure) -> None:
    if f.causes:  # a schema failure: ErrorInfo carries the cause tokens
        info = error_details_pb2.ErrorInfo(reason="SCHEMA_VALIDATION_FAILED", domain="zerobus.databricks.com")
        info.metadata["causes"] = ",".join(f.causes)
        detail = any_pb2.Any()
        detail.Pack(info)
        await context.abort_with_status(rpc_status.to_status(
            status_pb2.Status(code=f.code.value[0], message=f.message, details=[detail])))
    await context.abort(f.code, f.message)


def _md(context) -> dict:
    """Lower-cased metadata, plus the bearer token under "bearer"."""
    md = {k.lower(): v for k, v in context.invocation_metadata()}
    auth = md.get("authorization", "")
    md["bearer"] = auth[7:].strip() if auth.lower().startswith("bearer ") else None
    return md


async def _next_or_abort(requests, context, message: str):
    try:
        return await requests.__anext__()
    except StopAsyncIteration:
        await context.abort(INVALID, message)


def _json_record(raw: Any) -> dict:
    try:
        obj = json.loads(raw)
    except (TypeError, ValueError):
        obj = None
    if not isinstance(obj, dict):
        raise FormatError("record must be a JSON object")
    return obj


def _format(create, record_type: int):
    """(fields to check against the table or None, decoder, label for errors) for one record type."""
    if record_type == pb.JSON:
        return None, _json_record, ""
    if record_type == pb.PROTO:
        if not create.HasField("descriptor_proto"):
            raise FormatError("descriptor_proto is required for PROTO streams")
        proto = ProtoStream(create.descriptor_proto)
        return proto.fields, proto.decode, "the protobuf descriptor"
    if record_type == pb.AVRO:
        if not create.HasField("avro_schema_json"):
            raise FormatError("avro_schema_json is required for AVRO streams")
        avro = AvroStream(create.avro_schema_json)
        return avro.fields, avro.decode, "the Avro writer schema"
    raise FormatError(f"record type {record_type} is not supported")


async def _pump(requests, queue: asyncio.Queue, extract: Callable) -> None:
    """Reads the request stream ahead of the writer. A client that goes away ends the stream."""
    try:
        async for request in requests:
            sub = extract(request)
            if sub is not None:
                queue.put_nowait(sub)
    except Exception:
        pass
    finally:
        queue.put_nowait(END)


class ZerobusServicer(pb_grpc.ZerobusServicer):
    def __init__(self, mock) -> None:
        self.mock = mock

    async def _admit(self, context, md: dict, table: str) -> None:
        """Checks shared by both methods: table, create faults, token."""
        mock = self.mock
        header = md.get("x-databricks-zerobus-table-name")
        if not table:
            await context.abort(INVALID, "the table name is required")
        if header and header != table:
            await context.abort(INVALID, f"create names {table} but the call header names {header}")
        if not mock.tables.has(table):
            await context.abort(NOT_FOUND, f'Table "{table}" cannot be found.')
        fired = mock.scenarios.fire("create", table)
        if fired and fired[0].action == "not_found":
            await context.abort(NOT_FOUND, f'Table "{table}" cannot be found. (injected)')
        if fired:
            await context.abort(UNAVAILABLE, "injected: the server is unavailable")
        check = mock.tokens.check(md.get("bearer"), table)
        if check == "UNAUTHENTICATED":
            await context.abort(UNAUTH, "missing, unknown or expired access token")
        if check == "PERMISSION_DENIED":
            await context.abort(DENIED, f"the token was not issued for {table}")

    # ------------------------------------------------------------ EphemeralStream

    async def EphemeralStream(self, requests, context):
        await context.send_initial_metadata(())  # the SDK waits for headers before it sends create_stream
        md = _md(context)
        first = await _next_or_abort(requests, context, "the stream closed before its first message")
        if first.WhichOneof("payload") != "create_stream":
            await context.abort(INVALID, "the first message must be create_stream")
        create = first.create_stream
        table = create.table_name
        await self._admit(context, md, table)
        snap = self.mock.tables.snapshot(table)
        record_type = create.record_type if create.HasField("record_type") else pb.PROTO
        try:
            fields, decode, label = _format(create, record_type)
        except FormatError as exc:
            await context.abort(INVALID, str(exc))
        causes = check_fields(snap.columns, table, fields) if fields else []
        if causes:
            await _fail(context, StreamFailure(INVALID, f"{label} does not fit {table}: " + "; ".join(c.message for c in causes), [c.token for c in causes]))
        session = Session(self.mock.next_stream_id(), table, snap, "grpc", md["bearer"])
        yield pb.EphemeralStreamResponse(create_stream_response=pb.CreateIngestStreamResponse(stream_id=session.stream_id))
        async for event in self._run(requests, session, context, self._ephemeral_extract(session, decode)):
            if event[0] == "ack":
                yield pb.EphemeralStreamResponse(ingest_record_response=pb.IngestRecordResponse(
                    durability_ack_up_to_offset=event[1]))
            else:
                ms = event[1]
                yield pb.EphemeralStreamResponse(close_stream_signal=pb.CloseStreamSignal(
                    duration=duration_pb2.Duration(seconds=ms // 1000, nanos=ms % 1000 * 1_000_000)))

    # ------------------------------------------------------------ Arrow Flight DoPut

    async def do_put(self, requests, context):
        """Arrow Flight DoPut: a schema frame, then record batches with offsets in app_metadata."""
        await context.send_initial_metadata(())
        md = _md(context)
        first = await _next_or_abort(requests, context, "DoPut closed before the schema")
        table = md.get("x-databricks-zerobus-table-name", "")
        await self._admit(context, md, table)
        try:
            schema = fl.schema_from_header(first.data_header)
        except fl.FlightError as exc:
            await context.abort(INVALID, str(exc))
        snap = self.mock.tables.snapshot(table)
        causes = check_fields(snap.columns, table, [(f.name, fl.arrow_kind(f.type)) for f in schema])
        if causes:
            await _fail(context, StreamFailure(INVALID, f"the Arrow schema does not fit {table}: " + "; ".join(c.message for c in causes), [c.token for c in causes]))
        session = Session(self.mock.next_stream_id(), table, snap, "arrow", md["bearer"])
        header = bytes(first.data_header)
        yield fl.ready_result()
        async for event in self._run(requests, session, context, self._flight_extract(session, header)):
            if event[0] == "ack":
                yield fl.ack_result(event[1], event[2])
            else:
                yield fl.ack_result(event[2], event[3], close_ms=event[1])

    # ------------------------------------------------------------ engine

    async def _run(self, requests, session: Session, context, extract: Callable):
        """Yields ("ack", upto, stored) and ("close", ms, upto, stored) events, in order."""
        queue: asyncio.Queue = asyncio.Queue()
        reader = asyncio.ensure_future(_pump(requests, queue, extract))
        loop = asyncio.get_running_loop()
        window_end: Optional[float] = None  # set by a graceful close: store and ack until then
        try:
            while True:
                try:
                    sub = await (queue.get() if window_end is None
                                 else asyncio.wait_for(queue.get(), max(window_end - loop.time(), 0)))
                except asyncio.TimeoutError:
                    return
                if sub is END or (window_end is not None and sub.error is not None):
                    return
                if sub.error is not None:
                    raise sub.error
                close_ms = None
                if window_end is None:
                    if self.mock.tokens.check(session.token, session.table):
                        raise StreamFailure(UNAUTH, "the stream's token has expired")
                    for rule in self.mock.scenarios.fire("ingest", session.table):
                        if rule.action == "add_columns":
                            self.mock.add_schema_change(session.table)
                        elif rule.action == "abort":
                            if rule.params["persist"]:
                                self._store(session, sub)  # written but never acked: the SDK resends it
                            if rule.params["retryable"]:
                                raise StreamFailure(UNAVAILABLE, f"injected retryable stream error at offset {sub.offset}")
                            raise StreamFailure(INVALID, f"injected terminal stream error at offset {sub.offset}")
                        elif rule.action == "close":
                            close_ms = int(rule.params["duration_ms"])
                    if close_ms is not None:
                        yield ("close", close_ms, session.acked, session.stored)
                        window_end = loop.time() + close_ms / 1000.0
                self._store(session, sub)
                event = self._ack(session, sub.offset)
                if event:
                    yield event
        except StreamFailure as failure:
            await _fail(context, failure)
        finally:
            reader.cancel()

    def _store(self, session: Session, sub: Submission) -> None:
        if sub.rows:
            self.mock.tables.append(session.table, session.snap, sub.rows, protocol=session.label,
                                    stream_id=session.stream_id, offset=sub.offset)
            session.stored += len(sub.rows)

    def _ack(self, session: Session, offset: int):
        session.acked = max(session.acked, offset)
        if any(rule.action == "drop" for rule in self.mock.scenarios.fire("ack", session.table)):
            return None
        return ("ack", offset, session.stored)

    # ------------------------------------------------------------ request extractors

    def _ephemeral_extract(self, session: Session, decode: Callable):
        def extract(request):
            kind = request.WhichOneof("payload")
            if kind not in ("ingest_record", "ingest_record_batch"):
                return Submission(-1, error=StreamFailure(INVALID, "only ingest messages may follow create_stream"))
            body = getattr(request, kind)
            if not body.HasField("offset_id"):
                return Submission(-1, error=StreamFailure(INVALID, "offset_id is required"))
            offset = body.offset_id
            if offset != session.expected:
                return Submission(offset, error=StreamFailure(INVALID, f"expected offset_id {session.expected}, got {offset}"))
            session.expected += 1
            batch = kind == "ingest_record_batch"
            oneof = body.WhichOneof("batch" if batch else "record")
            if oneof is None:
                return Submission(offset, error=StreamFailure(INVALID, "ingest message has no record"))
            payloads = list(getattr(body, oneof).records) if batch else [getattr(body, oneof)]
            size = lambda raw: len(raw.encode("utf-8")) if isinstance(raw, str) else len(raw)  # noqa: E731
            return self._records(session, offset, payloads, decode, size)
        return extract

    def _flight_extract(self, session: Session, header: bytes):
        def extract(frame):
            frame_bytes = fl.ipc_frame(bytes(frame.data_header), bytes(frame.data_body))
            if not frame.app_metadata:  # a dictionary batch: sent in front of the next record batch
                session.dict_frames.append(frame_bytes)
                return None
            try:
                offset = fl.batch_offset(bytes(frame.app_metadata))
                if offset != session.expected:
                    return Submission(offset, error=StreamFailure(INVALID, f"expected offset_id {session.expected}, got {offset}"))
                session.expected += 1
                batch = fl.batch_from_frames(header, session.dict_frames + [frame_bytes])
            except fl.FlightError as exc:
                return Submission(-1, error=StreamFailure(INVALID, str(exc)))
            session.dict_frames = []
            return self._records(session, offset, batch.to_pylist(), lambda row: row,
                                 lambda row: len(json.dumps(row, default=str).encode("utf-8")))
        return extract

    def _records(self, session: Session, offset: int, payloads: list, decode: Callable, size_of: Callable) -> Submission:
        """Injected record faults, then the size limit, decoding and the table check."""
        for rule in self.mock.scenarios.fire("record", session.table):
            if rule.action == "too_large":
                return Submission(offset, error=StreamFailure(
                    OUT_OF_RANGE, f"offset {offset} exceeds the maximum message size (injected)"))
            return Submission(offset, error=StreamFailure(
                INVALID, f"offset {offset} does not fit {session.table} (injected)", [FIELD_NOT_IN_TABLE]))
        limit = self.mock.cfg.max_payload_bytes
        rows: List[dict] = []
        for index, raw in enumerate(payloads):
            size = size_of(raw)
            if size > limit:
                return Submission(offset, error=StreamFailure(
                    OUT_OF_RANGE, f"offset {offset} record {index} is {size} bytes, over the maximum message size of {limit}"))
            try:
                rows.append(validate_record(session.snap.columns, session.table, decode(raw)))
            except FormatError as exc:
                return Submission(offset, error=StreamFailure(INVALID, f"offset {offset} record {index}: {exc}"))
            except RecordRejected as exc:
                return Submission(offset, error=StreamFailure(INVALID, f"offset {offset} record {index} does not fit "
                                  f"{session.table}: " + "; ".join(c.message for c in exc.causes),
                                  [c.token for c in exc.causes]))
        return Submission(offset, rows=rows)


def flight_handler(servicer: ZerobusServicer):
    """A generic gRPC handler that serves Arrow Flight DoPut on the same server."""
    return grpc.method_handlers_generic_handler(
        fl.FLIGHT_SERVICE,
        {"DoPut": grpc.stream_stream_rpc_method_handler(
            servicer.do_put,
            request_deserializer=fl.FlightData.FromString,
            response_serializer=fl.PutResult.SerializeToString)},
    )
