"""gRPC listener: EphemeralStream, PersistentStream, RetireStream, and Arrow Flight DoPut.

Three front-ends share one stream engine (``_read`` / ``_write``):

* ``EphemeralStream`` (``databricks.zerobus.Zerobus``): the first message is
  ``create_stream``. Record types are JSON, PROTO and AVRO; an ingest message carries
  one record or a batch, with an ``offset_id``.
* ``PersistentStream``: the same ingest model, opened with ``create_stream`` or
  ``resume_stream`` (by stream ID). A resumed stream continues from the last committed
  offset plus one. ``RetireStream`` ends a persistent stream for good.
* ``arrow.flight.protocol.FlightService/DoPut``: the Arrow Flight path (see flight.py).
  The table and the token are in the gRPC headers; the first FlightData is the schema.

Protocol rules that the mock enforces (see README, differences):

* offsets are 0, 1, 2, ... per connection (for a resumed persistent stream, from the
  committed offset plus one); a submission is one message, whether a record or a batch;
* acks are cumulative: ``durability_ack_up_to_offset`` (Zerobus) or
  ``ack_up_to_offset`` / ``ack_up_to_records`` (Flight);
* a validation failure stops the stream after the earlier accepted records are stored
  and acknowledged.

All three protocols evaluate the same fault points: ``create`` (admission),
``record`` (before validation), ``ingest`` (before a submission is stored) and ``ack``
(before an acknowledgement is sent). Arrow Flight and Avro count as the "grpc" protocol
for fault scoping; rows are labelled "arrow" or "grpc" in the read API.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable, List, Optional

import grpc
from google.protobuf import any_pb2, duration_pb2
from google.protobuf.message import DecodeError
from google.rpc import error_details_pb2, status_pb2
from grpc_status import rpc_status

from . import avro as avro_mod
from . import flight as fl
from .pb import zerobus_service_pb2 as pb
from .pb import zerobus_service_pb2_grpc as pb_grpc
from .state import MockState
from .validation import (
    DescriptorError,
    ProtoSchema,
    RecordRejected,
    check_descriptor,
    load_proto,
    message_to_dict,
    validate_object,
)

END = object()
GROUP_MAX = 512
QUEUE_MAX = 10_000
SCHEMA_REASON = "SCHEMA_VALIDATION_FAILED"
SCHEMA_DOMAIN = "zerobus.databricks.com"
PROTOCOL_GRPC = "grpc"
PROTOCOL_ARROW = "arrow"


@dataclass
class StreamFailure(Exception):
    code: grpc.StatusCode
    message: str
    causes: List[str] = field(default_factory=list)
    error_code: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.code.name}: {self.message}"


@dataclass
class _Submission:
    offset: int
    rows: List[dict]
    error: Optional[StreamFailure] = None


@dataclass
class _Session:
    stream_id: str
    table: str
    snap: Any
    kind: str  # "json" | "proto" | "avro" | "arrow"
    label: str  # protocol label stored with each row
    proto: Optional[ProtoSchema] = None
    avro: Optional[avro_mod.AvroStream] = None
    arrow_header: Optional[bytes] = None
    dict_frames: List[bytes] = field(default_factory=list)
    persistent_id: Optional[str] = None
    expected_offset: int = 0
    acked_offset: int = -1
    records_stored: int = 0
    drop_budget: int = 0  # acks still to drop; -1 = every ack until the stream closes


def _schema_failure(message: str, causes: List[str], error_code: Optional[str]) -> StreamFailure:
    return StreamFailure(grpc.StatusCode.INVALID_ARGUMENT, message, causes, error_code)


def _rejection(exc: RecordRejected, prefix: str = "") -> StreamFailure:
    tokens = [c.token for c in exc.causes]
    codes = [c.error_code for c in exc.causes if c.error_code]
    return _schema_failure(prefix + exc.render(), tokens, codes[0] if codes else None)


def _status_with_info(failure: StreamFailure):
    info = error_details_pb2.ErrorInfo(reason=SCHEMA_REASON, domain=SCHEMA_DOMAIN)
    info.metadata["causes"] = ",".join(failure.causes)
    if failure.error_code:
        info.metadata["error_code"] = failure.error_code
    detail = any_pb2.Any()
    detail.Pack(info)
    status = status_pb2.Status(code=failure.code.value[0], message=failure.message, details=[detail])
    return rpc_status.to_status(status)


def _duration(seconds: float):
    d = duration_pb2.Duration()
    d.FromTimedelta(timedelta(seconds=seconds))
    return d


def _metadata(context) -> dict:
    return {k.lower(): v for k, v in context.invocation_metadata()}


def _bearer(md: dict) -> Optional[str]:
    auth = md.get("authorization", "")
    return auth[7:].strip() if auth.lower().startswith("bearer ") else None


class ZerobusServicer(pb_grpc.ZerobusServicer):
    def __init__(self, state: MockState):
        self.state = state

    # ------------------------------------------------------------ admission

    async def _admit(self, context, md: dict, table: str, *, label: str) -> None:
        """Checks shared by every front-end: table, create faults, token. Aborts on failure."""
        state = self.state
        if not table:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "the table name is required")
        header_table = md.get("x-databricks-zerobus-table-name")
        if header_table and header_table != table:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"create names {table} but the call header names {header_table}",
            )
        if not state.tables.has(table):
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Table \"{table}\" cannot be found.")
        for firing in state.registry.evaluate("create", PROTOCOL_GRPC, table):
            f = firing.fault
            if f.type == "grpc_status":
                code = getattr(grpc.StatusCode, str(f.params["code"]).upper())
                state.log_firing(firing, code.name, f.params.get("message", ""))
                await context.abort(code, f.params.get("message") or "injected stream-creation failure")
            if f.type == "unauthorized":
                state.log_firing(firing, "UNAUTHENTICATED")
                await context.abort(grpc.StatusCode.UNAUTHENTICATED, "injected: token rejected")
            if f.type == "permission_denied":
                state.log_firing(firing, "PERMISSION_DENIED")
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, "injected: caller lacks privileges")
            if f.type == "table_not_found":
                state.log_firing(firing, "NOT_FOUND")
                await context.abort(grpc.StatusCode.NOT_FOUND, f"Table \"{table}\" cannot be found. (injected)")
        check = state.tokens.check(_bearer(md), table)
        if check == "UNAUTHENTICATED":
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing, unknown or expired access token")
        if check == "PERMISSION_DENIED":
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, f"the token was not issued for {table}")

    async def _admit_create(self, context, md: dict, create, *, persistent_id: Optional[str] = None) -> _Session:
        """Admit a create_stream request (Zerobus record types) and return its session."""
        table = create.table_name if create.HasField("table_name") else ""
        await self._admit(context, md, table, label=PROTOCOL_GRPC)
        snap = self.state.tables.snapshot(table)
        record_type = create.record_type if create.HasField("record_type") else pb.PROTO
        if record_type == pb.RECORD_TYPE_UNSPECIFIED:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "record_type must be set")
        session = _Session(
            stream_id=persistent_id or self.state.next_stream_id(),
            table=table,
            snap=snap,
            kind="json",
            label=PROTOCOL_GRPC,
            persistent_id=persistent_id,
        )
        if record_type == pb.JSON:
            session.kind = "json"
        elif record_type == pb.PROTO:
            session.kind = "proto"
            if not create.HasField("descriptor_proto"):
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "descriptor_proto is required for PROTO streams")
            try:
                session.proto = load_proto(create.descriptor_proto)
            except DescriptorError as exc:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"descriptor_proto: {exc}")
            causes = check_descriptor(snap, session.proto.descriptor)
            if causes:
                failure = _rejection(RecordRejected(causes), prefix="descriptor does not fit the table: ")
                await context.abort_with_status(_status_with_info(failure))
        elif record_type == pb.AVRO:
            session.kind = "avro"
            if not create.HasField("avro_schema_json"):
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "avro_schema_json is required for AVRO streams")
            try:
                session.avro = avro_mod.load_writer_schema(create.avro_schema_json)
            except avro_mod.AvroSchemaError as exc:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"avro_schema_json: {exc}")
            causes = avro_mod.check_against_table(snap, session.avro)
            if causes:
                failure = _rejection(RecordRejected(causes), prefix="Avro writer schema does not fit the table: ")
                await context.abort_with_status(_status_with_info(failure))
        else:
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, f"record type {record_type} is not supported")
        return session

    # ------------------------------------------------------------ EphemeralStream

    async def EphemeralStream(self, request_iterator, context):
        # The SDK core waits for response headers before it sends create_stream, so the
        # headers go out first (a server that waits for create_stream would deadlock).
        await context.send_initial_metadata(())
        md = _metadata(context)
        first = await _first(request_iterator, context)
        if first.WhichOneof("payload") != "create_stream":
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "the first message must be create_stream")
        session = await self._admit_create(context, md, first.create_stream)
        yield pb.EphemeralStreamResponse(
            create_stream_response=pb.CreateIngestStreamResponse(stream_id=session.stream_id)
        )
        async for event in self._run(request_iterator, session, context, _extract_ephemeral(self, session)):
            if event[0] == "ack":
                yield pb.EphemeralStreamResponse(
                    ingest_record_response=pb.IngestRecordResponse(durability_ack_up_to_offset=event[1]))
            else:
                yield pb.EphemeralStreamResponse(close_stream_signal=pb.CloseStreamSignal(duration=_duration(event[1] / 1000)))

    # ------------------------------------------------------------ PersistentStream

    async def PersistentStream(self, request_iterator, context):
        await context.send_initial_metadata(())
        md = _metadata(context)
        first = await _first(request_iterator, context)
        kind = first.WhichOneof("payload")
        if kind == "create_stream":
            create = first.create_stream.create_stream
            session = await self._admit_create(context, md, create)
            entry = {"table": session.table, "kind": session.kind, "descriptor": create.descriptor_proto if create.HasField("descriptor_proto") else b"",
                     "avro": create.avro_schema_json if create.HasField("avro_schema_json") else "",
                     "committed": None, "retired": False}
            session.persistent_id = session.stream_id
            with self.state._lock:
                self.state.persistent[session.stream_id] = entry
            response = pb.PersistentStreamResponse(
                create_stream_response=pb.CreateIngestStreamResponse(stream_id=session.stream_id))
        elif kind == "resume_stream":
            resume = first.resume_stream
            stream_id = resume.stream_id
            entry = self.state.persistent.get(stream_id)
            if entry is None:
                await context.abort(grpc.StatusCode.NOT_FOUND, f"no persistent stream {stream_id}")
            if entry["retired"]:
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "a retired stream can never be resumed")
            table = entry["table"]
            await self._admit(context, md, table, label=PROTOCOL_GRPC)
            session = _Session(stream_id=stream_id, table=table, snap=self.state.tables.snapshot(table),
                               kind=entry["kind"], label=PROTOCOL_GRPC, persistent_id=stream_id)
            if session.kind == "proto":
                session.proto = load_proto(entry["descriptor"])
            if session.kind == "avro":
                session.avro = avro_mod.load_writer_schema(entry["avro"])
            committed = entry["committed"]
            session.expected_offset = 0 if committed is None else committed + 1
            session.acked_offset = -1 if committed is None else committed
            response = pb.PersistentStreamResponse(
                resume_stream_response=pb.ResumeIngestStreamResponse(
                    last_committed_offset=committed) if committed is not None else pb.ResumeIngestStreamResponse())
        else:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "the first message must be create_stream or resume_stream")
        yield response
        async for event in self._run(request_iterator, session, context, _extract_persistent(self, session)):
            if event[0] == "ack":
                yield pb.PersistentStreamResponse(
                    ingest_record_response=pb.IngestRecordResponse(durability_ack_up_to_offset=event[1]))
            else:
                yield pb.PersistentStreamResponse(close_stream_signal=pb.CloseStreamSignal(duration=_duration(event[1] / 1000)))

    async def RetireStream(self, request, context):
        md = _metadata(context)
        stream_id = request.stream_id if request.HasField("stream_id") else ""
        entry = self.state.persistent.get(stream_id)
        if entry is None or entry["table"] != request.table_name:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"no persistent stream {stream_id} for {request.table_name}")
        await self._admit(context, md, request.table_name, label=PROTOCOL_GRPC)
        entry["retired"] = True
        return pb.RetireStreamResponse()

    # ------------------------------------------------------------ Arrow Flight DoPut

    async def do_put(self, request_iterator, context):
        """Arrow Flight DoPut: schema frame, then record batches with offsets in app_metadata."""
        await context.send_initial_metadata(())
        md = _metadata(context)
        try:
            first = await request_iterator.__anext__()
        except StopAsyncIteration:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "DoPut closed before the schema")
        table = md.get("x-databricks-zerobus-table-name", "")
        await self._admit(context, md, table, label=PROTOCOL_ARROW)
        try:
            schema = fl.schema_from_header(first.data_header)
        except fl.FlightError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        snap = self.state.tables.snapshot(table)
        causes = fl.check_arrow_schema(snap, schema)
        if causes:
            failure = _rejection(RecordRejected(causes), prefix="Arrow schema does not fit the table: ")
            await context.abort_with_status(_status_with_info(failure))
        session = _Session(stream_id=self.state.next_stream_id(), table=table, snap=snap, kind="arrow",
                           label=PROTOCOL_ARROW, arrow_header=bytes(first.data_header))
        yield fl.ready_result()
        async for event in self._run(request_iterator, session, context, _extract_flight(self, session)):
            if event[0] == "ack":
                yield fl.ack_result(event[1], event[2])
            else:
                yield fl.ack_result(event[2], event[3], close_ms=int(event[1]))

    # ------------------------------------------------------------ engine

    async def _run(self, request_iterator, session: _Session, context, extract: Callable):
        queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
        reader = asyncio.ensure_future(self._read(request_iterator, queue, extract))
        try:
            async for event in self._write(queue, session):
                yield event
        except StreamFailure as failure:
            await self._abort(context, failure)
        finally:
            reader.cancel()

    async def _read(self, request_iterator, queue: asyncio.Queue, extract: Callable) -> None:
        try:
            async for request in request_iterator:
                sub = extract(request)
                if sub is not None:
                    await queue.put(sub)
        except asyncio.CancelledError:
            raise
        except Exception:  # client went away or sent a malformed stream
            pass
        finally:
            try:
                await queue.put(END)
            except asyncio.CancelledError:  # pragma: no cover
                pass

    async def _write(self, queue: asyncio.Queue, session: _Session):
        """Yields ("ack", upto, records) and ("close", ms, upto, records) events."""
        state = self.state
        closing = False
        while not closing:
            head = await queue.get()
            group = [head]
            while len(group) < GROUP_MAX and not queue.empty():
                group.append(queue.get_nowait())

            pending: List[_Submission] = []
            for item in group:
                if item is END:
                    closing = True
                    break
                if item.error is not None:
                    async for ev in self._flush(session, pending):
                        yield ev
                    pending = []
                    raise item.error

                for firing in state.registry.evaluate("ingest", PROTOCOL_GRPC, session.table, session.stream_id):
                    f = firing.fault
                    if f.type == "schema_change":
                        try:
                            snap = state.apply_schema_change(session.table, f.params["add_columns"])
                            state.log_firing(firing, "schema_change applied",
                                             f"table now schema v{snap.version}; open stream keeps v{session.snap.version}")
                        except Exception as exc:  # noqa: BLE001 - a bad injected schema change must not crash the stream
                            state.log_firing(firing, "schema_change failed", str(exc))
                    elif f.type == "stream_abort":
                        async for ev in self._flush(session, pending):
                            yield ev
                        pending = []
                        if f.params["persist_unacked"]:
                            self._store(session, [item])
                        code = grpc.StatusCode.UNAVAILABLE if f.params["retryable"] else grpc.StatusCode.INVALID_ARGUMENT
                        detail = "retryable" if f.params["retryable"] else "terminal"
                        state.log_firing(firing, code.name,
                                         f"{detail} abort at offset {item.offset}; persist_unacked={f.params['persist_unacked']}")
                        raise StreamFailure(code, f"injected {detail} stream error at offset {item.offset}")
                    elif f.type == "close_stream_signal":
                        async for ev in self._flush(session, pending):
                            yield ev
                        pending = []
                        ms = int(float(f.params["duration_ms"]))
                        state.log_firing(firing, "CloseStreamSignal", f"closes after {ms} ms")
                        yield ("close", ms, session.acked_offset, session.records_stored)
                        await asyncio.sleep(ms / 1000.0)
                        return
                pending.append(item)

            async for ev in self._flush(session, pending):
                yield ev

    def _store(self, session: _Session, subs: List[_Submission]) -> None:
        rows: List[dict] = []
        offsets: List[int] = []
        for sub in subs:
            rows.extend(sub.rows)
            offsets.extend([sub.offset] * len(sub.rows))
        if rows:
            self.state.tables.append(session.table, session.snap, rows, protocol=session.label,
                                     stream_id=session.stream_id, offsets=offsets)
            session.records_stored += len(rows)

    async def _flush(self, session: _Session, subs: List[_Submission]):
        """Store the submissions, then emit at most one cumulative ack (after ack faults)."""
        if not subs:
            return
        self._store(session, subs)
        upto = subs[-1].offset
        delay = 0.0
        dropped = False
        for firing in self.state.registry.evaluate("ack", PROTOCOL_GRPC, session.table, session.stream_id):
            f = firing.fault
            if f.type == "slow_ack":
                delay += float(f.params["delay_ms"]) / 1000.0
                self.state.log_firing(firing, "delayed ack", f"{f.params['delay_ms']} ms up to offset {upto}")
            elif f.type == "drop_acks":
                count = int(f.params["count"])
                if count == -1:
                    dropped = True  # every ack while the fault is active
                else:
                    session.drop_budget += count  # the next `count` acks
                self.state.log_firing(firing, "ack dropped", f"count={count} from offset {upto}")
        if session.drop_budget > 0:
            session.drop_budget -= 1
            dropped = True
        if delay:
            await asyncio.sleep(delay)
        session.acked_offset = max(session.acked_offset, upto)
        if session.persistent_id:
            entry = self.state.persistent.get(session.persistent_id)
            if entry is not None:
                entry["committed"] = session.acked_offset
        if dropped:
            return
        yield ("ack", upto, session.records_stored)

    async def _abort(self, context, failure: StreamFailure) -> None:
        if failure.causes:
            await context.abort_with_status(_status_with_info(failure))
        else:
            await context.abort(failure.code, failure.message)


async def _first(request_iterator, context):
    try:
        return await request_iterator.__anext__()
    except StopAsyncIteration:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "the stream closed before create_stream")


# ---------------------------------------------------------------- extractors

def _submission_error(offset: int, code: grpc.StatusCode, message: str) -> _Submission:
    return _Submission(offset, [], StreamFailure(code, message))


def _check_offset(session: _Session, body, offset_present: bool) -> Optional[_Submission]:
    if not offset_present:
        return _submission_error(-1, grpc.StatusCode.INVALID_ARGUMENT, "offset_id is required")
    offset = body.offset_id
    if offset != session.expected_offset:
        return _submission_error(offset, grpc.StatusCode.INVALID_ARGUMENT,
                                 f"expected offset_id {session.expected_offset}, got {offset}")
    session.expected_offset += 1
    return None


def _records_and_faults(server: ZerobusServicer, session: _Session, offset: int, payloads, batch: bool) -> _Submission:
    """Apply record faults, size limits and validation to one submission's payloads."""
    state = server.state
    for firing in state.registry.evaluate("record", PROTOCOL_GRPC, session.table, session.stream_id):
        if firing.type == "record_too_large":
            state.log_firing(firing, "OUT_OF_RANGE", f"offset {offset}")
            return _submission_error(offset, grpc.StatusCode.OUT_OF_RANGE,
                                     f"offset {offset} exceeds the maximum message size (injected)")
        if firing.type == "schema_mismatch":
            causes = list(firing.params["causes"])
            code = firing.params.get("error_code")
            state.log_firing(firing, "INVALID_ARGUMENT", f"offset {offset}")
            return _Submission(offset, [], _schema_failure(f"offset {offset} does not fit {session.table} (injected)", causes, code))
    limit = state.cfg.limits.max_payload_bytes
    rows: List[dict] = []
    for index, raw in enumerate(payloads):
        size = len(raw) if isinstance(raw, (bytes, bytearray)) else len(raw.encode("utf-8"))
        if size > limit:
            return _submission_error(offset, grpc.StatusCode.OUT_OF_RANGE,
                                     f"offset {offset} record {index} is {size} bytes; the maximum is {limit}")
        try:
            rows.append(_to_row(session, raw))
        except RecordRejected as exc:
            state.tables.record_rejection(session.table, [c.token for c in exc.causes])
            return _Submission(offset, [], _rejection(exc, prefix=f"offset {offset} record {index}: "))
        except StreamFailure as failure:
            return _Submission(offset, [], failure)
    return _Submission(offset, rows)


def _payloads(body, batch: bool):
    if not batch:
        kind = body.WhichOneof("record")
        if kind is None:
            return StreamFailure(grpc.StatusCode.INVALID_ARGUMENT, "ingest message has no record")
        return [getattr(body, kind)]
    kind = body.WhichOneof("batch")
    if kind == "json_batch":
        return list(body.json_batch.records)
    if kind == "proto_encoded_batch":
        return list(body.proto_encoded_batch.records)
    if kind == "avro_batch":
        return list(body.avro_batch.records)
    return StreamFailure(grpc.StatusCode.INVALID_ARGUMENT, "ingest_record_batch has no records")


def _to_row(session: _Session, raw) -> dict:
    if session.kind == "json":
        try:
            obj = json.loads(raw)
        except (TypeError, ValueError):
            raise StreamFailure(grpc.StatusCode.INVALID_ARGUMENT, "record is not valid JSON")
        if not isinstance(obj, dict):
            raise StreamFailure(grpc.StatusCode.INVALID_ARGUMENT, "record must be a JSON object")
        return validate_object(session.snap, obj)
    if session.kind == "proto":
        msg = session.proto.message_class()
        try:
            msg.ParseFromString(bytes(raw))
        except DecodeError:
            raise StreamFailure(grpc.StatusCode.INVALID_ARGUMENT, "record does not decode as the descriptor's message")
        return validate_object(session.snap, message_to_dict(msg), rescue_allowed=False)
    if session.kind == "avro":
        try:
            obj = avro_mod.decode_datum(session.avro, bytes(raw))
        except avro_mod.AvroDecodeError as exc:
            raise StreamFailure(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        return validate_object(session.snap, obj, rescue_allowed=False)
    raise StreamFailure(grpc.StatusCode.UNIMPLEMENTED, "unsupported record type")


def _extract_ephemeral(server: ZerobusServicer, session: _Session):
    def extract(request):
        kind = request.WhichOneof("payload")
        if kind == "ingest_record":
            body, batch = request.ingest_record, False
        elif kind == "ingest_record_batch":
            body, batch = request.ingest_record_batch, True
        else:
            return _submission_error(-1, grpc.StatusCode.INVALID_ARGUMENT, "only ingest messages may follow create_stream")
        err = _check_offset(session, body, body.HasField("offset_id"))
        if err is not None:
            return err
        payloads = _payloads(body, batch)
        if isinstance(payloads, StreamFailure):
            return _Submission(body.offset_id, [], payloads)
        return _records_and_faults(server, session, body.offset_id, payloads, batch)
    return extract


def _extract_persistent(server: ZerobusServicer, session: _Session):
    def extract(request):
        kind = request.WhichOneof("payload")
        if kind == "ingest_record":
            body, batch = request.ingest_record, False
        elif kind == "ingest_record_batch":
            body, batch = request.ingest_record_batch, True
        else:
            return _submission_error(-1, grpc.StatusCode.INVALID_ARGUMENT, "only ingest messages may follow the open")
        err = _check_offset(session, body, body.HasField("offset_id"))
        if err is not None:
            return err
        payloads = _payloads(body, batch)
        if isinstance(payloads, StreamFailure):
            return _Submission(body.offset_id, [], payloads)
        return _records_and_faults(server, session, body.offset_id, payloads, batch)
    return extract


def _extract_flight(server: ZerobusServicer, session: _Session):
    def extract(frame):
        state = server.state
        if not frame.app_metadata:
            # dictionary batch: buffered and sent in front of the next record batch
            session.dict_frames.append(fl.ipc_frame(bytes(frame.data_header), bytes(frame.data_body)))
            return None
        try:
            offset = fl.batch_offset(bytes(frame.app_metadata))
        except fl.FlightError as exc:
            return _submission_error(-1, grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        if offset != session.expected_offset:
            return _submission_error(offset, grpc.StatusCode.INVALID_ARGUMENT,
                                     f"expected offset_id {session.expected_offset}, got {offset}")
        session.expected_offset += 1
        frames = session.dict_frames + [fl.ipc_frame(bytes(frame.data_header), bytes(frame.data_body))]
        session.dict_frames = []
        try:
            batch = fl.batch_from_frames(session.arrow_header, frames)
            rows_native = fl.batch_rows(session.snap, batch)
        except fl.FlightError as exc:
            return _submission_error(offset, grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        for firing in state.registry.evaluate("record", PROTOCOL_GRPC, session.table, session.stream_id):
            if firing.type == "record_too_large":
                state.log_firing(firing, "OUT_OF_RANGE", f"offset {offset}")
                return _submission_error(offset, grpc.StatusCode.OUT_OF_RANGE,
                                         f"offset {offset} exceeds the maximum message size (injected)")
            if firing.type == "schema_mismatch":
                state.log_firing(firing, "INVALID_ARGUMENT", f"offset {offset}")
                return _Submission(offset, [], _schema_failure(
                    f"offset {offset} does not fit {session.table} (injected)",
                    list(firing.params["causes"]), firing.params.get("error_code")))
        limit = state.cfg.limits.max_payload_bytes
        rows: List[dict] = []
        for index, row in enumerate(rows_native):
            size = len(json.dumps(row, default=str).encode("utf-8"))
            if size > limit:
                return _submission_error(offset, grpc.StatusCode.OUT_OF_RANGE,
                                         f"offset {offset} row {index} is {size} bytes; the maximum is {limit}")
            try:
                rows.append(validate_object(session.snap, row, rescue_allowed=False))
            except RecordRejected as exc:
                state.tables.record_rejection(session.table, [c.token for c in exc.causes])
                return _Submission(offset, [], _rejection(exc, prefix=f"offset {offset} row {index}: "))
        return _Submission(offset, rows)
    return extract


def flight_handler(server: ZerobusServicer):
    """A generic gRPC handler that serves Arrow Flight DoPut on the same server."""
    return grpc.method_handlers_generic_handler(
        fl.FLIGHT_SERVICE,
        {
            "DoPut": grpc.stream_stream_rpc_method_handler(
                server.do_put,
                request_deserializer=fl.FlightData.FromString,
                response_serializer=fl.PutResult.SerializeToString,
            )
        },
    )
