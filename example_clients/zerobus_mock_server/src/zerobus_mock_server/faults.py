"""Fault injection: the catalogue, triggers, scope and the event log.

A fault has a type (what goes wrong), a scope (protocol, table, stream), a
trigger (when it fires) and params (how it fails). Each request or stream step
asks the registry which faults fire at its *point*:

    token   POST /oidc/v1/token
    create  gRPC create-stream
    insert  REST insert request
    record  one record (REST) or one ingest message (gRPC), before validation
    ingest  one accepted ingest message (gRPC), before it is written
    ack     one acknowledgement about to be sent (gRPC)

Triggers: ``always``; ``once``; ``every_nth`` (``n``); ``probability``
(``probability``, seeded by the config); ``after`` (``after`` matching events
have passed, then fires up to ``times`` times, unlimited if omitted).

Every injected fault is written to the event log (in memory and, when a path
is configured, as JSON lines).
"""

from __future__ import annotations

import itertools
import json
import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

PROTOCOLS = ("rest", "grpc")
GRPC_CODES = {
    "UNAVAILABLE",
    "RESOURCE_EXHAUSTED",
    "INTERNAL",
    "ABORTED",
    "DEADLINE_EXCEEDED",
    "UNKNOWN",
}
HTTP_STATUSES = (400, 401, 403, 404, 409, 415, 429, 500, 502, 503, 504)


@dataclass(frozen=True)
class FaultSpec:
    protocols: Tuple[str, ...]
    points: Tuple[str, ...]
    defaults: Dict[str, Any]
    description: str


FAULT_TYPES: Dict[str, FaultSpec] = {
    "token_invalid_client": FaultSpec(
        ("rest",), ("token",), {},
        "Token endpoint answers 401 invalid_client (client authentication fails).",
    ),
    "token_invalid_authorization_details": FaultSpec(
        ("rest",), ("token",), {},
        "Token endpoint answers 401 invalid_authorization_details.",
    ),
    "http_error": FaultSpec(
        ("rest",), ("token", "insert"), {"status": 503, "message": None, "retry_after_seconds": None},
        "Answers with the given HTTP status (429, 500, 503 by default 503) before any work.",
    ),
    "slow_response": FaultSpec(
        ("rest",), ("insert",), {"delay_ms": 1000},
        "Delays the REST response. A very large delay behaves like no response.",
    ),
    "unauthorized": FaultSpec(
        ("rest", "grpc"), ("insert", "create"), {},
        "Rejects the call as unauthenticated (401 / UNAUTHENTICATED).",
    ),
    "permission_denied": FaultSpec(
        ("rest", "grpc"), ("insert", "create"), {},
        "Rejects the call as not permitted (403 / PERMISSION_DENIED).",
    ),
    "table_not_found": FaultSpec(
        ("rest", "grpc"), ("insert", "create"), {},
        "Rejects the call as a missing table (404 / NOT_FOUND).",
    ),
    "schema_mismatch": FaultSpec(
        ("rest", "grpc"), ("record",), {"causes": ["FIELD_NOT_IN_TABLE"], "error_code": "4044"},
        "Rejects a record as not fitting the table, with the given causes.",
    ),
    "record_too_large": FaultSpec(
        ("rest", "grpc"), ("record",), {},
        "Rejects a record as larger than the maximum message size (OUT_OF_RANGE).",
    ),
    "grpc_status": FaultSpec(
        ("grpc",), ("create",), {"code": "UNAVAILABLE", "message": "injected stream-creation failure"},
        "Fails stream creation with the given gRPC status (UNAVAILABLE by default).",
    ),
    "stream_abort": FaultSpec(
        ("grpc",), ("ingest",), {"retryable": True, "persist_unacked": True},
        "Ends the stream mid-flight. retryable=true gives UNAVAILABLE (the SDK reconnects "
        "and resends unacknowledged records); false gives INVALID_ARGUMENT (terminal). "
        "persist_unacked=true writes the triggering batch before the abort, without an ack.",
    ),
    "close_stream_signal": FaultSpec(
        ("grpc",), ("ingest",), {"duration_ms": 1000},
        "Sends CloseStreamSignal, then ends the stream OK after duration_ms.",
    ),
    "slow_ack": FaultSpec(
        ("grpc",), ("ack",), {"delay_ms": 1000},
        "Delays one acknowledgement by delay_ms.",
    ),
    "drop_acks": FaultSpec(
        ("grpc",), ("ack",), {"count": -1},
        "Drops acknowledgements: count of them, or -1 for every ack until the stream closes. "
        "Records are still written.",
    ),
    "schema_change": FaultSpec(
        ("rest", "grpc"), ("insert", "ingest"), {"add_columns": []},
        "Adds columns to the table now. Open gRPC streams keep their schema; new streams and "
        "REST requests see the new one.",
    ),
}

TRIGGER_MODES = ("always", "once", "every_nth", "probability", "after")


@dataclass
class Fault:
    id: str
    type: str
    protocol: str  # "any" | "rest" | "grpc"
    table: str  # "*" or a table name
    stream: str  # "*" or a stream id (gRPC only)
    trigger: Dict[str, Any]
    params: Dict[str, Any]
    description: str
    matched: int = 0
    fired: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "protocol": self.protocol,
            "table": self.table,
            "stream": self.stream,
            "trigger": dict(self.trigger),
            "params": dict(self.params),
            "matched": self.matched,
            "fired": self.fired,
            "description": self.description,
        }


@dataclass(frozen=True)
class Firing:
    fault: Fault
    point: str
    protocol: str
    table: str
    stream: Optional[str]

    @property
    def type(self) -> str:
        return self.fault.type

    @property
    def params(self) -> Dict[str, Any]:
        return self.fault.params


@dataclass
class Event:
    seq: int
    time: str
    fault_id: str
    fault_type: str
    point: str
    protocol: str
    table: str
    stream: Optional[str]
    action: str
    detail: str

    def as_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


class FaultError(ValueError):
    """A fault spec is not valid."""


def _check_params(ftype: str, params: Dict[str, Any]) -> Dict[str, Any]:
    spec = FAULT_TYPES[ftype]
    unknown = set(params) - set(spec.defaults)
    if unknown:
        raise FaultError(f"{ftype} has no param(s) {sorted(unknown)}; allowed: {sorted(spec.defaults)}")
    merged = dict(spec.defaults)
    merged.update(params)
    if ftype == "http_error":
        if merged["status"] not in HTTP_STATUSES:
            raise FaultError(f"http_error status must be one of {HTTP_STATUSES}")
    if ftype == "grpc_status" and str(merged["code"]).upper() not in GRPC_CODES:
        raise FaultError(f"grpc_status code must be one of {sorted(GRPC_CODES)}")
    if ftype in ("slow_response", "slow_ack", "close_stream_signal"):
        key = "duration_ms" if ftype == "close_stream_signal" else "delay_ms"
        if not isinstance(merged[key], (int, float)) or merged[key] < 0:
            raise FaultError(f"{key} must be a non-negative number")
    if ftype == "drop_acks" and (not isinstance(merged["count"], int) or merged["count"] < -1):
        raise FaultError("drop_acks count must be an integer >= -1 (-1 = until close)")
    if ftype == "schema_mismatch":
        if not isinstance(merged["causes"], list) or not merged["causes"]:
            raise FaultError("schema_mismatch causes must be a non-empty list")
    if ftype == "schema_change" and not isinstance(merged["add_columns"], list):
        raise FaultError("schema_change add_columns must be a list")
    if ftype == "stream_abort":
        merged["retryable"] = bool(merged["retryable"])
        merged["persist_unacked"] = bool(merged["persist_unacked"])
    return merged


def _check_trigger(trigger: Dict[str, Any]) -> Dict[str, Any]:
    mode = trigger.get("mode", "always")
    if mode not in TRIGGER_MODES:
        raise FaultError(f"trigger mode must be one of {TRIGGER_MODES}")
    out: Dict[str, Any] = {"mode": mode}
    if mode == "every_nth":
        n = trigger.get("n")
        if not isinstance(n, int) or n < 1:
            raise FaultError("every_nth needs n >= 1")
        out["n"] = n
    elif mode == "probability":
        p = trigger.get("probability")
        if not isinstance(p, (int, float)) or not 0 <= p <= 1:
            raise FaultError("probability needs a value between 0 and 1")
        out["probability"] = float(p)
    elif mode == "after":
        after = trigger.get("after")
        if not isinstance(after, int) or after < 0:
            raise FaultError("after needs an integer >= 0")
        out["after"] = after
        times = trigger.get("times")
        if times is not None and (not isinstance(times, int) or times < 1):
            raise FaultError("times must be an integer >= 1 when given")
        out["times"] = times
    elif mode == "once":
        out = {"mode": "after", "after": 0, "times": 1}
    return out


class FaultRegistry:
    def __init__(self, seed: int = 0, event_log_path: Optional[Path] = None, max_events: int = 10000):
        self._faults: Dict[str, Fault] = {}
        self._ids = itertools.count(1)
        self._rng = random.Random(seed)
        self._lock = threading.Lock()
        self._events: Deque[Event] = deque(maxlen=max_events)
        self._seq = itertools.count(1)
        self._log_path = event_log_path
        if event_log_path:
            event_log_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------ management

    def add(self, spec: Dict[str, Any]) -> Fault:
        if not isinstance(spec, dict):
            raise FaultError("a fault must be an object")
        ftype = spec.get("type")
        if ftype not in FAULT_TYPES:
            raise FaultError(f"unknown fault type {ftype!r}; known: {sorted(FAULT_TYPES)}")
        fspec = FAULT_TYPES[ftype]
        protocol = spec.get("protocol", "any")
        if protocol not in ("any", *PROTOCOLS):
            raise FaultError("protocol must be any, rest or grpc")
        if protocol != "any" and protocol not in fspec.protocols:
            raise FaultError(f"{ftype} applies to {', '.join(fspec.protocols)} only")
        table = spec.get("table", "*") or "*"
        stream = spec.get("stream", "*") or "*"
        if stream != "*" and "grpc" not in fspec.protocols:
            raise FaultError(f"{ftype} has no stream scope")
        trigger = _check_trigger(spec.get("trigger") or {})
        params = _check_params(ftype, spec.get("params") or {})
        with self._lock:
            fid = spec.get("id") or f"f-{next(self._ids)}"
            if fid in self._faults:
                raise FaultError(f"fault id {fid!r} already exists")
            fault = Fault(
                id=str(fid),
                type=ftype,
                protocol=protocol,
                table=table,
                stream=stream,
                trigger=trigger,
                params=params,
                description=fspec.description,
            )
            self._faults[fault.id] = fault
            return fault

    def remove(self, fault_id: str) -> bool:
        with self._lock:
            return self._faults.pop(fault_id, None) is not None

    def clear(self) -> None:
        with self._lock:
            self._faults.clear()

    def list(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [f.as_dict() for f in self._faults.values()]

    def get(self, fault_id: str) -> Optional[Fault]:
        with self._lock:
            return self._faults.get(fault_id)

    # ------------------------------------------------------------ evaluation

    def evaluate(
        self,
        point: str,
        protocol: str,
        table: str,
        stream: Optional[str] = None,
    ) -> List[Firing]:
        firings: List[Firing] = []
        with self._lock:
            for fault in self._faults.values():
                spec = FAULT_TYPES[fault.type]
                if point not in spec.points or protocol not in spec.protocols:
                    continue
                if fault.protocol not in ("any", protocol):
                    continue
                if fault.table not in ("*", table):
                    continue
                if fault.stream != "*" and fault.stream != stream:
                    continue
                fault.matched += 1
                if self._decide(fault):
                    fault.fired += 1
                    firings.append(Firing(fault, point, protocol, table, stream))
        return firings

    def _decide(self, fault: Fault) -> bool:
        t = fault.trigger
        mode = t["mode"]
        if mode == "always":
            return True
        if mode == "after":
            if fault.matched <= t["after"]:
                return False
            return t["times"] is None or fault.fired < t["times"]
        if mode == "every_nth":
            return fault.matched % t["n"] == 0
        if mode == "probability":
            return self._rng.random() < t["probability"]
        return False  # pragma: no cover

    # ------------------------------------------------------------ event log

    def log(self, firing: Firing, action: str, detail: str = "") -> Event:
        event = Event(
            seq=next(self._seq),
            time=time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z",
            fault_id=firing.fault.id,
            fault_type=firing.fault.type,
            point=firing.point,
            protocol=firing.protocol,
            table=firing.table,
            stream=firing.stream,
            action=action,
            detail=detail,
        )
        with self._lock:
            self._events.append(event)
        if self._log_path is not None:
            with self._log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event.as_dict()) + "\n")
        return event

    def events(self, since: int = 0, fault_id: Optional[str] = None, table: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            items = list(self._events)
        out = []
        for e in items:
            if e.seq <= since:
                continue
            if fault_id and e.fault_id != fault_id:
                continue
            if table and e.table != table:
                continue
            out.append(e.as_dict())
        return out

    def clear_events(self) -> None:
        with self._lock:
            self._events.clear()
