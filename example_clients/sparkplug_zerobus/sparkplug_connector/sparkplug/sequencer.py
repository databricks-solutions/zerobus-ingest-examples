# Copyright (c) 2014-2022 Cirrus Link Solutions and others
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0.
#
# SPDX-License-Identifier: EPL-2.0
#
# A Python translation of the decisions in Eclipse Tahu 1.0.21's host library (tahu-host, tag v1.0.21):
# org.eclipse.tahu.host.seq.SequenceReorderManager and SequenceReorderMap, org.eclipse.tahu.host.TahuPayloadHandler,
# org.eclipse.tahu.host.manager.SparkplugEdgeNode and SparkplugDevice, org.eclipse.tahu.host.model.HostApplicationMetricMap.
"""Rebirth requests, ``seq`` reordering and drop-until-rebirth as a Sparkplug host application on Eclipse Tahu.

A port of the decisions of Tahu 1.0.21's host library (the classes above), quirks included (each marked
``Tahu quirk``); the one deliberate difference is marked ``Deviation``:

1. **Reordering**, on arrival, one queue and one expected ``seq`` per edge node (its devices' messages share them):
   NBIRTH sets the expected ``seq`` to 0; NDEATH goes on at once; a message without ``seq`` is dropped; the expected
   ``seq`` goes on (255 -> 0) and the held successors follow; any other ``seq`` is held. Once a second (``tick``),
   an edge node that has held messages and handled none in order for the timeout (default 5 s) loses them, its
   expected ``seq`` goes back to 0 and a rebirth is requested. A passing NBIRTH first drops the held messages older
   than itself.
2. **State checks**, for each message the reordering hands on: births bring an edge node and its devices online,
   deaths take them offline (NDEATH only with the NBIRTH's ``bdSeq``); data from an unknown or offline edge node or
   an unknown device asks for a rebirth and is dropped; each edge node's own ``seq`` check. Only the messages that
   pass get a row.
3. **Rebirth requests**: at most one per edge node per 5 s; at the reorder timeout a known edge node is first set
   offline (so its data is dropped until the NBIRTH). ``block_auto_rebirth`` stops them being sent, not decided.
4. **The births' metric types** (``HostApplicationMetricMap``): a metric sent without a datatype decodes with the type
   the births gave it (``tahu_json.MetricTypes``); a birth repeating an alias asks for a rebirth.
5. **The edge nodes' and devices' state** (``edge_nodes``): online or offline since when, with Sparkplug's times: a
   birth's own timestamp, an NDEATH's receipt (the host's UTC time: the MQTT server may send it as the will), a
   DDEATH's timestamp, and the host's time when the reorder timeout sets an edge node offline.

``strict``: the rows are those Tahu hands on, in its order (a held row's ``inserted_at`` is when it is released).
``keep_all``: every message gets its row at once; the same decisions run alongside, for the rebirth requests and
counted drops. Strict needs rebirth requests (only a rebirth ends an edge node's drops), so a source that can't send
them runs ``keep_all`` (``keep_all_from_now``). No I/O: ``now`` (monotonic seconds) is passed in, and the rebirth
requests go to a callback.
"""
from __future__ import annotations

import dataclasses
import logging
import time
from collections import Counter
from typing import Callable

from sparkplug_connector.sparkplug import sparkplug_b_pb2 as spb
from sparkplug_connector.sparkplug import tahu_json
from sparkplug_connector.sparkplug.raw_row import Outcome, RawRow
from sparkplug_connector.sparkplug.tahu_json import METRIC_TYPE_CODES, MetricTypes

logger = logging.getLogger(__name__)

STRICT, KEEP_ALL = "strict", "keep_all"
MODES = (STRICT, KEEP_ALL)
DEFAULT_REORDER_TIMEOUT_MS = 5000      # sequence_reordering_timeout
TICK_S = 1.0                           # SequenceReorderManager's timer
REBIRTH_LIMIT_S = 5.0                  # TahuPayloadHandler: one rebirth request per edge node per 5 s
_TEMPLATE = METRIC_TYPE_CODES["Template"]
# bdSeq's types Tahu reads as a long (SparkplugUtil.convertSeqNumber); any other fails the message
_BDSEQ_TYPES = frozenset({"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"})
# why a message gets no row (dropped_<reason>)
REASONS = ("gap_timeout", "pruned_at_nbirth", "replaced_seq", "no_seq", "unknown_node", "node_offline",
           "unknown_device", "device_offline", "no_bdseq", "bdseq_mismatch", "seq_check", "invalid_birth",
           "undecodable", "shutdown")


def rebirth_payload(timestamp_ms: int) -> bytes:
    """The NCMD: payload timestamp, no ``seq``, ``Node Control/Rebirth`` = true (Boolean, same timestamp)."""
    p = spb.Payload(timestamp=timestamp_ms)
    m = p.metrics.add()
    m.name, m.timestamp, m.datatype, m.is_null, m.boolean_value = "Node Control/Rebirth", timestamp_ms, 11, False, True
    return p.SerializeToString()


def rebirth_topic(group_id: str, edge_node_id: str) -> str:
    return f"spBv1.0/{group_id}/NCMD/{edge_node_id}"


class _Queue:
    """SequenceReorderMap: the edge node's expected ``seq`` and the messages held for a missing one."""
    __slots__ = ("expected", "last_update", "held")

    def __init__(self, now: float):
        self.expected = 0
        self.last_update = now
        self.held: dict[int, Outcome] = {}


class _Node:
    """SparkplugEdgeNode, with its devices' online flags (SparkplugDevice); since when and what changed them."""
    __slots__ = ("online", "birth_bdseq", "last_seq", "devices", "since", "changed_by", "device_since")

    def __init__(self) -> None:
        self.online = False
        self.birth_bdseq: int | None = None
        self.last_seq: int | None = None
        self.devices: dict[str, bool] = {}
        self.since: int | None = None                           # epoch ms
        self.changed_by: str | None = None
        self.device_since: dict[str, tuple[int, str]] = {}      # device: (epoch ms, what changed it)

    def mark(self, online: bool, ms: int, by: str) -> None:
        self.online, self.since, self.changed_by = online, ms, by

    def mark_device(self, device: str, online: bool, ms: int, by: str) -> None:
        self.devices[device] = online
        self.device_since[device] = (ms, by)


class _MetricMap:
    """One edge node's or device's MetricMap in HostApplicationMetricMap: its births' aliases and types."""
    __slots__ = ("alias_to_name", "types")

    def __init__(self) -> None:
        self.alias_to_name: dict[int, str] = {}
        self.types = MetricTypes()


class Sequencer:
    """One Sparkplug source's Tahu host decisions (see the module docstring)."""

    def __init__(self, sequence_handling: str = STRICT, reorder_timeout_ms: int = DEFAULT_REORDER_TIMEOUT_MS,
                 block_auto_rebirth: bool = False, request_rebirth: Callable[[str, str], None] | None = None,
                 name: str = "sparkplug", clock: Callable[[], float] = time.time):
        if sequence_handling not in MODES:
            raise ValueError(f"sequence_handling must be {' or '.join(MODES)}, not {sequence_handling!r}")
        if reorder_timeout_ms <= 0:
            raise ValueError(f"sequence_reordering_timeout must be a positive number of milliseconds, not {reorder_timeout_ms!r}")
        self.mode = sequence_handling
        self.timeout_s = reorder_timeout_ms / 1000.0
        self.block_auto_rebirth = block_auto_rebirth
        self._request = request_rebirth
        self._name = name
        self._clock = clock                             # the host's UTC time (seconds), for the edge node state
        self._queues: dict[tuple[str, str], _Queue] = {}
        self._nodes: dict[tuple[str, str], _Node] = {}
        self._maps: dict[tuple[str, str], dict[str | None, _MetricMap]] = {}
        self._last_request: dict[tuple[str, str], float] = {}
        self.counters: Counter[str] = Counter()

    # ------------------------------------------------------------------ input

    def on_message(self, outcome: Outcome, now: float) -> list[RawRow]:
        """The rows to write now for a message that has a row (``raw_row.convert``)."""
        if self.mode == KEEP_ALL:
            typed = self._typed(outcome)            # with the births' types as they are now
            self._reorder(outcome, now)
            return [outcome.row if typed is None else typed[2]]
        return self._reorder(outcome, now)

    def tick(self, now: float) -> None:
        """Once a second: an edge node whose held messages have waited the timeout loses them and asks for a rebirth."""
        for key, q in self._queues.items():
            if q.held and now - q.last_update > self.timeout_s:
                logger.info("[%s] %s/%s: no seq %d for %.0f ms: %d held messages dropped, rebirth requested",
                            self._name, *key, q.expected, self.timeout_s * 1000, len(q.held))
                for held in q.held.values():
                    self._drop(held, "gap_timeout")
                q.held.clear()
                q.expected = 0
                q.last_update = now
                self._request_rebirth(key, now, self._nodes.get(key))

    def close(self) -> None:
        """The source stops: what is held is dropped (a Tahu host application loses its queue the same way)."""
        for q in self._queues.values():
            for held in q.held.values():
                self._drop(held, "shutdown")
            q.held.clear()

    def stats(self) -> dict:
        """Counters; in ``keep_all`` the drops are what strict would drop (every message is written)."""
        c = self.counters
        return {
            "sequence_handling": self.mode,
            "messages_held": sum(len(q.held) for q in self._queues.values()),
            "messages_reordered": c["messages_reordered"],
            "messages_dropped": c["messages_dropped"],
            **{f"dropped_{r}": c[f"dropped_{r}"] for r in REASONS},
            "rebirth_requests": c["rebirth_requests"],
            "rebirth_requests_rate_limited": c["rebirth_requests_rate_limited"],
            "rebirth_requests_blocked": c["rebirth_requests_blocked"],
            "duplicate_aliases": c["duplicate_aliases"],
        }

    def edge_nodes(self) -> list[dict]:
        """Each edge node the decisions know, and its devices: online or offline, since when (epoch ms) and what
        changed it (``NBIRTH``, ``NDEATH``, ``DBIRTH``, ``DDEATH``, ``reorder timeout``)."""
        out = []
        for (group_id, edge_node_id), n in sorted(self._nodes.items()):
            devices = [{"device_id": d, "online": online, "since": n.device_since.get(d, (None, None))[0],
                        "changed_by": n.device_since.get(d, (None, None))[1]} for d, online in sorted(n.devices.items())]
            out.append({"group_id": group_id, "edge_node_id": edge_node_id, "online": n.online, "since": n.since,
                        "changed_by": n.changed_by, "bdseq": n.birth_bdseq, "devices": devices})
        return out

    def rebirth_requested(self, group_id: str, edge_node_id: str, now: float) -> None:
        """A rebirth request sent outside these decisions (an operator's): it counts for the 5 s limit, so an
        automatic one doesn't follow right behind it."""
        self._last_request[(group_id, edge_node_id)] = now

    def keep_all_from_now(self) -> list[RawRow]:
        """Switch to ``keep_all`` (rebirths can't be requested any more): the held messages get their rows now, in
        their edge nodes' ``seq`` order, and stay held for the decisions."""
        if self.mode == KEEP_ALL:
            return []
        self.mode = KEEP_ALL
        rows = []
        for q in self._queues.values():
            for seq in sorted(q.held, key=lambda s: (s - q.expected) % 256):
                typed = self._typed(q.held[seq])
                rows.append(q.held[seq].row if typed is None else typed[2])
        return rows

    # ------------------------------------------------------------------ 1. reordering (SequenceReorderManager)

    def _reorder(self, o: Outcome, now: float) -> list[RawRow]:
        t = o.topic
        key = (t.group_id, t.edge_node_id)
        q = self._queues.get(key)
        if q is None:
            q = self._queues[key] = _Queue(now)
        if t.message_type == "NBIRTH":
            q.expected = 0
        elif t.message_type == "NDEATH":
            return self._handle(o, now)
        seq = o.full.get("seq")
        if seq is None:
            return self._drop(o, "no_seq")
        if seq != q.expected:
            replaced = q.held.get(seq)
            if replaced is not None:
                self._drop(replaced, "replaced_seq")
            q.held[seq] = o
            return []
        q.expected = _next(q.expected)
        q.last_update = now
        if t.message_type == "NBIRTH":
            self._prune(q, o.full.get("timestamp"))
        rows = self._handle(o, now)
        nxt = _next(seq)
        while q.held:
            held = q.held.pop(nxt, None)
            if held is None:
                break
            q.expected = _next(q.expected)
            q.last_update = now
            self.counters["messages_reordered"] += 1
            rows += self._handle(held, now, inserted_at=o.row.inserted_at)
            nxt = _next(nxt)
        return rows

    def _prune(self, q: _Queue, nbirth_timestamp: int | None) -> None:
        """SequenceReorderMap.prune: the held messages older than the NBIRTH are dropped."""
        if nbirth_timestamp is None:
            logger.error("[%s] an NBIRTH without a timestamp: held messages not pruned", self._name)
            return
        for seq, held in list(q.held.items()):
            ts = held.full.get("timestamp")
            if ts is not None and ts < nbirth_timestamp:
                del q.held[seq]
                self._drop(held, "pruned_at_nbirth")

    # ------------------------------------------------------------------ 2. state checks (TahuPayloadHandler)

    def _handle(self, o: Outcome, now: float, inserted_at: int | None = None) -> list[RawRow]:
        typed = self._typed(o)
        if typed is None:
            return self._drop(o, "undecodable")
        _, decoded, row = typed                     # Deviation: the checks read the payload decompressed (below)
        if inserted_at is not None:
            row = dataclasses.replace(row, inserted_at=inserted_at)
        t = o.topic
        key = (t.group_id, t.edge_node_id)
        node = self._nodes.get(key)
        kind, device = t.message_type, t.device_id
        if kind == "NBIRTH":
            if node is None:
                node = self._nodes[key] = _Node()
            self._maps.pop(key, None)               # the edge node's and its devices' births' types
            try:
                bdseq = _bdseq(decoded)
            except ValueError:
                return self._drop(o, "invalid_birth")
            if decoded.get("timestamp") is None or bdseq is None:
                return self._drop(o, "invalid_birth")
            node.birth_bdseq, node.last_seq = bdseq, decoded.get("seq")
            node.mark(True, decoded["timestamp"], "NBIRTH")
            self._births(key, None, decoded["metrics"], now)
            return [row]
        if kind == "DBIRTH":
            if node is None:
                return self._drop(o, "unknown_node")
            birth_ms = _timestamp(decoded, row.inserted_at)
            if device not in node.devices:          # Tahu quirk: a new device starts online, even if the seq check fails
                node.mark_device(device, True, birth_ms, "DBIRTH")
            if not _seq_check(node, decoded.get("seq")):
                return self._drop(o, "seq_check")
            node.mark_device(device, True, birth_ms, "DBIRTH")
            self._births(key, device, decoded["metrics"], now)
            return [row]
        if kind == "NDATA":
            if node is None or not node.online:
                self._request_rebirth(key, now)
                return self._drop(o, "unknown_node" if node is None else "node_offline")
            return [row] if _seq_check(node, decoded.get("seq")) else self._drop(o, "seq_check")
        if kind == "DDATA":
            if node is None or device not in node.devices or not node.online:
                self._request_rebirth(key, now)
                return self._drop(o, "unknown_node" if node is None else "unknown_device" if device not in node.devices
                                  else "node_offline")
            return [row] if _seq_check(node, decoded.get("seq")) else self._drop(o, "seq_check")
        if kind == "NDEATH":
            try:
                bdseq = _bdseq(decoded)
            except ValueError:
                return self._drop(o, "no_bdseq")
            if node is None or bdseq is None:
                return self._drop(o, "unknown_node" if node is None else "no_bdseq")
            if not node.online:
                return self._drop(o, "node_offline")
            if bdseq != node.birth_bdseq:
                return self._drop(o, "bdseq_mismatch")
            node.mark(False, row.inserted_at, "NDEATH")     # received now: the host's time, not the payload's
            for d, online in node.devices.items():
                if online:                                  # one a DDEATH took offline keeps its time
                    node.mark_device(d, False, row.inserted_at, "NDEATH")
            return [row]
        # DDEATH
        online = None if node is None else node.devices.get(device)
        if online is None or not node.online or not online:
            return self._drop(o, "unknown_node" if node is None else "unknown_device" if online is None
                              else "node_offline" if not node.online else "device_offline")
        if not _seq_check(node, decoded.get("seq")):
            return self._drop(o, "seq_check")
        node.mark_device(device, False, _timestamp(decoded, row.inserted_at), "DDEATH")
        return [row]

    def _typed(self, o: Outcome) -> tuple[dict, dict, RawRow] | None:
        """The message decoded with the births' types of its edge node or device (None: undecodable then): the
        payload as received, decompressed, and its row. Deviation: Tahu's checks read the payload as received; a
        compressed one carries only ``seq`` outside (Tahu's own compression), so a compressed birth would never pass
        and its edge node would be asked for a rebirth every 5 s: the checks and the reordering read it decompressed."""
        t = o.topic
        m = self._maps.get((t.group_id, t.edge_node_id), {}).get(t.device_id)
        if m is None or m.types.empty or not (tahu_json.untyped(o.decoded) or tahu_json.untyped(o.full)):
            return o.decoded, o.full, o.row
        try:
            decoded = tahu_json.decode(o.payload, m.types)
            full = tahu_json.decompress(decoded, m.types)
            return decoded, full, dataclasses.replace(o.row, msg=tahu_json.dumps(full))
        except Exception as e:  # noqa: BLE001 - as any undecodable payload
            logger.warning("[%s] %s undecodable with its births' types: %s", self._name, o.row.msg_topic, e)
            return None

    def _births(self, key: tuple[str, str], device: str | None, metrics: list, now: float) -> None:
        """The birth's metrics into the births' types; a repeated alias asks for a rebirth. Tahu quirk: the first
        metric that can't be added (null, no name, no type) ends the loop (an exception there)."""
        maps = self._maps.setdefault(key, {})
        for m in metrics:
            if m is None:
                return
            alias = m.get("alias")
            if alias is not None:
                existing = maps.get(device)
                if existing is not None and alias in existing.alias_to_name:
                    logger.warning("[%s] %s/%s: a birth repeats alias %d: rebirth requested", self._name, *key, alias)
                    self.counters["duplicate_aliases"] += 1
                    self._request_rebirth(key, now)
                    return
            if not _add_metric(maps, device, m.get("name"), m):
                return

    # ------------------------------------------------------------------ 3. rebirth requests

    def _request_rebirth(self, key: tuple[str, str], now: float, node: _Node | None = None) -> None:
        last = self._last_request.get(key)
        if last is not None and now - last < REBIRTH_LIMIT_S:
            self.counters["rebirth_requests_rate_limited"] += 1
            return
        self._last_request[key] = now
        if node is not None and node.online:        # its data is dropped until the NBIRTH (offline: keeps its time)
            node.mark(False, int(self._clock() * 1000), "reorder timeout")
        self.counters["rebirth_requests"] += 1
        if self.block_auto_rebirth:
            self.counters["rebirth_requests_blocked"] += 1
            return
        if self._request is not None:
            self._request(*key)

    def _drop(self, o: Outcome, reason: str) -> list[RawRow]:
        self.counters["messages_dropped"] += 1
        self.counters[f"dropped_{reason}"] += 1
        logger.debug("[%s] %s: no row (%s)", self._name, o.row.msg_topic, reason)
        return []


def _next(seq: int) -> int:
    return 0 if seq + 1 == 256 else seq + 1


def _timestamp(decoded: dict, default: int) -> int:
    """The payload's timestamp (epoch ms), or ``default`` (the receipt) without one."""
    ts = decoded.get("timestamp")
    return default if ts is None else ts


def _seq_check(node: _Node, seq: int | None) -> bool:
    """SparkplugEdgeNode.handleSeq. Tahu quirk: it counts before comparing, so after a message the checks before it
    dropped, every later one fails until the next NBIRTH."""
    if node.last_seq is None:
        return False
    node.last_seq = 0 if node.last_seq + 1 == 256 else node.last_seq + 1
    return node.last_seq == seq


def _bdseq(decoded: dict) -> int | None:
    """SparkplugUtil.getBdSequenceNumber: the first ``bdSeq`` metric's value (None: no such metric); ValueError
    where Tahu throws (a null metric before it, a null or non-integer value)."""
    for m in decoded["metrics"]:
        if m is None:
            raise ValueError("a null metric")
        if m.get("name") == "bdSeq":
            value = m.get("value")
            if m.get("dataType") not in _BDSEQ_TYPES or not isinstance(value, int):
                raise ValueError(f"bdSeq is {m.get('dataType')} {value!r}")
            return value - (1 << 64) if value >= 1 << 63 else value     # a UInt64 as Java's long
    return None


def _add_metric(maps: dict[str | None, _MetricMap], device: str | None, name: str | None, m: dict) -> bool:
    """HostApplicationMetricMap.addMetric (template members as ``<name>/<member>``); False where Tahu throws."""
    target = maps.get(device)
    if target is None:
        target = maps[device] = _MetricMap()
    code = METRIC_TYPE_CODES.get(m.get("dataType"))
    alias = m.get("alias")
    if alias is not None:
        if name is None:
            return False
        target.alias_to_name[alias] = name
        if code is None:
            return False
        target.types.aliases[alias] = code
    if name is None or code is None:
        return False
    target.types.names[name] = code
    value = m.get("value")
    if code == _TEMPLATE and isinstance(value, dict):
        for member in value["metrics"]:
            if member is None or not _add_metric(maps, device, f"{name}/{member.get('name', 'null')}", member):
                return False
    return True
