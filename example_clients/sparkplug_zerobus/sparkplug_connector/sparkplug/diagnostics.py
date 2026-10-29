"""The connector's own diagnostics as Sparkplug rows (``diagnostics.enabled``, off by default).

Three edge nodes of their own in the group ``diagnostics.group_id`` (default ``BRIDGE``), each with one device named
after its model, whose rows the connector writes straight into the raw table (no MQTT), like any edge node's:

- ``Bridge Info`` / ``<bridge_model>`` (default ``BRIDGE``), instance ``Core/<bridge_model>``: ``<bridge_model> Version``,
  ``PrimaryHostId``, ``ConnectedEdgeNodes``, ``DisconnectedEdgeNodes``, ``MQTTMessageCount``, ``ModelCount``,
  ``InstanceCount``
- ``Edge Node Info`` / ``EdgeNode``, an instance ``Core/EdgeNode-<edge node id>`` per edge node seen: ``GroupId``,
  ``EdgeNodeId``, ``Connected``, ``ConnectDateTime``, ``DisconnectDateTime``, ``BirthCount``, ``DeathCount``,
  ``MQTTMessageCount``
- ``MQTT Client Info`` / ``MQTTClient``, an instance ``Core/MQTTClient`` per Sparkplug source: ``ClientID``,
  ``Connected``, ``ConnectDateTime``, ``DisconnectDateTime``, ``ServerUrl``, ``ServerName``

Templates version ``v2.0.0``. At start each edge node gets an NBIRTH (``bdSeq`` and its template definitions) and a
DBIRTH (its instances), the edge nodes known then a DDATA; afterwards, every 10 s, a DDATA per edge node if anything
changed. ``seq`` and ``bdSeq`` count 0 to 255; no NDEATH. The JSON is the shape a Java host builds these payloads in
(Eclipse Tahu's builders, a default Jackson mapper), its quirks included: every metric stamped with the time it was
built, the dates as Java's ``Date.toString()`` (``Tue Sep 29 11:48:45 EDT 2026``, the process's time zone; epoch 0
when unknown, ``"null"`` for an MQTT client never connected), ``Connected`` a String in the EdgeNode definition and a
Boolean in its instances, and the Edge Node Info DBIRTH carrying an instance for itself (all zero).

The counters come from the rows the Sparkplug sources hand on (``on_row``: with ``sequence_handling: strict``, the
messages a Tahu host application's checks pass) and from their connections (``on_client``).
"""
from __future__ import annotations

import datetime
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from sparkplug_connector.sparkplug.raw_row import NAMESPACE, RawRow
from sparkplug_connector.sparkplug.tahu_json import dumps

TEMPLATE_VERSION = "v2.0.0"
REFRESH_S = 10.0
EDGE_NODE_MODEL, MQTT_CLIENT_MODEL = "EdgeNode", "MQTTClient"
DEFAULTS = {"group_id": "BRIDGE", "bridge_model": "BRIDGE", "bridge_info_edge_node_id": "Bridge Info",
            "edge_node_info_edge_node_id": "Edge Node Info", "mqtt_client_info_edge_node_id": "MQTT Client Info"}
_DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


_LAST_DATE_MS = 253402300800000                # 10000-01-01T00:00:00Z: the first ms a Python date can't hold


def java_date_string(ms: int, tz: datetime.tzinfo | None = None) -> str:
    """``new java.util.Date(ms).toString()``: ``EEE MMM dd HH:mm:ss zzz yyyy`` in English, in ``tz`` (default: the
    process's local time zone, as a JVM's default)."""
    utc = datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc)
    t = utc.astimezone(tz) if tz is not None else utc.astimezone()
    return (f"{_DAYS[t.weekday()]} {_MONTHS[t.month - 1]} {t.day:02d} {t.hour:02d}:{t.minute:02d}:{t.second:02d} "
            f"{t.tzname()} {t.year}")


@dataclass
class _EdgeNode:
    online: bool = False
    connect_ms: int = 0
    disconnect_ms: int = 0
    births: int = 0
    deaths: int = 0
    messages: int = 0


@dataclass
class _Client:
    client_id: str
    server_url: str
    connected: bool = False
    connect_ms: int = 0
    disconnect_ms: int = 0


@dataclass
class Tracker:
    """The counters behind the diagnostics (one per connector: every Sparkplug source feeds it)."""
    edge_nodes: dict[tuple[str, str], _EdgeNode] = field(default_factory=dict)
    clients: dict[str, _Client] = field(default_factory=dict)          # by source name
    mqtt_message_count: int = 0
    models: set[str] = field(default_factory=set)
    instances: dict[tuple, set[str]] = field(default_factory=dict)
    update_required: bool = True

    def on_row(self, row: RawRow) -> None:
        """A row a Sparkplug source handed on."""
        key = (row.group_id, row.edge_node_id)
        node = self.edge_nodes.get(key)
        if node is None:
            node = self.edge_nodes[key] = _EdgeNode()
        self.mqtt_message_count += 1
        self.update_required = True
        kind = row.message_type
        if kind in ("NBIRTH", "NDEATH", "DBIRTH"):
            payload = _payload(row.msg)
            when = payload.get("timestamp")
            # a Java Date prints any long, Python's dates end at year 9999: a timestamp no date holds (microseconds,
            # garbage) would make every later refresh raise, so the row's receive time stands in
            when = when if isinstance(when, int) and 0 <= when < _LAST_DATE_MS else row.inserted_at
            if kind == "NBIRTH":
                self._templates(key, payload)
                node.online, node.connect_ms = True, when
                node.births += 1
            elif kind == "NDEATH":
                node.online, node.disconnect_ms = False, when
                node.deaths += 1
            else:
                self._templates(key + (row.device_id,), payload)
        node.messages += 1

    def on_client(self, source: str, connected: bool, now_ms: int | None = None) -> None:
        """A Sparkplug source connected to its MQTT server, or lost its connection."""
        c = self.clients.get(source)
        if c is None:
            return
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        c.connected = connected
        if connected:
            c.connect_ms = now_ms
        else:
            c.disconnect_ms = now_ms
        self.update_required = True

    def add_client(self, source: str, client_id: str, server_url: str) -> None:
        """A Sparkplug source's MQTT client (again when it starts: its id and URL as it uses them, state kept)."""
        c = self.clients.get(source)
        if c is None:
            self.clients[source] = _Client(client_id, server_url)
        else:
            c.client_id, c.server_url = client_id, server_url

    def _templates(self, descriptor: tuple, payload: dict[str, Any]) -> None:
        for m in payload.get("metrics") or []:
            if isinstance(m, dict) and m.get("dataType") == "Template" and isinstance(m.get("value"), dict):
                if m["value"].get("isDefinition"):
                    self.models.add(m.get("name"))
                else:
                    self.instances.setdefault(descriptor, set()).add(m.get("name"))


def _payload(msg: str) -> dict[str, Any]:
    try:
        value = json.loads(msg)
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


class _MetaEdgeNode:
    """One diagnostics edge node: NBIRTH (bdSeq + definitions) and DBIRTH (instances) first, DDATA after."""

    def __init__(self, group_id: str, edge_node_id: str, device_id: str):
        self.group_id, self.edge_node_id, self.device_id = group_id, edge_node_id, device_id
        self.online = False
        self.seq = self.bd_seq = 0

    def rows(self, metrics: list[dict[str, Any]], now_ms: int, metric_ms: int) -> list[RawRow]:
        out = []
        if not self.online:
            birth = [_metric("bdSeq", "Int64", self.bd_seq, metric_ms)]
            self.bd_seq = 0 if self.bd_seq == 255 else self.bd_seq + 1
            birth += [m for m in metrics if m["value"]["isDefinition"]]
            out.append(self._row("NBIRTH", None, {"timestamp": now_ms, "metrics": birth, "seq": self._next_seq()}, now_ms))
            kind, self.online = "DBIRTH", True
        else:
            kind = "DDATA"
        instances = [m for m in metrics if not m["value"]["isDefinition"]]
        out.append(self._row(kind, self.device_id, {"timestamp": now_ms, "metrics": instances, "seq": self._next_seq()},
                             now_ms))
        return out

    def _next_seq(self) -> int:
        seq = self.seq
        self.seq = 0 if seq == 255 else seq + 1
        return seq

    def _row(self, kind: str, device_id: str | None, payload: dict[str, Any], now_ms: int) -> RawRow:
        topic = f"{NAMESPACE}/{self.group_id}/{kind}/{self.edge_node_id}" + (f"/{device_id}" if device_id else "")
        return RawRow(msg_id=str(uuid.uuid4()), msg_topic=topic, namespace=NAMESPACE, group_id=self.group_id,
                      message_type=kind, edge_node_id=self.edge_node_id, device_id=device_id, msg=dumps(payload),
                      inserted_at=now_ms)


def _metric(name: str, data_type: str, value: Any, ms: int) -> dict[str, Any]:
    return {"name": name, "timestamp": ms, "dataType": data_type, "value": value}


def _template(name: str, metrics: list[dict[str, Any]], definition: bool, reference: str | None, ms: int) -> dict:
    value: dict[str, Any] = {"version": TEMPLATE_VERSION}
    if reference is not None:
        value["reference"] = reference
    value["isDefinition"] = definition
    value["metrics"] = metrics
    return _metric(name, "Template", value, ms)


class Diagnostics:
    """Builds the diagnostics rows from a ``Tracker``: ``start_rows`` once, then ``refresh_rows`` every 10 s."""

    def __init__(self, tracker: Tracker, settings: dict[str, Any] | None = None, version: str = "",
                 primary_host_id: Callable[[], str | None] = lambda: None, tz: datetime.tzinfo | None = None,
                 clock: Callable[[], float] = time.time):
        s = {**DEFAULTS, **{k: v for k, v in (settings or {}).items() if k in DEFAULTS and str(v or "").strip()}}
        self.group_id, self.model = str(s["group_id"]), str(s["bridge_model"])
        self.tracker, self.version, self._primary_host_id, self._tz, self._clock = tracker, version, primary_host_id, tz, clock
        self._bridge = _MetaEdgeNode(self.group_id, str(s["bridge_info_edge_node_id"]), self.model)
        self._edge = _MetaEdgeNode(self.group_id, str(s["edge_node_info_edge_node_id"]), EDGE_NODE_MODEL)
        self._client = _MetaEdgeNode(self.group_id, str(s["mqtt_client_info_edge_node_id"]), MQTT_CLIENT_MODEL)

    def _ms(self) -> int:
        return int(self._clock() * 1000)

    def start_rows(self, now_ms: int | None = None, metric_ms: int | None = None) -> list[RawRow]:
        """The births of the three edge nodes, then the edge nodes known so far (``Diagnostics.start``)."""
        now = self._ms() if now_ms is None else now_ms
        ms = now if metric_ms is None else metric_ms
        rows = self._bridge.rows([self._bridge_template(True, ms), self._bridge_template(False, ms)], now, ms)
        rows += self._edge.rows([_template(EDGE_NODE_MODEL, self._edge_definition(ms), True, None, ms),
                                 self._edge_instance(self._edge.group_id, self._edge.edge_node_id, None, ms)], now, ms)
        client_metrics = []
        for c in self.tracker.clients.items():
            client_metrics += [self._client_template(c, True, ms), self._client_template(c, False, ms)]
        rows += self._client.rows(client_metrics, now, ms)
        rows += self._edge.rows(self._edge_instances(ms), now, ms)
        self.tracker.update_required = False
        return rows

    def refresh_rows(self, now_ms: int | None = None, metric_ms: int | None = None) -> list[RawRow]:
        """A DDATA per edge node if anything changed since the last rows (``DynamicDiagnostics``), else none."""
        if not self.tracker.update_required:
            return []
        now = self._ms() if now_ms is None else now_ms
        ms = now if metric_ms is None else metric_ms
        rows = self._bridge.rows([self._bridge_template(False, ms)], now, ms)
        rows += self._client.rows([self._client_template(c, False, ms) for c in self.tracker.clients.items()], now, ms)
        rows += self._edge.rows(self._edge_instances(ms), now, ms)
        self.tracker.update_required = False
        return rows

    # ------------------------------------------------------------------ the templates

    def _bridge_template(self, definition: bool, ms: int) -> dict[str, Any]:
        t = self.tracker
        online = sum(1 for n in t.edge_nodes.values() if n.online)
        metrics = [_metric(f"{self.model} Version", "String", self.version, ms),
                   _metric("PrimaryHostId", "String", self._primary_host_id(), ms),
                   _metric("ConnectedEdgeNodes", "Int32", online, ms),
                   _metric("DisconnectedEdgeNodes", "Int32", len(t.edge_nodes) - online, ms),
                   _metric("MQTTMessageCount", "Int32", _int32(t.mqtt_message_count), ms),
                   _metric("ModelCount", "Int32", len(t.models), ms),
                   _metric("InstanceCount", "Int32", sum(len(v) for v in t.instances.values()), ms)]
        name = self.model if definition else f"Core/{self.model}"
        return _template(name, metrics, definition, None if definition else self.model, ms)

    def _edge_definition(self, ms: int) -> list[dict[str, Any]]:
        return [_metric("GroupId", "String", "", ms), _metric("EdgeNodeId", "String", "", ms),
                _metric("Connected", "String", "", ms), _metric("ConnectDateTime", "String", "", ms),
                _metric("DisconnectDateTime", "String", "", ms), _metric("BirthCount", "Int32", 0, ms),
                _metric("DeathCount", "Int32", 0, ms), _metric("MQTTMessageCount", "Int32", 0, ms)]

    def _edge_instance(self, group_id: str, edge_node_id: str, node: _EdgeNode | None, ms: int) -> dict[str, Any]:
        node = node or _EdgeNode()
        metrics = [_metric("GroupId", "String", group_id, ms), _metric("EdgeNodeId", "String", edge_node_id, ms),
                   _metric("Connected", "Boolean", node.online, ms),
                   _metric("ConnectDateTime", "String", java_date_string(node.connect_ms, self._tz), ms),
                   _metric("DisconnectDateTime", "String", java_date_string(node.disconnect_ms, self._tz), ms),
                   _metric("BirthCount", "Int32", _int32(node.births), ms),
                   _metric("DeathCount", "Int32", _int32(node.deaths), ms),
                   _metric("MQTTMessageCount", "Int32", _int32(node.messages), ms)]
        return _template(f"Core/{EDGE_NODE_MODEL}-{edge_node_id}", metrics, False, EDGE_NODE_MODEL, ms)

    def _edge_instances(self, ms: int) -> list[dict[str, Any]]:
        return [self._edge_instance(g, e, n, ms) for (g, e), n in self.tracker.edge_nodes.items()]

    def _client_template(self, item: tuple[str, _Client], definition: bool, ms: int) -> dict[str, Any]:
        source, c = item
        metrics = [_metric("ClientID", "String", c.client_id, ms),
                   _metric("Connected", "String", "true" if c.connected else "false", ms),
                   _metric("ConnectDateTime", "String", java_date_string(c.connect_ms, self._tz) if c.connect_ms else "null", ms),
                   _metric("DisconnectDateTime", "String",
                           java_date_string(c.disconnect_ms, self._tz) if c.disconnect_ms else "null", ms),
                   _metric("ServerUrl", "String", c.server_url, ms), _metric("ServerName", "String", source, ms)]
        name = MQTT_CLIENT_MODEL if definition else f"Core/{MQTT_CLIENT_MODEL}"
        return _template(name, metrics, definition, None if definition else MQTT_CLIENT_MODEL, ms)


def _int32(v: int) -> int:
    """A Java int counter (wraps past 2^31 - 1)."""
    return ((v & 0xFFFFFFFF) ^ 0x80000000) - 0x80000000
