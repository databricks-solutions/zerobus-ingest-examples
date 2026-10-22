"""One ``sparkplug_raw`` row per Sparkplug B message.

The columns of ``sparkplug_raw``: ``msg_id``,
``msg_topic``, ``namespace``, ``group_id``, ``message_type``, ``edge_node_id``, ``device_id``, ``msg``
(VARIANT: the payload as Tahu JSON, ``to_tahu_json``) and ``inserted_at`` (DECIMAL(38,0): epoch ms at receipt).

Stored: births, deaths and data of
edge nodes and devices. Skipped: host STATE, other namespaces, commands (NCMD, DCMD),
malformed ``spBv1.0/...`` topics and payloads that can't be decoded.
"""
from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass

from sparkplug_connector.sparkplug.tahu_json import to_tahu_json

NAMESPACE = "spBv1.0"
NODE_TYPES = frozenset({"NBIRTH", "NDEATH", "NDATA", "NCMD"})
DEVICE_TYPES = frozenset({"DBIRTH", "DDEATH", "DDATA", "DCMD"})
COMMANDS = frozenset({"NCMD", "DCMD"})
COLUMNS = ("msg_id", "msg_topic", "namespace", "group_id", "message_type", "edge_node_id", "device_id", "msg", "inserted_at")


@dataclass(frozen=True)
class SparkplugTopic:
    """An edge node or device message; ``device_id`` is None for node messages."""
    namespace: str
    group_id: str
    message_type: str
    edge_node_id: str
    device_id: str | None


@dataclass(frozen=True)
class StateTopic:
    """A host application's STATE (``spBv1.0/STATE/<host>``, or legacy ``STATE/<host>``)."""
    host_id: str
    legacy: bool


@dataclass(frozen=True)
class OtherTopic:
    """Anything else: not Sparkplug B, or a malformed Sparkplug topic (``reason`` says why)."""
    reason: str


def parse_topic(topic: str) -> SparkplugTopic | StateTopic | OtherTopic:
    """``spBv1.0/<group>/<type>/<edge>`` (node) or ``spBv1.0/<group>/<type>/<edge>/<device>`` (device)."""
    p = topic.split("/")
    if len(p) == 2 and p[0] == "STATE" and p[1]:
        return StateTopic(p[1], legacy=True)
    if p[0] != NAMESPACE:
        return OtherTopic(f"not a {NAMESPACE} topic")
    if len(p) == 3 and p[1] == "STATE" and p[2]:
        return StateTopic(p[2], legacy=False)
    if not 4 <= len(p) <= 5:
        return OtherTopic(f"expected 4 or 5 topic levels, got {len(p)}")
    if not all(p):
        return OtherTopic("empty topic level")
    message_type = p[2]
    if message_type in NODE_TYPES and len(p) == 4:
        return SparkplugTopic(p[0], p[1], message_type, p[3], None)
    if message_type in DEVICE_TYPES and len(p) == 5:
        return SparkplugTopic(p[0], p[1], message_type, p[3], p[4])
    if message_type in NODE_TYPES or message_type in DEVICE_TYPES:
        return OtherTopic(f"{message_type} with {len(p)} topic levels")
    return OtherTopic(f"unknown Sparkplug message type {message_type}")


class Skip(str, enum.Enum):
    """Why a message gets no row."""
    STATE = "state"
    FOREIGN_TOPIC = "foreign_topic"
    COMMAND = "command"
    MALFORMED_TOPIC = "malformed_topic"
    UNDECODABLE_PAYLOAD = "undecodable_payload"


@dataclass(frozen=True)
class RawRow:
    msg_id: str
    msg_topic: str
    namespace: str
    group_id: str
    message_type: str
    edge_node_id: str
    device_id: str | None
    msg: str
    inserted_at: int

    def to_zerobus_record(self) -> dict[str, str | None]:
        """The row as a Zerobus JSON record, in the table's column order. ``msg`` (VARIANT) goes as its
        JSON text and ``inserted_at`` (DECIMAL(38,0)) as a string of digits: Zerobus rejects an object
        for a VARIANT and a JSON number for a DECIMAL."""
        return {
            "msg_id": self.msg_id,
            "msg_topic": self.msg_topic,
            "namespace": self.namespace,
            "group_id": self.group_id,
            "message_type": self.message_type,
            "edge_node_id": self.edge_node_id,
            "device_id": self.device_id,
            "msg": self.msg,
            "inserted_at": str(self.inserted_at),
        }


@dataclass(frozen=True)
class Outcome:
    """A row, or why there is none (``detail``: the topic or decode error, for the log)."""
    row: RawRow | None = None
    skip: Skip | None = None
    detail: str | None = None


def convert(topic: str, payload: bytes, received_at_ms: int) -> Outcome:
    """The row for a message received at ``received_at_ms`` (epoch ms), or why it gets none."""
    parsed = parse_topic(topic)
    if isinstance(parsed, StateTopic):
        return Outcome(skip=Skip.STATE)
    if isinstance(parsed, OtherTopic):
        skip = Skip.MALFORMED_TOPIC if topic.startswith(NAMESPACE + "/") else Skip.FOREIGN_TOPIC
        return Outcome(skip=skip, detail=parsed.reason)
    if parsed.message_type in COMMANDS:
        return Outcome(skip=Skip.COMMAND)
    try:
        msg = to_tahu_json(payload)
    except Exception as e:  # noqa: BLE001 - one bad payload never stops the source
        return Outcome(skip=Skip.UNDECODABLE_PAYLOAD, detail=f"{e}, {len(payload)} bytes")
    return Outcome(row=RawRow(
        msg_id=str(uuid.uuid4()), msg_topic=topic, namespace=NAMESPACE, group_id=parsed.group_id,
        message_type=parsed.message_type, edge_node_id=parsed.edge_node_id, device_id=parsed.device_id,
        msg=msg, inserted_at=received_at_ms))
