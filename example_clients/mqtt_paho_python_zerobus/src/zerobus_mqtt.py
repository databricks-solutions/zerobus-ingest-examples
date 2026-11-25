"""Long-running Zerobus Ingest MQTT publisher built on Paho MQTT 2.x.

Each connection is a new Paho client whose CONNECT carries a freshly fetched OAuth token.
Each QoS 1 record is tracked until its PUBACK arrives. Unacknowledged records are retried
with backoff, and the retry policy accounts for duplicates (see the README).

Threading: the application thread owns the publisher state. Paho's network thread only puts
events on a queue and sets a per-connection event. Paho calls the PUBACK callback while it holds
its own output-queue mutex, so the callbacks take no publisher lock: a publish() made under
self._lock can never wait on a callback that waits on self._lock.
"""

import base64
import json
import logging
import os
import queue
import ssl
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional

import paho.mqtt.client as mqtt
import requests

PORT = 8883
KEEPALIVE_SECONDS = 60
CONNECT_TIMEOUT_SECONDS = 10
CONNACK_TIMEOUT_SECONDS = 30
# The CONNACK Receive Maximum is 4,096. Stay well below it.
MAX_INFLIGHT = 100
# Complete packet limit after CONNECT is 64 KiB. Keep a margin for the topic and header.
MAX_PUBLISH_PAYLOAD_BYTES = 64 * 1024 - 1024
# Attempts per record. A record that keeps failing is usually malformed, so the publisher gives up
# after a small number of tries instead of retrying forever.
MAX_ATTEMPTS = 5  # PUBACK with a failure reason code other than 0x99
MAX_CONNECTION_LOSS_ATTEMPTS = 2  # connection closed while this record was the only one in flight
MAX_BAD_RECORD_ATTEMPTS = 2  # PUBACK 0x99 (Payload format invalid): the payload isn't valid JSON
PAYLOAD_FORMAT_INVALID = 0x99
MAX_BACKOFF_SECONDS = 30
# Durable records, failures and rejections are counted in full. Only the last few are kept.
RECENT_RECORDS = 50

log = logging.getLogger("zerobus_mqtt")


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Set the {name} environment variable")
    return value


@dataclass(frozen=True)
class Config:
    workspace_url: str
    workspace_id: str
    client_id: str
    client_secret: str = field(repr=False)
    table_name: str
    mqtt_host: str

    @classmethod
    def from_env(cls) -> "Config":
        table_name = _required_env("DATABRICKS_TABLE_NAME")
        if table_name.count(".") != 2:
            raise RuntimeError("DATABRICKS_TABLE_NAME must be catalog.schema.table")
        return cls(
            workspace_url=_required_env("DATABRICKS_WORKSPACE_URL").rstrip("/"),
            workspace_id=_required_env("DATABRICKS_WORKSPACE_ID"),
            client_id=_required_env("DATABRICKS_CLIENT_ID"),
            client_secret=_required_env("DATABRICKS_CLIENT_SECRET"),
            table_name=table_name,
            mqtt_host=_required_env("ZEROBUS_MQTT_HOST"),
        )


def fetch_token(config: Config) -> str:
    """Client credentials grant, scoped to the target table (see zerobus-mqtt.md)."""
    catalog, schema, _ = config.table_name.split(".")
    authorization_details = [
        {"type": "unity_catalog_privileges", "privileges": ["USE CATALOG"],
         "object_type": "CATALOG", "object_full_path": catalog},
        {"type": "unity_catalog_privileges", "privileges": ["USE SCHEMA"],
         "object_type": "SCHEMA", "object_full_path": f"{catalog}.{schema}"},
        {"type": "unity_catalog_privileges", "privileges": ["SELECT", "MODIFY"],
         "object_type": "TABLE", "object_full_path": config.table_name},
    ]
    response = requests.post(
        f"{config.workspace_url}/oidc/v1/token",
        auth=(config.client_id, config.client_secret),
        data={
            "grant_type": "client_credentials",
            "scope": "all-apis",
            "resource": f"api://databricks/workspaces/{config.workspace_id}/zerobusDirectWriteApi",
            "authorization_details": json.dumps(authorization_details),
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def token_times(token: str) -> dict[str, Any]:
    """Return only the iat and exp claims, so logs show that a token was refreshed, never the token."""
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return {"iat": claims.get("iat"), "exp": claims.get("exp")}
    except (IndexError, ValueError):
        return {}


@dataclass
class Pending:
    payload: bytes
    record: dict
    attempts: int = 0  # PUBLISH packets sent for this record
    puback_failures: int = 0  # PUBACKs with a failure reason code
    connection_losses: int = 0  # connections that closed while this record was the only one in flight
    suspect: bool = False  # in flight when a connection closed with other records; sent alone until answered
    not_before: float = 0.0
    last_reason: str = ""


class _Attempt:
    """One connection attempt. Paho passes it to the callbacks as userdata, so each event names its connection."""

    def __init__(self):
        self.connack = threading.Event()
        self.reason = None  # CONNACK reason code, once received
        self.closing = False  # set by _teardown before it closes this connection


class ZerobusMqttPublisher:
    def __init__(self, config: Config):
        self.config = config
        self._lock = threading.RLock()  # guards queue, in-flight records and counters; never taken by callbacks
        self._connect_lock = threading.RLock()  # one connect or reconnect at a time; only one assigns _client
        self._events: queue.SimpleQueue = queue.SimpleQueue()  # from Paho callbacks to the application thread
        self._client: Optional[mqtt.Client] = None
        self._current: Optional[_Attempt] = None  # the attempt whose events are applied; others are ignored
        self._connected = threading.Event()
        self._inflight: dict[int, Pending] = {}  # PUBLISH mid -> record awaiting PUBACK
        self._queue: list[Pending] = []  # waiting to be sent or retried
        self.durable_count = 0
        self.failed_count = 0
        self.rejected_count = 0
        self.recent_durable: deque = deque(maxlen=RECENT_RECORDS)
        self.recent_failed: deque = deque(maxlen=RECENT_RECORDS)  # (record, reason)
        self.recent_rejected: deque = deque(maxlen=RECENT_RECORDS)  # (record, reason)
        self.connect_count = 0

    # ----- connection -------------------------------------------------------

    def _new_client(self, attempt: _Attempt) -> mqtt.Client:
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"zerobus-mqtt-{uuid.uuid4()}",
            protocol=mqtt.MQTTv5,
            # The app reconnects itself with a fresh token, so Paho's automatic reconnect is off.
            reconnect_on_failure=False,
            userdata=attempt,
        )
        client.on_connect = self._on_connect
        client.on_publish = self._on_publish
        client.on_disconnect = self._on_disconnect
        client.connect_timeout = CONNECT_TIMEOUT_SECONDS
        client.max_inflight_messages_set(MAX_INFLIGHT)
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        return client

    def connect(self) -> None:
        """Fetch a new token and open a new connection. Called at start and on every reconnect."""
        with self._connect_lock:
            token = fetch_token(self.config)
            log.info("token fetched %s", token_times(token))
            properties = mqtt.Properties(mqtt.PacketTypes.CONNECT)
            properties.UserProperty = [
                ("Authorization", f"Bearer {token}"),
                ("x-databricks-zerobus-table-name", self.config.table_name),
            ]
            del token

            attempt = _Attempt()
            client = self._new_client(attempt)
            result = client.connect(
                self.config.mqtt_host,
                port=PORT,
                keepalive=KEEPALIVE_SECONDS,
                clean_start=True,
                properties=properties,
            )
            del properties
            if result != mqtt.MQTT_ERR_SUCCESS:
                raise ConnectionError(f"MQTT connect failed: {mqtt.error_string(result)}")
            client.loop_start()

            if not attempt.connack.wait(CONNACK_TIMEOUT_SECONDS) or attempt.reason is None:
                client.disconnect()
                client.loop_stop()
                raise TimeoutError("Timed out waiting for CONNACK")
            if attempt.reason.is_failure:
                client.disconnect()
                client.loop_stop()
                raise ConnectionRefusedError(f"CONNECT rejected: {attempt.reason}")

            with self._lock:
                self._current = attempt
                self._client = client
                self._connected.set()
                self.connect_count += 1
            log.info("connected (connection %d)", self.connect_count)


    def _teardown(self, reason: str) -> None:
        """Close the current connection, if any, and requeue what it left unacknowledged.

        The order matters:
        1. Mark the attempt as closing, so the DISCONNECT that Paho echoes back charges nobody.
        2. Disconnect and stop the network thread. loop_stop() joins that thread, so every callback
           this connection will make is queued when it returns.
        3. Drain the queue while this attempt is still the current one. Acknowledgments that arrived
           before the close are applied, so their records are not resent.
        4. Requeue only the records still unacknowledged, and retire the attempt.

        A close the server reported before this point is charged first, as in _handle_disconnect().
        """
        with self._lock:
            old = self._client
            attempt = self._current
        if attempt is not None:
            attempt.closing = True
        if old is not None:
            old.disconnect()
            old.loop_stop()  # joins the network thread; keep it outside self._lock
        with self._lock:
            self._drain()
            self._requeue_inflight(reason, charge=False)
            self._client = None
            self._current = None
            self._connected.clear()

    def reconnect(self, reason: str) -> None:
        """Drop the current connection and connect with a new token.

        Records in flight are retried. This close is ours, not the server's, so no record is charged.
        """
        with self._connect_lock:
            self._teardown(reason)
            self.connect()

    def ensure_connected(self) -> None:
        """Connect with backoff until a connection is up. Only one connect attempt runs at a time."""
        backoff = 1.0
        while True:
            with self._connect_lock:
                self._drain()
                if self._connected.is_set():
                    return
                try:
                    if self._client is not None:
                        self._teardown("connection lost")
                    self.connect()
                    return
                except (OSError, TimeoutError, requests.RequestException) as exc:
                    log.warning("connect failed: %s; retrying in %.0fs", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

    # ----- callbacks (Paho network thread) ----------------------------------
    # Paho may call these while it holds its own output-queue mutex. They take no publisher lock:
    # they record the event and return. The application thread applies it in _drain().

    def _on_connect(self, client, attempt, flags, reason_code, properties):
        attempt.reason = reason_code
        attempt.connack.set()

    def _on_disconnect(self, client, attempt, flags, reason_code, properties):
        attempt.connack.set()  # unblocks a connect() that is still waiting for CONNACK
        # Paho also reports our own DISCONNECT here. Once _teardown has set closing, that close is ours.
        kind = "closed" if attempt.closing else "disconnect"
        self._events.put((kind, attempt, reason_code))

    def _on_publish(self, client, attempt, mid, reason_code, properties):
        # A PUBACK can arrive before publish() returns. The event waits in the queue until
        # _drain() runs, which is after pump() has recorded the mid, so it always finds its record.
        self._events.put(("puback", attempt, mid, reason_code))

    def _drain(self) -> None:
        """Apply the callback events for the current connection, in the order they arrived."""
        with self._lock:
            while True:
                try:
                    event = self._events.get_nowait()
                except queue.Empty:
                    return
                kind, attempt = event[0], event[1]
                if attempt is not self._current:
                    continue  # from a connection that was already closed or replaced
                if kind == "puback":
                    self._apply_puback(event[2], event[3])
                elif kind == "disconnect":
                    self._handle_disconnect(event[2])
                # "closed" is our own close, echoed by Paho. _teardown requeues what is left.

    def _apply_puback(self, mid: int, reason_code) -> None:
        pending = self._inflight.pop(mid, None)
        if pending is None:
            log.debug("PUBACK mid=%s matched nothing", mid)
            return
        pending.suspect = False  # the server answered it, so it's no longer an unattributed casualty
        log.debug("PUBACK mid=%s roll_id=%s reason=%s", mid, pending.record.get("roll_id"), reason_code)
        if reason_code.is_failure:
            self._retry_after_puback(pending, f"PUBACK {reason_code}",
                                     bad_record=reason_code.value == PAYLOAD_FORMAT_INVALID)
        else:
            self.durable_count += 1
            self.recent_durable.append(pending.record)

    def _handle_disconnect(self, reason_code) -> None:
        log.warning("disconnected: %s", reason_code)
        self._current = None
        self._connected.clear()
        self._requeue_inflight(f"disconnected: {reason_code}", charge=True)

    # ----- publishing -------------------------------------------------------

    def submit(self, record: dict) -> None:
        """Queue one record. It is sent now if connected, or on the next connection."""
        self.submit_raw(json.dumps(record).encode("utf-8"), record)

    def submit_raw(self, payload: bytes, record: dict) -> None:
        """Queue one payload as given. `record` is the bookkeeping copy used in the durable and failed lists.

        Use it to send a payload that isn't valid JSON, to test how the table handles it.
        Pass a dict with a `roll_id` so the outcome can be matched to the row.
        """
        with self._lock:
            self._queue.append(Pending(payload=payload, record=record))
        self.pump()

    def pump(self) -> None:
        """Send queued records while the connection is up and the in-flight window has room."""
        with self._lock:
            self._drain()
            if not self._connected.is_set() or self._client is None:
                return
            now = time.monotonic()
            # A record that was in flight at a close with other records is a suspect. Suspects are
            # sent one at a time, so the next close can be pinned on a single record. Other records
            # wait until no suspect is left.
            isolating = any(p.suspect for p in self._queue) or any(p.suspect for p in self._inflight.values())
            window = 1 if isolating else MAX_INFLIGHT
            remaining = []
            for pending in self._queue:
                if (len(self._inflight) >= window or pending.not_before > now
                        or (isolating and not pending.suspect)):
                    remaining.append(pending)
                    continue
                if len(pending.payload) > MAX_PUBLISH_PAYLOAD_BYTES:
                    # An oversized PUBLISH closes the connection. Retrying it never succeeds.
                    self._reject(pending, "payload exceeds the packet size limit")
                    continue
                info = self._client.publish(self.config.table_name, pending.payload, qos=1)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    remaining.append(pending)
                    continue
                pending.attempts += 1
                self._inflight[info.mid] = pending
                log.debug("PUBLISH mid=%s attempt=%d bytes=%d", info.mid, pending.attempts, len(pending.payload))
            self._queue = remaining

    def _retry_after_puback(self, pending: Pending, reason: str, bad_record: bool = False) -> None:
        pending.last_reason = reason
        pending.puback_failures += 1
        limit = MAX_BAD_RECORD_ATTEMPTS if bad_record else MAX_ATTEMPTS
        if pending.puback_failures >= limit:
            self._fail(pending, reason)
            return
        pending.not_before = time.monotonic() + min(2 ** pending.puback_failures, MAX_BACKOFF_SECONDS)
        self._queue.append(pending)

    def _requeue_inflight(self, reason: str, charge: bool) -> None:
        """Requeue the records a connection left unacknowledged.

        charge=True: the server closed the connection. If one record was in flight, the close is
        pinned on it, and it gives up after MAX_CONNECTION_LOSS_ATTEMPTS closes. If several were in
        flight, nobody is charged: each becomes a suspect and is sent alone until it's answered.
        charge=False: this client closed the connection (token rotation, shutdown). Nobody is charged.

        A record whose PUBACK was lost may already be durable, so a retry can create a duplicate row.
        """
        lost = list(self._inflight.values())
        self._inflight.clear()
        if not lost:
            return
        if not charge:
            self._queue.extend(lost)
            return
        if len(lost) == 1:
            pending = lost[0]
            pending.suspect = False
            pending.connection_losses += 1
            pending.last_reason = reason
            if pending.connection_losses >= MAX_CONNECTION_LOSS_ATTEMPTS:
                self._fail(pending, reason)
                return
            pending.not_before = time.monotonic() + min(2 ** pending.connection_losses, MAX_BACKOFF_SECONDS)
            self._queue.append(pending)
            return
        for pending in lost:
            pending.suspect = True
            pending.last_reason = reason
            self._queue.append(pending)

    def _fail(self, pending: Pending, reason: str) -> None:
        self.failed_count += 1
        self.recent_failed.append((pending.record, reason))
        log.error("giving up after %d attempts: %s", pending.attempts, reason)

    def _reject(self, pending: Pending, reason: str) -> None:
        self.rejected_count += 1
        self.recent_rejected.append((pending.record, reason))
        log.error("record rejected locally: %s", reason)

    # ----- shutdown ---------------------------------------------------------

    def flush(self, timeout: float = 60.0) -> bool:
        """Wait until every record is durable or has been given up on."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                self._drain()
                done = not self._queue and not self._inflight
            if done:
                return True
            if not self._connected.is_set():
                self.ensure_connected()
            self.pump()
            time.sleep(0.1)
        return False

    def close(self, timeout: float = 60.0) -> bool:
        """Flush, then send DISCONNECT and stop the network thread."""
        clean = self.flush(timeout)
        with self._connect_lock:
            self._teardown("closing")
        return clean
