"""Offline tests for the retry and bookkeeping logic. No broker or network is used.

A scripted FakeClient stands in for paho.mqtt.client.Client. Like Paho, it can call the PUBACK
callback from inside publish(), and it can make the "server" close a connection on a payload.

Run from the folder that holds src/:  PYTHONPATH=src python -m pytest tests
"""

import base64
import json
import threading
import time
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

import zerobus_mqtt as zm


def make_config(**overrides):
    values = dict(
        workspace_url="https://example.invalid",
        workspace_id="1234",
        client_id="client",
        client_secret="secret",
        table_name="main.default.dice_rolls",
        mqtt_host="1234.zerobus.us-west-2.example.invalid",
    )
    values.update(overrides)
    return zm.Config(**values)


def puback(reason_value):
    return mqtt.ReasonCode(mqtt.PacketTypes.PUBACK, identifier=reason_value)


class FakeBroker:
    """Scripted server. Payloads containing `close_on` make the server close the connection."""

    def __init__(self, close_on=b"BAD", auto_ack=True):
        self.close_on = close_on
        self.auto_ack = auto_ack
        self.sent = []  # every PUBLISH payload, in order
        self.open_clients = []
        self.acks_while_stopping = []  # mids whose PUBACK the network thread queues during loop_stop()

    def new_client(self, attempt):
        return FakeClient(self, attempt)


class FakeClient:
    def __init__(self, broker, userdata):
        self.broker = broker
        self.userdata = userdata
        self.on_connect = self.on_publish = self.on_disconnect = None
        self.open = False
        self.mid = 0

    def max_inflight_messages_set(self, n):
        pass

    def tls_set(self, **kwargs):
        pass

    def connect(self, host, port, keepalive, clean_start, properties):
        return mqtt.MQTT_ERR_SUCCESS

    def loop_start(self):
        self.open = True
        self.broker.open_clients.append(self)
        self.on_connect(self, self.userdata, None, mqtt.ReasonCode(mqtt.PacketTypes.CONNACK, identifier=0), None)

    def loop_stop(self):
        # Paho's network thread can still deliver PUBACKs while the loop stops, before the join returns.
        for mid in self.broker.acks_while_stopping:
            self.on_publish(self, self.userdata, mid, puback(0), None)
        self.broker.acks_while_stopping = []

    def disconnect(self):
        if self.open:
            self._drop()
            self.on_disconnect(self, self.userdata, None, mqtt.ReasonCode(mqtt.PacketTypes.DISCONNECT, identifier=0), None)

    def publish(self, topic, payload, qos):
        if not self.open:
            return SimpleNamespace(rc=mqtt.MQTT_ERR_NO_CONN, mid=0)
        self.mid += 1
        mid = self.mid
        self.broker.sent.append(payload)
        if self.broker.close_on in payload:
            self.server_close()
        elif self.broker.auto_ack:
            # Paho can call on_publish before publish() returns, on its network thread.
            self.on_publish(self, self.userdata, mid, puback(0), None)
        return SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS, mid=mid)

    def server_close(self):
        self._drop()
        self.on_disconnect(self, self.userdata, None,
                           mqtt.ReasonCode(mqtt.PacketTypes.DISCONNECT, identifier=0x83), None)

    def _drop(self):
        self.open = False
        if self in self.broker.open_clients:
            self.broker.open_clients.remove(self)


@pytest.fixture
def broker_publisher(monkeypatch):
    monkeypatch.setattr(zm, "fetch_token", lambda config: "header.e30.signature")
    broker = FakeBroker()
    pub = zm.ZerobusMqttPublisher(make_config())

    def new_client(attempt):
        client = broker.new_client(attempt)  # same callbacks as the real _new_client wires up
        client.on_connect = pub._on_connect
        client.on_publish = pub._on_publish
        client.on_disconnect = pub._on_disconnect
        return client

    monkeypatch.setattr(pub, "_new_client", new_client)
    return broker, pub


def connected_publisher(mid=7, attempts=1):
    """A publisher with one record in flight on the current connection (no broker needed)."""
    pub = zm.ZerobusMqttPublisher(make_config())
    attempt = zm._Attempt()
    pub._current = attempt
    pub._client = object()
    pub._connected.set()
    pending = zm.Pending(payload=b"{}", record={"roll_id": "r1"}, attempts=attempts)
    pub._inflight[mid] = pending
    return pub, pending, attempt


def deliver(pub, attempt, mid, reason_value):
    pub._on_publish(None, attempt, mid, puback(reason_value), None)
    pub._drain()


# ----- config and token ---------------------------------------------------

def test_config_rejects_two_part_table_name(monkeypatch):
    env = {
        "DATABRICKS_WORKSPACE_URL": "https://example.invalid",
        "DATABRICKS_WORKSPACE_ID": "1",
        "DATABRICKS_CLIENT_ID": "c",
        "DATABRICKS_CLIENT_SECRET": "s",
        "DATABRICKS_TABLE_NAME": "schema.table",
        "ZEROBUS_MQTT_HOST": "h",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(RuntimeError, match="catalog.schema.table"):
        zm.Config.from_env()


def test_config_repr_hides_secret():
    assert "secret" not in repr(make_config())


def test_token_times_reads_claims_only():
    payload = base64.urlsafe_b64encode(json.dumps({"iat": 1, "exp": 3601}).encode()).decode().rstrip("=")
    token = f"header.{payload}.signature"
    assert zm.token_times(token) == {"iat": 1, "exp": 3601}
    assert zm.token_times("not-a-jwt") == {}


# ----- PUBACK handling ----------------------------------------------------

def test_successful_puback_marks_record_durable():
    pub, pending, att = connected_publisher(mid=7)
    deliver(pub, att, 7, 0)
    assert pub.durable_count == 1 and list(pub.recent_durable) == [pending.record]
    assert not pub._inflight and not pub._queue


def test_failed_puback_is_retried_with_backoff():
    pub, pending, att = connected_publisher(mid=7)
    deliver(pub, att, 7, 0x80)
    assert pending in pub._queue
    assert pending.not_before > 0
    assert pub.durable_count == 0


def test_callbacks_from_a_replaced_connection_are_ignored():
    pub, pending, _ = connected_publisher(mid=7)
    stale = zm._Attempt()
    deliver(pub, stale, 7, 0)
    assert pub.durable_count == 0 and 7 in pub._inflight


def test_payload_format_invalid_is_retried_once_then_failed():
    # PUBACK 0x99: the payload isn't valid JSON. The server answers, so the connection stays up,
    # and the record gets MAX_BAD_RECORD_ATTEMPTS (2) sends in total.
    pub, pending, att = connected_publisher(mid=7)
    deliver(pub, att, 7, 0x99)
    assert pending in pub._queue and pub.failed_count == 0
    pub._queue.clear()
    pending.not_before = 0.0
    pub._inflight[8] = pending
    deliver(pub, att, 8, 0x99)
    assert pub.failed_count == 1 and pub.recent_failed[0][0] == pending.record
    assert "Payload format invalid" in pub.recent_failed[0][1]
    assert pub.durable_count == 0 and not pub._queue


def test_puback_failures_do_not_use_up_connection_loss_budget():
    pub, pending, att = connected_publisher(mid=7)
    deliver(pub, att, 7, 0x80)  # one PUBACK failure
    pending.not_before = 0.0
    pub._queue.clear()
    pub._inflight[8] = pending
    pub._requeue_inflight("disconnected", charge=True)  # lone record, one connection loss
    assert pending.connection_losses == 1 and pending.puback_failures == 1
    assert pub.failed_count == 0


# ----- connection losses (bad-record isolation) ---------------------------

def test_lone_record_is_charged_per_connection_loss():
    pub, pending, _ = connected_publisher()
    pub._requeue_inflight("disconnected", charge=True)
    assert pending in pub._queue and pub.failed_count == 0 and pending.connection_losses == 1
    pub._queue.clear()
    pub._inflight[8] = pending
    pub._requeue_inflight("disconnected", charge=True)
    assert pub.failed_count == 1 and pub.recent_failed[0][0] == pending.record
    assert not pub._queue


def test_close_with_several_records_in_flight_charges_none():
    pub, first, _ = connected_publisher(mid=1)
    second = zm.Pending(payload=b"{}", record={"roll_id": "r2"}, attempts=1)
    pub._inflight[2] = second
    pub._requeue_inflight("disconnected", charge=True)
    assert first.suspect and second.suspect
    assert first.connection_losses == 0 and second.connection_losses == 0
    assert pub.failed_count == 0
    assert {id(p) for p in pub._queue} == {id(first), id(second)}


def test_app_initiated_reconnect_charges_nobody():
    pub, pending, _ = connected_publisher()
    pub._requeue_inflight("demo: forced reconnect", charge=False)
    assert pending in pub._queue
    assert pending.connection_losses == 0 and not pending.suspect


# ----- teardown ordering (ITEM-061 revision 2) ----------------------------

def test_ack_queued_while_the_old_connection_stops_is_applied_not_resent(broker_publisher):
    # The network thread queues the PUBACK for mid 1 while loop_stop() runs, after the old teardown's
    # drain would already have finished. It must be applied, so the record is not requeued and sent again.
    # The reconnect must charge nothing: Paho's echo of our own DISCONNECT is not a server close.
    broker, pub = broker_publisher
    broker.auto_ack = False
    pub.connect()
    pub.submit({"roll_id": "A"})  # PUBLISH sent as mid 1; no PUBACK yet
    assert 1 in pub._inflight
    broker.acks_while_stopping = [1]
    pub.reconnect("demo: forced reconnect")
    pub.pump()  # would send the record again if it had been requeued
    assert pub.durable_count == 1
    assert not pub._inflight and not pub._queue and pub.failed_count == 0
    assert sum(1 for payload in broker.sent if json.loads(payload)["roll_id"] == "A") == 1
    assert pub.connect_count == 2


def test_server_close_reported_before_a_reconnect_is_still_charged(broker_publisher):
    broker, pub = broker_publisher
    broker.auto_ack = False
    pub.connect()
    pub.submit({"roll_id": "A"})
    pending = pub._inflight[1]
    pub._client.server_close()  # the close event waits in the queue until the next drain
    pub.reconnect("demo: forced reconnect")
    assert pending.connection_losses == 1  # the server's close is charged to the lone record
    assert pending in pub._queue and pub.failed_count == 0


def test_valid_records_sharing_closed_connections_with_a_bad_record_are_not_abandoned(broker_publisher, monkeypatch):
    monkeypatch.setattr(zm, "MAX_BACKOFF_SECONDS", 0)
    broker, pub = broker_publisher
    broker.auto_ack = False
    pub.connect()
    for roll in ("V1", "V2", "BAD", "V3"):
        pub.submit({"roll_id": roll})  # V1, V2 and BAD are in flight when the server closes
    broker.auto_ack = True
    assert pub.flush(timeout=10)
    assert pub.durable_count == 3
    assert [record["roll_id"] for record, _ in pub.recent_failed] == ["BAD"]
    assert pub.failed_count == 1
    assert pub.connect_count == 3


def test_suspects_are_sent_one_at_a_time(broker_publisher):
    broker, pub = broker_publisher
    broker.auto_ack = False
    pub.connect()
    for roll in ("A", "B", "C"):
        pub.submit({"roll_id": roll})
    assert len(broker.sent) == 3
    pub._client.server_close()
    pub.pump()  # applies the close: all three become suspects
    pub.ensure_connected()
    before = len(broker.sent)
    pub.pump()
    assert len(broker.sent) - before == 1  # a suspect goes out alone
    assert len(pub._inflight) == 1


def test_lone_bad_record_is_given_up_after_two_closes(broker_publisher, monkeypatch):
    monkeypatch.setattr(zm, "MAX_BACKOFF_SECONDS", 0)
    broker, pub = broker_publisher
    pub.connect()
    pub.submit({"roll_id": "BAD"})
    assert pub.flush(timeout=10)
    assert pub.durable_count == 0 and pub.failed_count == 1


# ----- lock order (finding 2) ----------------------------------------------

def test_paho_callbacks_do_not_wait_for_the_publisher_lock():
    pub, _, attempt = connected_publisher(mid=7)
    finished = threading.Event()

    def callbacks():
        pub._on_publish(None, attempt, 7, puback(0), None)
        pub._on_disconnect(None, attempt, None, puback(0), None)
        finished.set()

    with pub._lock:  # as pump() holds it while publish() runs
        worker = threading.Thread(target=callbacks, daemon=True)
        worker.start()
        assert finished.wait(5), "a Paho callback blocked on the publisher lock"


def test_puback_that_arrives_before_publish_returns_is_matched(broker_publisher):
    broker, pub = broker_publisher
    pub.connect()  # auto_ack: the PUBACK is delivered from inside publish()
    pub.submit({"roll_id": "early"})
    assert 1 in pub._inflight  # the mid was recorded
    pub.pump()
    assert pub.durable_count == 1 and not pub._inflight


# ----- bounded bookkeeping (findings 3 and 4) -------------------------------

def test_counts_are_exact_and_recent_lists_are_bounded(broker_publisher):
    broker, pub = broker_publisher
    pub.connect()
    for i in range(1000):
        pub.submit({"roll_id": f"r{i}"})
    assert pub.flush(timeout=10)
    assert pub.durable_count == 1000
    assert len(pub.recent_durable) == zm.RECENT_RECORDS

    for i in range(200):
        pub.submit_raw(b"x" * (zm.MAX_PUBLISH_PAYLOAD_BYTES + 1), {"roll_id": f"big{i}"})
    assert pub.rejected_count == 200
    assert len(pub.recent_rejected) == zm.RECENT_RECORDS


# ----- single reconnect owner (finding 5) ----------------------------------

def test_concurrent_reconnects_run_one_connect_at_a_time(broker_publisher, monkeypatch):
    broker, pub = broker_publisher
    state = {"now": 0, "max": 0}
    guard = threading.Lock()

    def slow_token(config):
        with guard:
            state["now"] += 1
            state["max"] = max(state["max"], state["now"])
        time.sleep(0.02)
        with guard:
            state["now"] -= 1
        return "header.e30.signature"

    monkeypatch.setattr(zm, "fetch_token", slow_token)
    pub.connect()
    threads = [threading.Thread(target=pub.reconnect, args=("scheduled",)) for _ in range(3)]
    threads += [threading.Thread(target=pub.ensure_connected) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert not any(t.is_alive() for t in threads)
    assert state["max"] == 1  # never two attempts in progress at once
    assert len(broker.open_clients) == 1  # every replaced connection was closed
    assert pub._client in broker.open_clients


# ----- helpers ---------------------------------------------------------------

def test_oversized_record_is_rejected_locally_without_a_connection():
    pub = zm.ZerobusMqttPublisher(make_config())
    pub.submit({"roll_id": "big", "blob": "x" * (zm.MAX_PUBLISH_PAYLOAD_BYTES + 1)})
    assert pub._queue and pub.rejected_count == 0  # not connected: stays queued, nothing sent
    pub._connected.set()
    pub._client = object()  # pump() only checks presence here; the oversize check runs first
    pub._current = zm._Attempt()
    pub.pump()
    assert pub.rejected_count == 1 and not pub._queue


def test_submit_raw_sends_payload_unchanged():
    pub = zm.ZerobusMqttPublisher(make_config())
    pub.submit_raw(b"not json ((", {"roll_id": "bad-1"})
    assert len(pub._queue) == 1
    assert pub._queue[0].payload == b"not json (("
    assert pub._queue[0].record == {"roll_id": "bad-1"}


def test_roll_loop_calls_ensure_connected_before_each_pump(monkeypatch):
    # The example's loop is the only place that reconnects a dropped connection mid-run.
    import sys
    import dice_roller

    calls = []

    class RecordingPublisher:
        connect_count = durable_count = failed_count = rejected_count = 0
        recent_failed = recent_rejected = []

        def __init__(self, config):
            pass

        def connect(self):
            calls.append("connect")

        def reconnect(self, reason):
            calls.append("reconnect")

        def submit(self, record):
            calls.append("submit")

        def submit_raw(self, payload, record):
            calls.append("submit_raw")

        def ensure_connected(self):
            calls.append("ensure_connected")

        def pump(self):
            calls.append("pump")

        def close(self):
            calls.append("close")
            return True

    monkeypatch.setattr(dice_roller, "ZerobusMqttPublisher", RecordingPublisher)
    monkeypatch.setattr(dice_roller.Config, "from_env", lambda: None)
    monkeypatch.setattr(sys, "argv", ["dice_roller.py", "--rolls", "4", "--interval", "0",
                                      "--force-reconnect-after", "2", "--send-invalid-record"])
    assert dice_roller.main() == 0
    pumps = [i for i, call in enumerate(calls) if call == "pump"]
    assert len(pumps) == 4 + 2  # four rolls plus the two bad records (the non-JSON one is not pumped)
    assert all(calls[i - 1] == "ensure_connected" for i in pumps)
