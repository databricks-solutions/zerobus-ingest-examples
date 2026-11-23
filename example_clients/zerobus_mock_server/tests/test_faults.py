"""Fault catalogue, triggers, scope and the event log (unit tests)."""

import json

import pytest

from zerobus_mock_server.faults import FAULT_TYPES, FaultError, FaultRegistry


def reg(seed=0, log=None):
    return FaultRegistry(seed=seed, event_log_path=log)


def test_every_documented_fault_type_is_present():
    expected = {
        "token_invalid_client", "token_invalid_authorization_details", "http_error", "slow_response",
        "unauthorized", "permission_denied", "table_not_found", "schema_mismatch", "record_too_large",
        "grpc_status", "stream_abort", "close_stream_signal", "slow_ack", "drop_acks", "schema_change",
    }
    assert expected <= set(FAULT_TYPES)


def test_once_fires_one_time():
    r = reg()
    r.add({"type": "unauthorized", "protocol": "rest", "trigger": {"mode": "once"}})
    fired = [len(r.evaluate("insert", "rest", "t")) for _ in range(4)]
    assert fired == [1, 0, 0, 0]


def test_every_nth():
    r = reg()
    r.add({"type": "unauthorized", "trigger": {"mode": "every_nth", "n": 3}})
    fired = [len(r.evaluate("insert", "rest", "t")) for _ in range(7)]
    assert fired == [0, 0, 1, 0, 0, 1, 0]


def test_after_with_times():
    r = reg()
    r.add({"type": "unauthorized", "trigger": {"mode": "after", "after": 2, "times": 2}})
    fired = [len(r.evaluate("insert", "rest", "t")) for _ in range(6)]
    assert fired == [0, 0, 1, 1, 0, 0]


def test_probability_is_repeatable_for_a_seed():
    def run(seed):
        r = reg(seed)
        r.add({"type": "unauthorized", "trigger": {"mode": "probability", "probability": 0.5}})
        return [len(r.evaluate("insert", "rest", "t")) for _ in range(40)]

    assert run(11) == run(11)
    assert 5 < sum(run(11)) < 35


def test_scope_by_protocol_table_and_stream():
    r = reg()
    r.add({"type": "unauthorized", "protocol": "rest", "table": "a.b.c"})
    r.add({"type": "stream_abort", "stream": "mock-stream-2", "trigger": {"mode": "always"}})
    assert len(r.evaluate("insert", "rest", "a.b.c")) == 1
    assert r.evaluate("insert", "rest", "x.y.z") == []
    assert r.evaluate("insert", "grpc", "a.b.c") == []
    assert r.evaluate("ingest", "grpc", "t", "mock-stream-1") == []
    assert len(r.evaluate("ingest", "grpc", "t", "mock-stream-2")) == 1


@pytest.mark.parametrize("spec", [
    {"type": "nope"},
    {"type": "grpc_status", "protocol": "rest"},
    {"type": "unauthorized", "params": {"bogus": 1}},
    {"type": "http_error", "params": {"status": 418}},
    {"type": "grpc_status", "params": {"code": "NOPE"}},
    {"type": "unauthorized", "trigger": {"mode": "every_nth"}},
    {"type": "unauthorized", "trigger": {"mode": "probability", "probability": 2}},
    {"type": "stream_abort", "table": "x", "protocol": "rest"},
    {"type": "drop_acks", "params": {"count": -5}},
    {"type": "unauthorized", "protocol": "smtp"},
])
def test_invalid_specs_are_rejected(spec):
    with pytest.raises(FaultError):
        reg().add(spec)


def test_defaults_are_filled_in():
    f = reg().add({"type": "stream_abort"})
    assert f.params == {"retryable": True, "persist_unacked": True}
    assert f.protocol == "any" and f.table == "*" and f.stream == "*"


def test_every_injected_fault_is_logged_as_a_json_line(tmp_path):
    log = tmp_path / "events.jsonl"
    r = reg(log=log)
    f = r.add({"type": "unauthorized", "trigger": {"mode": "once"}})
    firing = r.evaluate("insert", "rest", "t")[0]
    r.log(firing, "401 UNAUTHENTICATED", "unit test")
    lines = [json.loads(x) for x in log.read_text().splitlines()]
    assert len(lines) == 1
    assert lines[0]["fault_id"] == f.id and lines[0]["action"] == "401 UNAUTHENTICATED"
    assert r.events(since=0)[0]["seq"] == 1
    assert r.events(since=1) == []
