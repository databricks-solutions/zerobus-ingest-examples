"""Unit tests for the Python mock library: the controls, the caller-visible outcomes, and scenarios.json."""

import json
from pathlib import Path

import pytest

from zerobus_mock_lib import SCENARIOS, FakeMock, FakeZerobusSdk, ZerobusException

T = "main.default.events"
CONTRACT = Path(__file__).resolve().parents[2] / "scenarios.json"


def stream_for(mock=None):
    mock = mock or FakeMock()
    return FakeZerobusSdk(mock).create_stream("id", "secret", T), mock


def test_embedded_scenario_table_matches_scenarios_json():
    assert SCENARIOS == json.loads(CONTRACT.read_text())["scenarios"]


def test_every_scenario_in_the_contract_is_known_to_the_fake():
    assert set(SCENARIOS) == {
        "auth-failure", "table-not-found", "schema-mismatch", "record-too-large", "server-unavailable",
        "stream-abort-retryable", "stream-abort-fatal", "graceful-close", "no-acks",
        "schema-change-while-open", "token-expired",
    }


def test_records_are_recorded_in_order_with_offsets():
    s, _ = stream_for()
    assert [s.ingest_record_offset({"n": i}) for i in range(3)] == [0, 1, 2]
    assert s.ingested == [{"n": 0}, {"n": 1}, {"n": 2}]
    assert s.stored == s.ingested


def test_fail_next_after_skips_then_fails_once():
    s, mock = stream_for()
    mock.fail_next("schema-mismatch", after=2)
    s.ingest_record_offset("a")
    s.ingest_record_offset("b")
    with pytest.raises(ZerobusException, match="does not fit"):
        s.ingest_record_offset("c")
    assert s.ingest_record_offset("d") == 2  # one-shot: the next call is fine
    assert s.ingested == ["a", "b", "d"]


@pytest.mark.parametrize("scenario", ["schema-mismatch", "record-too-large"])
def test_held_ingest_scenario_fails_every_call_until_cleared(scenario):
    s, mock = stream_for()
    mock.set_scenario(scenario)
    for _ in range(2):
        with pytest.raises(ZerobusException):
            s.ingest_record_offset("x")
    assert s.stored == []
    mock.clear_scenario()
    assert s.ingest_record_offset("ok") == 0


def test_stream_abort_fatal_is_terminal_for_the_stream():
    s, mock = stream_for()
    s.ingest_record_offset("kept")
    mock.fail_next("stream-abort-fatal")
    with pytest.raises(ZerobusException, match="terminal stream error"):
        s.ingest_record_offset("x")
    with pytest.raises(ZerobusException, match="terminal stream error"):
        s.ingest_record_offset("after")
    with pytest.raises(ZerobusException, match="terminal stream error"):
        s.flush()
    assert s.stored == ["kept"]
    fresh, _ = stream_for(mock)
    assert fresh.ingest_record_offset("new stream") == 0


def test_held_stream_abort_fatal_fails_the_stream_for_good():
    s, mock = stream_for()
    mock.set_scenario("stream-abort-fatal")
    with pytest.raises(ZerobusException, match="terminal stream error"):
        s.ingest_record_offset("x")
    mock.clear_scenario()
    with pytest.raises(ZerobusException, match="terminal stream error"):
        s.ingest_record_offset("still dead")


def test_auth_failure_and_table_not_found_raise_at_create():
    mock = FakeMock()
    mock.fail_next("auth-failure")
    with pytest.raises(ZerobusException, match="invalid_client"):
        FakeZerobusSdk(mock).create_stream("id", "secret", T)
    mock.fail_next("table-not-found")
    with pytest.raises(ZerobusException, match="cannot be found"):
        FakeZerobusSdk(mock).create_stream("id", "secret", T)


def test_server_unavailable_holds_until_cleared():
    mock = FakeMock()
    mock.set_scenario("server-unavailable")
    with pytest.raises(ZerobusException, match="UNAVAILABLE"):
        FakeZerobusSdk(mock).create_stream("id", "secret", T)
    mock.clear_scenario()
    FakeZerobusSdk(mock).create_stream("id", "secret", T)


def test_token_expiry_at_create_is_refreshed_and_mid_stream_is_terminal():
    mock = FakeMock()
    mock.expire_token()
    s = FakeZerobusSdk(mock).create_stream("id", "secret", T)  # the SDK refreshes: no error
    mock.expire_token()
    with pytest.raises(ZerobusException, match="expired"):
        s.ingest_record_offset("late")
    with pytest.raises(ZerobusException, match="expired"):
        s.ingest_record_offset("later")  # the stream failed for good
    fresh, _ = stream_for(mock)
    assert fresh.ingest_record_offset("new") == 0


def test_no_acks_makes_flush_fail_and_records_are_kept():
    s, mock = stream_for()
    mock.set_scenario("no-acks")
    s.ingest_record_offset("a")
    with pytest.raises(ZerobusException, match="timed out"):
        s.flush()
    assert s.ingested == ["a"]
    assert s.get_unacked_records() == ["a"]


def test_clearing_no_acks_releases_the_withheld_records():
    s, mock = stream_for()
    mock.set_scenario("no-acks")
    s.ingest_record_offset("a")
    with pytest.raises(ZerobusException, match="timed out"):
        s.flush()
    mock.clear_scenario()
    s.flush()  # the acks arrive once the hold is cleared
    assert s.get_unacked_records() == []


def test_fail_next_no_acks_fails_one_flush():
    s, mock = stream_for()
    mock.fail_next("no-acks")
    s.ingest_record_offset("a")
    with pytest.raises(ZerobusException, match="timed out"):
        s.flush()
    s.flush()  # one-shot


def test_graceful_close_raises_nothing_and_stores_the_record():
    s, mock = stream_for()
    mock.fail_next("graceful-close")
    assert s.ingest_record_offset("g") == 0
    s.flush()
    s.close()
    assert s.stored == ["g"]


def test_stream_abort_retryable_stores_the_record_twice_without_an_error():
    s, mock = stream_for()
    mock.fail_next("stream-abort-retryable", after=1)
    s.ingest_record_offset("once")
    assert s.ingest_record_offset("twice") == 1  # no error: the SDK recovers
    s.flush()
    assert s.stored == ["once", "twice", "twice"]
    assert s.ingested == ["once", "twice"]


def test_schema_change_while_open_raises_nothing_and_routes_rows_to_fallback():
    s, mock = stream_for()
    s.ingest_record_offset("before")
    mock.fail_next("schema-change-while-open")
    s.ingest_record_offset("during")
    s.flush()
    assert s.stored == ["before"]
    assert s.fallback == ["during"]


def test_reset_clears_everything():
    mock = FakeMock()
    mock.fail_next("auth-failure")
    mock.set_scenario("no-acks")
    mock.expire_token(2)
    mock.reset()
    s = FakeZerobusSdk(mock).create_stream("id", "secret", T)
    s.ingest_record_offset("fine")
    s.flush()


def test_unknown_scenario_is_rejected_and_token_expired_cannot_be_held():
    with pytest.raises(ValueError):
        FakeMock().fail_next("no-such")
    with pytest.raises(ValueError):
        FakeMock().set_scenario("token-expired")
