"""The real Python SDK against each injected fault: what the SDK sees, and what landed.

Retriable errors go through the SDK's own recovery. Non-retriable errors surface
as ZerobusException from ingest, flush or close.
"""

import time

from helpers import TABLES
from sdk_helpers import create, drive, run_in_thread

AQ = TABLES["air_quality"]
DRIFT = TABLES["drift"]


def recs(n, prefix="sensor"):
    return [{"device_name": f"{prefix}-{i}", "temp": i} for i in range(n)]


def add(mock, **spec):
    return mock.add_fault(spec)


def fired(mock):
    return [e["fault_type"] for e in mock.faults.events()]


def test_token_fault_is_a_non_retriable_sdk_error(mock, fresh):
    add(mock, type="token_invalid_client", table=AQ)
    try:
        create(mock, AQ)
        raise AssertionError("create_stream should fail")
    except Exception as exc:  # noqa: BLE001
        assert "invalid_client" in str(exc)
    assert fired(mock) == ["token_invalid_client"]


def test_grpc_unauthenticated_at_create_is_not_retried(mock, fresh):
    add(mock, type="unauthorized", protocol="grpc", table=AQ, trigger={"mode": "always"})
    started = time.monotonic()
    try:
        create(mock, AQ)
        raise AssertionError("create_stream should fail")
    except AssertionError:
        raise
    except Exception as exc:  # noqa: BLE001
        assert "injected: token rejected" in str(exc)
    assert time.monotonic() - started < 10  # no retry loop


def test_table_not_found_at_create_is_reported(mock, fresh):
    add(mock, type="table_not_found", protocol="grpc", table=AQ)
    try:
        create(mock, AQ)
        raise AssertionError("create_stream should fail")
    except AssertionError:
        raise
    except Exception as exc:  # noqa: BLE001
        assert "cannot be found" in str(exc)


def test_retriable_unavailable_at_create_is_retried_by_the_sdk(mock, fresh):
    add(mock, type="grpc_status", protocol="grpc", table=AQ, trigger={"mode": "once"},
        params={"code": "UNAVAILABLE", "message": "injected"})
    stream = create(mock, AQ)  # the first attempt fails; the SDK's recovery makes the second succeed
    assert drive(stream, recs(3)) is None
    assert mock.tables.stats(AQ)["rows"] == 3
    assert fired(mock) == ["grpc_status"]


def test_schema_mismatch_on_a_record_surfaces_and_earlier_records_land(mock, fresh):
    add(mock, type="schema_mismatch", protocol="grpc", table=AQ, trigger={"mode": "after", "after": 4, "times": 1})
    exc = drive(create(mock, AQ), recs(10))
    assert exc is not None
    assert "does not fit" in str(exc) and "injected" in str(exc)
    rows = mock.tables.read(AQ)
    assert [r.values["device_name"] for r in rows] == [f"sensor-{i}" for i in range(4)]
    assert fired(mock) == ["schema_mismatch"]


def test_record_too_large_on_a_record_is_out_of_range(mock, fresh):
    add(mock, type="record_too_large", protocol="grpc", table=AQ, trigger={"mode": "after", "after": 1, "times": 1})
    exc = drive(create(mock, AQ), recs(4))
    assert exc is not None and "maximum message size" in str(exc)
    assert mock.tables.stats(AQ)["rows"] == 1
    assert fired(mock) == ["record_too_large"]


def test_retryable_mid_flight_abort_is_recovered_with_duplicates(mock, fresh):
    # The 6th submission is written but not acked, and the stream ends with UNAVAILABLE.
    # The SDK reconnects and resends what it hasn't seen acked, so the row appears twice.
    add(mock, type="stream_abort", protocol="grpc", table=AQ,
        trigger={"mode": "after", "after": 5, "times": 1},
        params={"retryable": True, "persist_unacked": True})
    exc = drive(create(mock, AQ), recs(10))
    assert exc is None, f"SDK should recover, got {exc!r}"
    st = mock.tables.stats(AQ, key="device_name")
    assert st["distinct_keys"] == 10
    assert st["rows"] > 10 and st["duplicate_rows"] >= 1  # at-least-once
    assert len({r.stream_id for r in mock.tables.read(AQ, limit=1000)}) == 2  # reconnected
    assert fired(mock) == ["stream_abort"]


def test_terminal_abort_is_an_sdk_error_and_only_acked_rows_are_kept(mock, fresh):
    add(mock, type="stream_abort", protocol="grpc", table=AQ,
        trigger={"mode": "after", "after": 5, "times": 1},
        params={"retryable": False, "persist_unacked": False})
    exc = drive(create(mock, AQ), recs(10))
    assert exc is not None
    assert "injected terminal stream error at offset 5" in str(exc)
    rows = mock.tables.read(AQ)
    assert [r.values["device_name"] for r in rows] == [f"sensor-{i}" for i in range(5)]


def test_close_stream_signal_ends_the_stream_and_the_sdk_resumes(mock, fresh):
    add(mock, type="close_stream_signal", protocol="grpc", table=AQ,
        trigger={"mode": "after", "after": 3, "times": 1}, params={"duration_ms": 300})
    exc = drive(create(mock, AQ), recs(10))
    assert exc is None
    st = mock.tables.stats(AQ, key="device_name")
    assert st["distinct_keys"] == 10
    assert fired(mock) == ["close_stream_signal"]
    assert any(e["action"] == "CloseStreamSignal" for e in mock.faults.events())


def test_dropped_acks_hold_flush_until_an_ack_arrives(mock, fresh):
    add(mock, type="drop_acks", protocol="grpc", table=AQ, params={"count": -1})
    stream = create(mock, AQ)
    for rec in recs(5):
        stream.ingest_record_offset(rec)
    flusher, box = run_in_thread(stream.flush)
    flusher.join(3)
    assert flusher.is_alive(), "flush should still be waiting for an ack"
    assert mock.tables.stats(AQ)["rows"] == 5  # the records are stored, only the acks are missing
    mock.clear_faults()  # the next ack covers everything before it
    stream.ingest_record_offset({"device_name": "sensor-5", "temp": 5})
    flusher.join(10)
    assert not flusher.is_alive() and "exc" not in box
    stream.close()
    assert mock.tables.stats(AQ)["rows"] == 6
    assert "ack dropped" in [e["action"] for e in mock.faults.events()]


def test_slow_ack_delays_flush(mock, fresh):
    add(mock, type="slow_ack", protocol="grpc", table=AQ, params={"delay_ms": 400})
    stream = create(mock, AQ)
    started = time.monotonic()
    assert drive(stream, recs(3)) is None
    assert time.monotonic() - started >= 0.4


def test_open_stream_keeps_its_schema_until_it_reopens(mock, fresh):
    # Stream A is opened before the table gains a nullable column.
    stream_a = create(mock, DRIFT)
    mock.state.apply_schema_change(DRIFT, [{"name": "note", "type": "STRING", "nullable": True}])
    exc = drive(stream_a, [{"device_name": "old", "temp": 1, "note": "hi"}])
    assert exc is not None and "FIELD_NOT_IN_TABLE" in str(exc)
    assert mock.tables.stats(DRIFT)["rows"] == 0

    # Stream B is opened after the change and sees the new column.
    stream_b = create(mock, DRIFT)
    assert drive(stream_b, [{"device_name": "new", "temp": 2, "note": "hi"}]) is None
    rows = mock.tables.values(DRIFT)
    assert rows == [{"device_name": "new", "temp": 2, "note": "hi"}]


def test_breaking_change_sends_an_open_streams_rows_to_fallback(mock, fresh):
    stream_a = create(mock, DRIFT)
    assert drive(stream_a, [{"device_name": "x", "temp": 0}], close=False) is None
    mock.state.apply_schema_change(DRIFT, [{"name": "region", "type": "STRING", "nullable": False}])
    stream_b = create(mock, DRIFT)
    exc = drive(stream_a, [{"device_name": "y", "temp": 1}, {"device_name": "z", "temp": 2}])
    assert exc is None
    assert [r.values["device_name"] for r in mock.tables.read(DRIFT)] == ["x"]
    fb = mock.tables.read(DRIFT, destination="fallback")
    assert {r.values["device_name"] for r in fb} == {"y", "z"}
    assert drive(stream_b, [{"device_name": "w", "temp": 3, "region": "eu"}]) is None
    assert "w" in [r.values["device_name"] for r in mock.tables.read(DRIFT)]
