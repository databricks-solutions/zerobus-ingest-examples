"""The real Python SDK against each named scenario: what it raises, and what landed.

The text an error must contain is the scenario's ``message`` in scenarios.json, so this file
and the mock libraries check the same contract. Retryable errors go through the SDK's own
recovery; the others surface from create_stream, ingest, flush or close.
"""

import pytest
from zerobus.sdk.shared import StreamConfigurationOptions

from helpers import AQ, DRIFT
from sdk_helpers import contract, create, create_error, drive
from zerobus_mock_server.schema import make_column


def recs(n, prefix="sensor"):
    return [{"device_name": f"{prefix}-{i}", "temp": i} for i in range(n)]


def names(mock, table=AQ, destination="table"):
    return [r["device_name"] for r in mock.tables.rows(table, destination)]


def test_auth_failure_raises_from_create(mock):
    mock.scenarios.fail_next("auth-failure")
    assert contract("auth-failure") in str(create_error(mock, AQ))


def test_table_not_found_raises_from_create(mock):
    mock.scenarios.fail_next("table-not-found")
    assert contract("table-not-found") in str(create_error(mock, AQ))


def test_schema_mismatch_surfaces_and_earlier_records_land(mock):
    mock.scenarios.fail_next("schema-mismatch", after=3)
    exc = drive(create(mock, AQ), recs(6, "m"))
    assert contract("schema-mismatch") in str(exc)
    assert names(mock) == ["m-0", "m-1", "m-2"]


def test_record_too_large_is_terminal(mock):
    mock.scenarios.fail_next("record-too-large", after=1)
    exc = drive(create(mock, AQ), recs(4, "big"))
    assert contract("record-too-large") in str(exc)
    assert len(mock.tables.rows(AQ)) == 1


def test_server_unavailable_is_retried_then_raises_and_clears(mock):
    mock.scenarios.set_scenario("server-unavailable")
    opts = StreamConfigurationOptions(recovery_retries=0)
    assert contract("server-unavailable") in str(create_error(mock, AQ, options=opts))
    mock.scenarios.clear_scenario()
    assert drive(create(mock, AQ), recs(1, "back")) is None


def test_retryable_abort_is_recovered_with_duplicates(mock):
    # The 5th submission is stored without an ack and the stream ends UNAVAILABLE. The SDK
    # reconnects and resends, so the row can appear twice (at-least-once).
    mock.scenarios.fail_next("stream-abort-retryable", after=4)
    assert drive(create(mock, AQ), recs(10, "r")) is None
    cov = mock.tables.coverage(AQ, expected=10, key="device_name")
    assert cov["missing"] == 0 and cov["duplicate_rows"] >= 1


def test_terminal_abort_is_an_sdk_error_and_only_acked_rows_are_kept(mock):
    mock.scenarios.fail_next("stream-abort-fatal", after=4)
    exc = drive(create(mock, AQ), recs(10, "f"))
    assert contract("stream-abort-fatal") in str(exc)
    assert names(mock) == [f"f-{i}" for i in range(4)]


def test_graceful_close_stores_and_acks_in_the_window_then_resumes(mock):
    mock.scenarios.fail_next("graceful-close", after=3)
    assert drive(create(mock, AQ), recs(10, "g")) is None
    cov = mock.tables.coverage(AQ, expected=10, key="device_name")
    assert cov["missing"] == 0 and cov["duplicate_rows"] == 0


def test_no_acks_makes_flush_time_out_with_the_rows_stored(mock):
    mock.scenarios.set_scenario("no-acks")
    stream = create(mock, AQ, options=StreamConfigurationOptions(flush_timeout_ms=1000))
    for rec in recs(5, "na"):
        stream.ingest_record_offset(rec)
    with pytest.raises(Exception) as info:
        stream.flush()
    assert contract("no-acks") in str(info.value)
    assert len(names(mock)) == 5  # stored, never acknowledged


def test_schema_change_while_open_sends_the_open_streams_rows_to_fallback(mock):
    mock.scenarios.fail_next("schema-change-while-open")
    assert drive(create(mock, AQ), recs(3, "sc")) is None
    assert names(mock) == []
    assert names(mock, destination="fallback") == ["sc-0", "sc-1", "sc-2"]


def test_token_expired_mid_stream_is_terminal_then_a_new_stream_works(mock):
    stream = create(mock, AQ)
    stream.ingest_record_offset({"device_name": "before", "temp": 1})
    stream.flush()
    mock.tokens.expire_next(1)  # the stream's next message finds its token expired
    exc = drive(stream, recs(2, "after"))
    assert contract("token-expired") in str(exc)
    assert drive(create(mock, AQ), recs(1, "fresh")) is None
    assert names(mock) == ["before", "fresh-0"]


def test_token_expired_at_create_is_refreshed_by_the_sdk(mock):
    mock.tokens.expire_next(1)  # the create's check fails once; the SDK mints a new token and retries
    assert drive(create(mock, AQ), recs(2, "refresh")) is None
    assert mock.tables.coverage(AQ, expected=2, key="device_name")["missing"] == 0


def test_stream_keeps_its_schema_until_it_is_reopened(mock):
    stream_a = create(mock, DRIFT)  # opened before the table gains a nullable column
    mock.tables.add_columns(DRIFT, [make_column("note", "STRING")])
    exc = drive(stream_a, [{"device_name": "old", "temp": 1, "note": "hi"}])
    assert exc is not None and "is not a column" in str(exc)
    assert names(mock, DRIFT) == []
    assert drive(create(mock, DRIFT), [{"device_name": "new", "temp": 2, "note": "hi"}]) is None
    assert mock.tables.rows(DRIFT) == [{"device_name": "new", "temp": 2, "note": "hi"}]
