"""The in-memory table store: order, streams, schema changes and the fallback bucket, and coverage."""

from zerobus_mock_server.config import TableConfig
from zerobus_mock_server.schema import Column, make_column
from zerobus_mock_server.tables import TableStore

T = "main.default.t"


def _store():
    cols = (Column("device_name", "STRING", False), Column("temp", "INT"))
    return TableStore([TableConfig(T, cols)])


def test_appends_keep_order_offsets_and_streams():
    store = _store()
    snap = store.snapshot(T)
    store.append(T, snap, [{"device_name": "a", "temp": 1}], protocol="grpc", stream_id="s1", offset=0)
    store.append(T, snap, [{"device_name": "b"}], protocol="rest", stream_id=None)
    recs = store.read(T)
    assert [(r.values["device_name"], r.stream_id, r.offset, r.protocol) for r in recs] == [
        ("a", "s1", 0, "grpc"), ("b", None, None, "rest")]
    assert store.rows(T) == [{"device_name": "a", "temp": 1}, {"device_name": "b", "temp": None}]


def test_a_nullable_column_added_later_reads_null_for_older_rows():
    store = _store()
    old = store.snapshot(T)
    assert store.add_columns(T, [make_column("note", "STRING")])
    store.append(T, old, [{"device_name": "a", "temp": 1}], protocol="grpc", stream_id="s1", offset=0)
    assert store.rows(T) == [{"device_name": "a", "temp": 1, "note": None}]
    assert store.snapshot(T).version == 2


def test_a_non_nullable_column_added_later_sends_older_streams_rows_to_fallback():
    store = _store()
    old = store.snapshot(T)
    store.add_columns(T, [make_column("region", "STRING", nullable=False)])
    store.append(T, old, [{"device_name": "x", "temp": 0}], protocol="grpc", stream_id="s1", offset=0)
    assert store.rows(T) == []
    assert [r["device_name"] for r in store.rows(T, "fallback")] == ["x"]
    store.append(T, store.snapshot(T), [{"device_name": "y", "region": "eu"}], protocol="grpc", stream_id="s2", offset=0)
    assert [r["device_name"] for r in store.rows(T)] == ["y"]


def test_coverage_counts_missing_and_duplicate_keys():
    store = _store()
    snap = store.snapshot(T)
    store.append(T, snap, [{"device_name": f"sensor-{i}"} for i in (0, 1, 1, 3)], protocol="grpc", stream_id="s", offset=0)
    cov = store.coverage(T, expected=4, key="device_name")
    assert cov["missing"] == 1 and cov["missing_keys"] == [2]
    assert cov["duplicate_rows"] == 1 and cov["unexpected_rows"] == 0


def test_reset_clears_rows_and_can_restore_the_configured_schema():
    store = _store()
    store.add_columns(T, [make_column("note", "STRING")])
    store.append(T, store.snapshot(T), [{"device_name": "a"}], protocol="rest", stream_id=None)
    store.reset(schema=True)
    assert store.rows(T) == [] and store.snapshot(T).version == 1
    assert [c.name for c in store.snapshot(T).columns] == ["device_name", "temp"]
