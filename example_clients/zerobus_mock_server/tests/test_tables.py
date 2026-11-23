"""In-memory table store: order, reads, stats, and schema-change routing (unit tests)."""

from zerobus_mock_server.schema import make_column
from zerobus_mock_server.tables import TableConfig, TableStore


def store():
    return TableStore([TableConfig("main.default.t", (
        make_column("device_name", "STRING", False),
        make_column("temp", "INT"),
    ))])


def test_appends_keep_order_and_offsets():
    s = store()
    snap = s.snapshot("main.default.t")
    s.append("main.default.t", snap, [{"device_name": "a", "temp": 1}, {"device_name": "b", "temp": 2}],
             protocol="grpc", stream_id="mock-stream-1", offsets=[0, 0])
    s.append("main.default.t", snap, [{"device_name": "c", "temp": 3}],
             protocol="grpc", stream_id="mock-stream-1", offsets=[1])
    recs = s.read("main.default.t")
    assert [r.values["device_name"] for r in recs] == ["a", "b", "c"]
    assert [r.offset for r in recs] == [0, 0, 1]
    assert [r.seq for r in recs] == sorted(r.seq for r in recs)


def test_reads_filter_by_stream_and_offset():
    s = store()
    snap = s.snapshot("main.default.t")
    s.append("main.default.t", snap, [{"device_name": "a"}], protocol="grpc", stream_id="s1", offsets=[0])
    s.append("main.default.t", snap, [{"device_name": "b"}], protocol="grpc", stream_id="s2", offsets=[0])
    s.append("main.default.t", snap, [{"device_name": "c"}], protocol="rest", stream_id=None)
    assert [r.values["device_name"] for r in s.read("main.default.t", stream_id="s2")] == ["b"]
    assert [r.values["device_name"] for r in s.read("main.default.t", offset_min=1, offset_max=1)] == []
    assert s.read("main.default.t", since_seq=1)[0].values["device_name"] == "b"


def test_stats_count_duplicates_by_key():
    s = store()
    snap = s.snapshot("main.default.t")
    s.append("main.default.t", snap, [{"device_name": "a"}, {"device_name": "a"}, {"device_name": "b"}],
             protocol="grpc", stream_id="s1", offsets=[0, 0, 1])
    st = s.stats("main.default.t", key="device_name")
    assert st["rows"] == 3
    assert st["distinct_keys"] == 2
    assert st["duplicate_rows"] == 1
    assert st["duplicate_keys"] == ["a"]
    assert st["rows_by_stream"] == {"s1": 3}


def test_nullable_column_added_later_reads_null_for_old_streams():
    s = store()
    old = s.snapshot("main.default.t")
    s.add_columns("main.default.t", [make_column("note", "STRING", True)])
    s.append("main.default.t", old, [{"device_name": "a", "temp": 1}], protocol="grpc", stream_id="old", offsets=[0])
    rec = s.read("main.default.t")[0]
    assert rec.destination == "table"
    assert rec.values == {"device_name": "a", "temp": 1, "note": None}


def test_non_nullable_column_added_later_sends_old_stream_rows_to_fallback():
    s = store()
    old = s.snapshot("main.default.t")
    s.add_columns("main.default.t", [make_column("region", "STRING", False)])
    s.append("main.default.t", old, [{"device_name": "a", "temp": 1}], protocol="grpc", stream_id="old", offsets=[0])
    assert s.read("main.default.t") == []
    fb = s.read("main.default.t", destination="fallback")
    assert len(fb) == 1 and "region" in fb[0].reason
    new = s.snapshot("main.default.t")
    s.append("main.default.t", new, [{"device_name": "b", "temp": 2, "region": "eu"}],
             protocol="grpc", stream_id="new", offsets=[0])
    assert [r.values["device_name"] for r in s.read("main.default.t")] == ["b"]


def test_reset_clears_rows_and_can_restore_schema():
    s = store()
    snap = s.snapshot("main.default.t")
    s.append("main.default.t", snap, [{"device_name": "a"}], protocol="rest", stream_id=None)
    s.add_columns("main.default.t", [make_column("note", "STRING", True)])
    s.reset(schema=True)
    assert s.read("main.default.t", destination="all") == []
    assert s.snapshot("main.default.t").version == 1


def test_python_handle_exposes_rows_and_fallback():
    s = store()
    snap = s.snapshot("main.default.t")
    s.append("main.default.t", snap, [{"device_name": "a", "temp": 1}], protocol="grpc", stream_id="s1", offsets=[0])
    h = s["main.default.t"]
    assert h.rows == [{"device_name": "a", "temp": 1}]
    assert len(h) == 1 and h.records[0].stream_id == "s1"
    assert h.stats()["rows"] == 1
    h.reset()
    assert h.rows == []
