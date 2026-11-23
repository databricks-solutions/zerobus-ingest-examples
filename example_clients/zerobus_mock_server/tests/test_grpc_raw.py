"""gRPC EphemeralStream with the generated stubs (JSON records): offsets, acks, status codes and error details.

These check the wire behaviour directly. The SDK's view is in test_sdk_e2e.py and test_sdk_faults.py.
"""

import json

import grpc
import pytest
from google.rpc import error_details_pb2, status_pb2

from helpers import AQ, DRIFT, get_token
from zerobus_mock_server.pb import zerobus_service_pb2 as pb
from zerobus_mock_server.pb import zerobus_service_pb2_grpc as pb_grpc


@pytest.fixture
def toks(rest):
    return get_token(rest, AQ), get_token(rest, DRIFT)


def _create(table, record_type=pb.JSON):
    return pb.EphemeralStreamRequest(create_stream=pb.CreateIngestStreamRequest(table_name=table, record_type=record_type))


def _json(offset, obj):
    return pb.EphemeralStreamRequest(ingest_record=pb.IngestRecordRequest(offset_id=offset, json_record=json.dumps(obj)))


def _md(tok, table):
    return [("authorization", f"Bearer {tok}"), ("x-databricks-zerobus-table-name", table)]


def _collect(channel, requests, md):
    """Responses received, and the error that ended the stream (None when it ended OK)."""
    stub = pb_grpc.ZerobusStub(channel)
    responses = []
    try:
        for resp in stub.EphemeralStream(iter(requests), metadata=md, timeout=10):
            responses.append(resp)
    except grpc.RpcError as err:
        return responses, err
    return responses, None


def _acks(responses):
    return [r.ingest_record_response.durability_ack_up_to_offset for r in responses
            if r.WhichOneof("payload") == "ingest_record_response"]


def _error_info(err):
    for key, value in err.trailing_metadata() or ():
        if key == "grpc-status-details-bin":
            for detail in status_pb2.Status.FromString(value).details:
                info = error_details_pb2.ErrorInfo()
                if detail.Unpack(info):
                    return info
    return None


def test_json_records_are_acked_in_order_and_stored(channel, fresh, toks):
    responses, err = _collect(channel, [_create(AQ)] + [_json(i, {"device_name": f"d{i}", "temp": i}) for i in range(5)],
                              _md(toks[0], AQ))
    assert err is None
    assert responses[0].create_stream_response.stream_id.startswith("mock-stream-")
    assert _acks(responses) == [0, 1, 2, 3, 4]
    assert [r["device_name"] for r in fresh.tables.rows(AQ)] == [f"d{i}" for i in range(5)]


def test_batch_is_one_submission_with_one_offset(channel, fresh, toks):
    batch = pb.IngestRecordBatchRequest(offset_id=0, json_batch=pb.JsonRecordBatch(
        records=[json.dumps({"device_name": "a"}), json.dumps({"device_name": "b"})]))
    responses, err = _collect(channel, [_create(AQ), pb.EphemeralStreamRequest(ingest_record_batch=batch)],
                              _md(toks[0], AQ))
    assert err is None and _acks(responses) == [0]
    assert [r.offset for r in fresh.tables.read(AQ)] == [0, 0]


def test_wrong_offset_is_invalid_argument(channel, toks):
    _, err = _collect(channel, [_create(AQ), _json(3, {"device_name": "a"})], _md(toks[0], AQ))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "expected offset_id 0, got 3" in err.details()


def test_missing_token_is_unauthenticated_and_another_tables_token_is_permission_denied(channel, fresh, toks):
    _, missing = _collect(channel, [_create(AQ)], [("x-databricks-zerobus-table-name", AQ)])
    assert missing.code() == grpc.StatusCode.UNAUTHENTICATED
    _, wrong = _collect(channel, [_create(AQ)], _md(toks[1], AQ))  # a DRIFT token on an AQ stream
    assert wrong.code() == grpc.StatusCode.PERMISSION_DENIED


def test_unknown_table_and_header_mismatch_are_refused(channel, toks):
    _, unknown = _collect(channel, [_create("main.default.nope")], _md(toks[0], "main.default.nope"))
    assert unknown.code() == grpc.StatusCode.NOT_FOUND
    _, mismatch = _collect(channel, [_create(AQ)], _md(toks[0], DRIFT))
    assert mismatch.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_first_message_must_be_create(channel, toks):
    _, err = _collect(channel, [_json(0, {"device_name": "a"})], _md(toks[0], AQ))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_record_with_an_unknown_field_stops_the_stream_after_the_earlier_records(channel, fresh, toks):
    requests = [_create(AQ), _json(0, {"device_name": "a"}), _json(1, {"device_name": "b"}),
                _json(2, {"device_name": "c", "nope": 1}), _json(3, {"device_name": "d"})]
    responses, err = _collect(channel, requests, _md(toks[0], AQ))
    assert err is not None and err.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert _acks(responses) == [0, 1]
    info = _error_info(err)
    assert info is not None and info.reason == "SCHEMA_VALIDATION_FAILED"
    assert info.metadata["causes"] == "FIELD_NOT_IN_TABLE"
    assert [r["device_name"] for r in fresh.tables.rows(AQ)] == ["a", "b"]
