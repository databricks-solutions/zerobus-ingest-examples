"""gRPC EphemeralStream against a live mock, using the generated stubs directly.

These tests assert status codes and wire details. Behaviour seen through the real
SDK is covered in test_sdk_e2e.py and test_sdk_faults.py.
"""

import json

import grpc
import httpx
import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.rpc import error_details_pb2, status_pb2

from helpers import TABLES, ca_context, get_token
from zerobus_mock_server.pb import zerobus_service_pb2 as pb
from zerobus_mock_server.pb import zerobus_service_pb2_grpc as pb_grpc

AQ = TABLES["air_quality"]
PR = TABLES["proto_readings"]
FD = descriptor_pb2.FieldDescriptorProto


@pytest.fixture
def channel(mock):
    creds = grpc.ssl_channel_credentials(root_certificates=mock.ca_path.read_bytes())
    ch = grpc.secure_channel(f"localhost:{mock.cfg.server.grpc_port}", creds)
    yield ch
    ch.close()


@pytest.fixture
def token(mock, fresh):
    with httpx.Client(base_url=mock.rest_base_url, verify=ca_context(mock.ca_path)) as c:
        return get_token(c, AQ), get_token(c, TABLES["sensor_events"])


@pytest.fixture
def pr_tok(mock, fresh):
    with httpx.Client(base_url=mock.rest_base_url, verify=ca_context(mock.ca_path)) as c:
        return get_token(c, PR)


def metadata(tok, table):
    return [("authorization", f"Bearer {tok}"), ("x-databricks-zerobus-table-name", table)]


def create(table, record_type=pb.JSON, descriptor=None):
    kwargs = {"table_name": table, "record_type": record_type}
    if descriptor is not None:
        kwargs["descriptor_proto"] = descriptor
    return pb.EphemeralStreamRequest(create_stream=pb.CreateIngestStreamRequest(**kwargs))


def json_ingest(offset, obj):
    return pb.EphemeralStreamRequest(ingest_record=pb.IngestRecordRequest(offset_id=offset, json_record=json.dumps(obj)))


def run(channel, requests, md, timeout=10):
    stub = pb_grpc.ZerobusStub(channel)
    return list(stub.EphemeralStream(iter(requests), metadata=md, timeout=timeout))


def status_of(channel, requests, md):
    with pytest.raises(grpc.RpcError) as e:
        run(channel, requests, md)
    return e.value


def acks(responses):
    return [r.ingest_record_response.durability_ack_up_to_offset for r in responses if r.WhichOneof("payload") == "ingest_record_response"]


def test_create_then_json_records_are_acked_in_order(channel, mock, fresh, token):
    tok, _ = token
    reqs = [create(AQ)] + [json_ingest(i, {"device_name": f"d{i}", "temp": i}) for i in range(5)]
    responses = run(channel, reqs, metadata(tok, AQ))
    assert responses[0].WhichOneof("payload") == "create_stream_response"
    assert responses[0].create_stream_response.stream_id.startswith("mock-stream-")
    assert acks(responses)[-1] == 4
    rows = fresh.tables.read(AQ)
    assert [r.values["device_name"] for r in rows] == [f"d{i}" for i in range(5)]
    assert {r.offset for r in rows} == set(range(5))


def test_batch_is_one_submission_with_one_offset(channel, fresh, token):
    tok, _ = token
    batch = pb.IngestRecordBatchRequest(offset_id=0, json_batch=pb.JsonRecordBatch(
        records=[json.dumps({"device_name": "a"}), json.dumps({"device_name": "b"})]))
    responses = run(channel, [create(AQ), pb.EphemeralStreamRequest(ingest_record_batch=batch)], metadata(tok, AQ))
    assert acks(responses) == [0]
    assert [r.offset for r in fresh.tables.read(AQ)] == [0, 0]


def test_wrong_offset_is_invalid_argument(channel, token):
    tok, _ = token
    err = status_of(channel, [create(AQ), json_ingest(3, {"device_name": "a"})], metadata(tok, AQ))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "expected offset_id 0, got 3" in err.details()


def test_missing_token_is_unauthenticated(channel, fresh):
    err = status_of(channel, [create(AQ)], [("x-databricks-zerobus-table-name", AQ)])
    assert err.code() == grpc.StatusCode.UNAUTHENTICATED


def test_token_for_another_table_is_permission_denied(channel, token):
    tok_aq, tok_se = token
    err = status_of(channel, [create(AQ)], metadata(tok_se, AQ))
    assert err.code() == grpc.StatusCode.PERMISSION_DENIED


def test_unknown_table_is_not_found(channel, token):
    err = status_of(channel, [create("main.default.nope")], metadata(token[0], "main.default.nope"))
    assert err.code() == grpc.StatusCode.NOT_FOUND


def test_header_and_create_table_must_agree(channel, token):
    err = status_of(channel, [create(AQ)], metadata(token[0], TABLES["sensor_events"]))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_first_message_must_be_create(channel, token):
    err = status_of(channel, [json_ingest(0, {"device_name": "a"})], metadata(token[0], AQ))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT


def error_info(err):
    for key, value in err.trailing_metadata() or ():
        if key == "grpc-status-details-bin":
            st = status_pb2.Status.FromString(value)
            for detail in st.details:
                info = error_details_pb2.ErrorInfo()
                if detail.Unpack(info):
                    return info
    return None


def test_schema_failure_carries_error_info_with_causes(channel, token):
    tok, _ = token
    err = status_of(channel, [create(AQ), json_ingest(0, {"temp": 1})], metadata(tok, AQ))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT
    info = error_info(err)
    assert info is not None
    assert info.reason == "SCHEMA_VALIDATION_FAILED" and info.domain == "zerobus.databricks.com"
    assert "MISSING_REQUIRED_COLUMN" in info.metadata["causes"]


def test_unknown_field_on_plain_table_carries_error_code_4044(channel, token):
    tok, _ = token
    err = status_of(channel, [create(AQ), json_ingest(0, {"device_name": "a", "region": "us"})], metadata(tok, AQ))
    info = error_info(err)
    assert info.metadata["causes"] == "FIELD_NOT_IN_TABLE"
    assert info.metadata["error_code"] == "4044"


def test_records_before_a_bad_one_are_stored_and_acked(channel, fresh, token):
    tok, _ = token
    reqs = [create(AQ), json_ingest(0, {"device_name": "a"}), json_ingest(1, {"device_name": "b"}),
            json_ingest(2, {"temp": 5})]
    stub = pb_grpc.ZerobusStub(channel)
    received = []
    with pytest.raises(grpc.RpcError) as e:
        for resp in stub.EphemeralStream(iter(reqs), metadata=metadata(tok, AQ), timeout=10):
            received.append(resp)
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert [r.offset for r in fresh.tables.read(AQ)] == [0, 1]
    assert acks(received)[-1] == 1


# ---------------------------------------------------------------- protobuf streams

def reading_descriptor(fields=None):
    """A file with one self-contained message (its Location type is nested, as the SDK requires)."""
    fdp = descriptor_pb2.FileDescriptorProto(name="readings.proto", package="zbtest", syntax="proto2")
    msg = fdp.message_type.add(name="Reading")
    loc = msg.nested_type.add(name="Location")
    loc.field.add(name="lat", number=1, type=FD.TYPE_DOUBLE, label=FD.LABEL_OPTIONAL)
    loc.field.add(name="lon", number=2, type=FD.TYPE_DOUBLE, label=FD.LABEL_OPTIONAL)
    msg.field.add(name="device_name", number=1, type=FD.TYPE_STRING, label=FD.LABEL_OPTIONAL)
    msg.field.add(name="temp", number=2, type=FD.TYPE_INT32, label=FD.LABEL_OPTIONAL)
    msg.field.add(name="humidity", number=3, type=FD.TYPE_INT64, label=FD.LABEL_OPTIONAL)
    msg.field.add(name="tags", number=4, type=FD.TYPE_STRING, label=FD.LABEL_REPEATED)
    msg.field.add(name="location", number=5, type=FD.TYPE_MESSAGE, label=FD.LABEL_OPTIONAL, type_name=".zbtest.Reading.Location")
    if fields is not None:
        del msg.field[:]
        for f in fields:
            msg.field.add(**f)
    return fdp


def message_bytes(fdp):
    """What the SDK sends as descriptor_proto: the serialized message descriptor."""
    return fdp.message_type[0].SerializeToString()


def reading_class(fdp):
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("zbtest.Reading"))


def test_proto_stream_stores_typed_records(channel, fresh, pr_tok):
    fdp = reading_descriptor()
    cls = reading_class(fdp)
    reqs = [create(PR, record_type=pb.PROTO, descriptor=message_bytes(fdp))]
    for i in range(3):
        msg = cls(device_name=f"p{i}", temp=20 + i, humidity=40, tags=["a", "b"])
        msg.location.lat = 51.5
        msg.location.lon = -0.12
        reqs.append(pb.EphemeralStreamRequest(ingest_record=pb.IngestRecordRequest(offset_id=i, proto_encoded_record=msg.SerializeToString())))
    responses = run(channel, reqs, metadata(pr_tok, PR))
    assert acks(responses)[-1] == 2
    rows = fresh.tables.values(PR)
    assert [r["device_name"] for r in rows] == ["p0", "p1", "p2"]
    assert rows[0]["tags"] == ["a", "b"]
    assert rows[0]["location"] == {"lat": 51.5, "lon": -0.12}


def test_proto_descriptor_with_unknown_field_is_refused_at_create(channel, pr_tok):
    fdp = reading_descriptor(fields=[
        dict(name="device_name", number=1, type=FD.TYPE_STRING, label=FD.LABEL_OPTIONAL),
        dict(name="region", number=2, type=FD.TYPE_STRING, label=FD.LABEL_OPTIONAL),
    ])
    err = status_of(channel, [create(PR, record_type=pb.PROTO, descriptor=message_bytes(fdp))], metadata(pr_tok, PR))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "FIELD_NOT_IN_TABLE" in error_info(err).metadata["causes"]


def test_proto_descriptor_with_wrong_type_is_refused_at_create(channel, pr_tok):
    fdp = reading_descriptor(fields=[
        dict(name="device_name", number=1, type=FD.TYPE_INT32, label=FD.LABEL_OPTIONAL),  # STRING column
    ])
    err = status_of(channel, [create(PR, record_type=pb.PROTO, descriptor=message_bytes(fdp))], metadata(pr_tok, PR))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "TYPE_INCOMPATIBLE" in error_info(err).metadata["causes"]


def test_proto_descriptor_missing_a_required_column_is_refused(channel, pr_tok):
    fdp = reading_descriptor(fields=[dict(name="temp", number=2, type=FD.TYPE_INT32, label=FD.LABEL_OPTIONAL)])
    err = status_of(channel, [create(PR, record_type=pb.PROTO, descriptor=message_bytes(fdp))], metadata(pr_tok, PR))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "MISSING_REQUIRED_COLUMN" in error_info(err).metadata["causes"]


def test_proto_stream_without_descriptor_is_refused(channel, pr_tok):
    err = status_of(channel, [create(PR, record_type=pb.PROTO)], metadata(pr_tok, PR))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_file_form_descriptor_is_accepted_as_well(channel, pr_tok, mock):
    fdp = reading_descriptor()
    cls = reading_class(fdp)
    msg = cls(device_name="f", temp=1)
    reqs = [create(PR, record_type=pb.PROTO, descriptor=fdp.SerializeToString()),
            pb.EphemeralStreamRequest(ingest_record=pb.IngestRecordRequest(offset_id=0, proto_encoded_record=msg.SerializeToString()))]
    assert acks(run(channel, reqs, metadata(pr_tok, PR)))[-1] == 0
