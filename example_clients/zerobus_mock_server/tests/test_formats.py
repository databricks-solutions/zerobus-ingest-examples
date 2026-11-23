"""Avro records, Arrow Flight DoPut, and PersistentStream against a live mock (raw clients).

Arrow is driven through pyarrow's own Flight client over TLS (the same DoPut call the
SDKs make, with the table and token in the headers). Avro and persistent streams use the
generated gRPC stubs directly. The real SDKs are covered in test_sdk_formats.py.
"""

import datetime as dt
import io
import json
from decimal import Decimal

import fastavro
import grpc
import httpx
import pyarrow as pa
import pyarrow.flight as flight
import pytest

from helpers import TABLES, ca_bytes, ca_context, get_token
from zerobus_mock_server.pb import zerobus_service_pb2 as pb
from zerobus_mock_server.pb import zerobus_service_pb2_grpc as pb_grpc
from google.rpc import error_details_pb2

AQ = TABLES["air_quality"]
STRICT = TABLES["strict"]
PR_TABLE = TABLES["proto_readings"]

AVRO_AQ = {"type": "record", "name": "AirQuality", "fields": [
    {"name": "device_name", "type": "string"},
    {"name": "temp", "type": ["null", "int"], "default": None},
    {"name": "humidity", "type": ["null", "long"], "default": None}]}


@pytest.fixture
def channel(mock):
    creds = grpc.ssl_channel_credentials(root_certificates=mock.ca_path.read_bytes())
    ch = grpc.secure_channel(f"localhost:{mock.cfg.server.grpc_port}", creds)
    yield ch
    ch.close()


@pytest.fixture
def toks(mock, fresh):
    with httpx.Client(base_url=mock.rest_base_url, verify=ca_context(mock.ca_path)) as c:
        out = {t: get_token(c, TABLES[t]) for t in ("air_quality", "strict", "proto_readings")}
        out["aq"] = out["air_quality"]
        return out


def md(tok, table):
    return [("authorization", f"Bearer {tok}"), ("x-databricks-zerobus-table-name", table)]


def acks(responses):
    return [r.ingest_record_response.durability_ack_up_to_offset for r in responses
            if r.WhichOneof("payload") == "ingest_record_response"]


def avro_create(table, schema):
    return pb.EphemeralStreamRequest(create_stream=pb.CreateIngestStreamRequest(
        table_name=table, record_type=pb.AVRO, avro_schema_json=json.dumps(schema)))


def avro_record(offset, datum):
    return pb.EphemeralStreamRequest(ingest_record=pb.IngestRecordRequest(offset_id=offset, avro_encoded_record=datum))


def datum(schema, obj):
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(schema), obj)
    return buf.getvalue()


def run(channel, requests, metadata, timeout=10):
    stub = pb_grpc.ZerobusStub(channel)
    return list(stub.EphemeralStream(iter(requests), metadata=metadata, timeout=timeout))


def failure(channel, requests, metadata):
    with pytest.raises(grpc.RpcError) as e:
        run(channel, requests, metadata)
    return e.value


def error_info(err):
    for key, value in err.trailing_metadata() or ():
        if key == "grpc-status-details-bin":
            from google.rpc import status_pb2
            st = status_pb2.Status.FromString(value)
            for d in st.details:
                info = error_details_pb2.ErrorInfo()
                if d.Unpack(info):
                    return info
    return None


# ---------------------------------------------------------------- Avro

def test_avro_datums_and_batch_are_stored_with_the_writer_schema(channel, fresh, toks):
    reqs = [avro_create(AQ, AVRO_AQ)]
    reqs += [avro_record(i, datum(AVRO_AQ, {"device_name": f"v-{i}", "temp": i, "humidity": 9})) for i in range(3)]
    batch = pb.IngestRecordBatchRequest(offset_id=3, avro_batch=pb.AvroRecordBatch(records=[
        datum(AVRO_AQ, {"device_name": "b-0", "temp": None, "humidity": 1}),
        datum(AVRO_AQ, {"device_name": "b-1", "temp": 5, "humidity": None})]))
    reqs.append(pb.EphemeralStreamRequest(ingest_record_batch=batch))
    responses = run(channel, reqs, md(toks["aq"], AQ))
    assert acks(responses)[-1] == 3
    rows = fresh.tables.read(AQ)
    assert [r.values["device_name"] for r in rows] == ["v-0", "v-1", "v-2", "b-0", "b-1"]
    assert rows[0].protocol == "grpc" and rows[3].offset == 3 and rows[4].offset == 3
    assert rows[3].values["temp"] is None


def test_avro_logical_types_are_converted_to_the_table_types(channel, fresh, toks):
    schema = {"type": "record", "name": "Strict", "fields": [
        {"name": "id", "type": "long"},
        {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
        {"name": "created", "type": {"type": "int", "logicalType": "date"}},
        {"name": "updated", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "tags", "type": {"type": "map", "values": "int"}},
        {"name": "blob", "type": "bytes"},
        {"name": "flag", "type": "boolean"}]}
    obj = {"id": 7, "amount": Decimal("12.5"), "created": dt.date(2024, 2, 29),
           "updated": dt.datetime(2024, 2, 29, 12, 0, tzinfo=dt.timezone.utc), "tags": {"a": 1},
           "blob": b"\x00\x01", "flag": True}
    reqs = [avro_create(STRICT, schema), avro_record(0, datum(schema, obj))]
    assert acks(run(channel, reqs, md(toks["strict"], STRICT)))[-1] == 0
    row = fresh.tables.values(STRICT)[0]
    assert row["amount"] == Decimal("12.50")
    assert row["created"] == dt.date(2024, 2, 29)
    assert row["tags"] == {"a": 1} and row["blob"] == b"\x00\x01" and row["flag"] is True


@pytest.mark.parametrize("payload, phrase", [
    (b"Obj\x01\x04\x00", "container file"),
    (b"\x00\x00\x00\x01" + datum(AVRO_AQ, {"device_name": "f", "temp": 1, "humidity": 1}), "framed"),
])
def test_avro_container_and_framed_payloads_are_rejected(channel, fresh, toks, payload, phrase):
    err = failure(channel, [avro_create(AQ, AVRO_AQ), avro_record(0, payload)], md(toks["aq"], AQ))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert phrase in err.details()
    assert fresh.tables.stats(AQ)["rows"] == 0


def test_avro_trailing_bytes_are_rejected(channel, fresh, toks):
    good = datum(AVRO_AQ, {"device_name": "x", "temp": 1, "humidity": 2})
    err = failure(channel, [avro_create(AQ, AVRO_AQ), avro_record(0, good + b"\x01")], md(toks["aq"], AQ))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT and "trailing bytes" in err.details()


def test_avro_named_type_references_are_refused_at_create(channel, toks):
    schema = {"type": "record", "name": "R", "fields": [
        {"name": "device_name", "type": {"type": "enum", "name": "Color", "symbols": ["red"]}},
        {"name": "temp", "type": "Color"}]}
    err = failure(channel, [avro_create(AQ, schema)], md(toks["aq"], AQ))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT and "named type reference" in err.details()


def test_avro_schema_field_not_in_table_carries_error_info(channel, toks):
    schema = dict(AVRO_AQ, fields=AVRO_AQ["fields"] + [{"name": "region", "type": "string"}])
    err = failure(channel, [avro_create(AQ, schema)], md(toks["aq"], AQ))
    info = error_info(err)
    assert info is not None and "FIELD_NOT_IN_TABLE" in info.metadata["causes"]
    assert info.metadata["error_code"] == "4044"


def test_avro_schema_missing_a_required_column_is_refused(channel, toks):
    schema = {"type": "record", "name": "R", "fields": [{"name": "temp", "type": ["null", "int"]}]}
    err = failure(channel, [avro_create(AQ, schema)], md(toks["aq"], AQ))
    assert "MISSING_REQUIRED_COLUMN" in error_info(err).metadata["causes"]


def test_avro_nullable_field_for_not_null_column_is_refused(channel, toks):
    schema = {"type": "record", "name": "R", "fields": [{"name": "device_name", "type": ["null", "string"]}]}
    err = failure(channel, [avro_create(AQ, schema)], md(toks["aq"], AQ))
    assert "TYPE_INCOMPATIBLE" in error_info(err).metadata["causes"]


def test_avro_schema_with_a_bad_value_stops_the_stream_after_earlier_records(channel, fresh, toks):
    reqs = [avro_create(AQ, AVRO_AQ), avro_record(0, datum(AVRO_AQ, {"device_name": "ok", "temp": 1, "humidity": 1})),
            avro_record(1, b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff")]
    err = failure(channel, reqs, md(toks["aq"], AQ))
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert [r.offset for r in fresh.tables.read(AQ)] == [0]


# ---------------------------------------------------------------- Arrow Flight (pyarrow client)

ARROW_SCHEMA = pa.schema([pa.field("device_name", pa.string(), nullable=False),
                          pa.field("temp", pa.int32()), pa.field("humidity", pa.int64())])


@pytest.fixture
def flight_client(mock):
    client = flight.connect(f"grpc+tls://localhost:{mock.cfg.server.grpc_port}",
                            tls_root_certs=ca_bytes(mock.ca_path))
    yield client
    client.close()


def put_arrow(client, tok, table, schema, batches, *, metadata_for=None):
    """Drive DoPut like the SDKs do. A refused stream surfaces at close() (pyarrow), so it is re-raised there."""
    opts = flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {tok}".encode()),
                                             (b"x-databricks-zerobus-table-name", table.encode())])
    writer, reader = client.do_put(flight.FlightDescriptor.for_path(table), schema, opts)
    results = []

    def read_result():
        buf = reader.read()
        if buf is None:
            writer.close()  # raises the server's status (pyarrow surfaces it here)
            raise AssertionError("the server ended the stream without a result")
        return json.loads(buf.to_pybytes())

    results.append(read_result())  # ready signal (or a setup error)
    for n, b in enumerate(batches):
        meta = metadata_for(n) if metadata_for else json.dumps({"offset_id": n}).encode()
        writer.write_with_metadata(b, meta)
        results.append(read_result())
    writer.done_writing()
    writer.close()
    return results


def batch(n, prefix="a"):
    return pa.record_batch({"device_name": [f"{prefix}-{i}" for i in range(n)],
                            "temp": list(range(n)), "humidity": [1] * n}, schema=ARROW_SCHEMA)


def test_arrow_flight_batches_are_acked_and_stored(flight_client, mock, fresh, toks):
    results = put_arrow(flight_client, toks["aq"], AQ, ARROW_SCHEMA, [batch(100, "x"), batch(50, "y")])
    assert results[0] == {"ack_up_to_offset": -1, "ack_up_to_records": 0}  # ready signal first
    assert results[1]["ack_up_to_offset"] == 0 and results[1]["ack_up_to_records"] == 100
    assert results[2]["ack_up_to_offset"] == 1 and results[2]["ack_up_to_records"] == 150
    rows = mock.tables.read(AQ, limit=1000)
    assert len(rows) == 150 and {r.protocol for r in rows} == {"arrow"}
    assert rows[100].values["device_name"] == "y-0" and rows[100].offset == 1


def test_arrow_schema_with_an_extra_field_is_refused(flight_client, mock, fresh, toks):
    schema = ARROW_SCHEMA.append(pa.field("region", pa.string()))
    with pytest.raises((flight.FlightError, pa.ArrowException)) as e:
        put_arrow(flight_client, toks["aq"], AQ, schema, [])
    assert "FIELD_NOT_IN_TABLE" in str(e.value)


def test_arrow_nullability_must_match_the_table(flight_client, mock, fresh, toks):
    schema = pa.schema([pa.field("device_name", pa.string(), nullable=True), pa.field("temp", pa.int32()),
                        pa.field("humidity", pa.int64())])
    with pytest.raises((flight.FlightError, pa.ArrowException)) as e:
        put_arrow(flight_client, toks["aq"], AQ, schema, [])
    assert "nullability" in str(e.value)


def test_arrow_fields_must_follow_the_table_order(flight_client, mock, fresh, toks):
    schema = pa.schema([pa.field("temp", pa.int32()), pa.field("device_name", pa.string(), nullable=False),
                        pa.field("humidity", pa.int64())])
    with pytest.raises((flight.FlightError, pa.ArrowException)) as e:
        put_arrow(flight_client, toks["aq"], AQ, schema, [])
    assert "FIELD_OUT_OF_ORDER" in str(e.value)


def test_arrow_batch_with_a_wrong_offset_is_refused(flight_client, mock, fresh, toks):
    with pytest.raises((flight.FlightError, pa.ArrowException)) as e:
        put_arrow(flight_client, toks["aq"], AQ, ARROW_SCHEMA, [batch(2)], metadata_for=lambda n: b'{"offset_id": 4}')
    assert "expected offset_id 0, got 4" in str(e.value)


def test_arrow_missing_token_is_refused(flight_client, mock, fresh, toks):
    with pytest.raises((flight.FlightError, pa.ArrowException)) as e:
        put_arrow(flight_client, "", AQ, ARROW_SCHEMA, [])
    assert "UNAUTHENTICATED" in str(e.value) or "unauthenticated" in str(e.value).lower() or "token" in str(e.value).lower()


# ---------------------------------------------------------------- PersistentStream

def persist_create(table, record_type=pb.JSON):
    return pb.PersistentStreamRequest(create_stream=pb.CreatePersistentStreamRequest(
        create_stream=pb.CreateIngestStreamRequest(table_name=table, record_type=record_type)))


def persist_resume(stream_id, record_type=pb.JSON):
    return pb.PersistentStreamRequest(resume_stream=pb.ResumeIngestStreamRequest(stream_id=stream_id, record_type=record_type))


def persist_ingest(offset, obj):
    return pb.PersistentStreamRequest(ingest_record=pb.IngestRecordRequest(offset_id=offset, json_record=json.dumps(obj)))


def persistent(channel, requests, metadata):
    stub = pb_grpc.ZerobusStub(channel)
    return list(stub.PersistentStream(iter(requests), metadata=metadata, timeout=10))


def test_persistent_stream_resumes_from_the_committed_offset(channel, fresh, toks):
    first = persistent(channel, [persist_create(AQ)] + [persist_ingest(i, {"device_name": f"p{i}"}) for i in range(3)],
                       md(toks["aq"], AQ))
    stream_id = first[0].create_stream_response.stream_id
    assert acks(first)[-1] == 2
    second = persistent(channel, [persist_resume(stream_id)] + [persist_ingest(3, {"device_name": "p3"})], md(toks["aq"], AQ))
    assert second[0].resume_stream_response.last_committed_offset == 2
    assert acks(second) == [3]
    assert [r.offset for r in fresh.tables.read(AQ)] == [0, 1, 2, 3]


def test_persistent_resume_must_continue_after_the_watermark(channel, fresh, toks):
    first = persistent(channel, [persist_create(AQ), persist_ingest(0, {"device_name": "a"})], md(toks["aq"], AQ))
    stream_id = first[0].create_stream_response.stream_id
    stub = pb_grpc.ZerobusStub(channel)
    with pytest.raises(grpc.RpcError) as e:
        list(stub.PersistentStream(iter([persist_resume(stream_id), persist_ingest(0, {"device_name": "again"})]),
                                   metadata=md(toks["aq"], AQ), timeout=10))
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_retired_persistent_stream_cannot_resume_and_unknown_ids_are_not_found(channel, fresh, toks):
    first = persistent(channel, [persist_create(AQ)], md(toks["aq"], AQ))
    stream_id = first[0].create_stream_response.stream_id
    stub = pb_grpc.ZerobusStub(channel)
    stub.RetireStream(pb.RetireStreamRequest(table_name=AQ, stream_id=stream_id), metadata=md(toks["aq"], AQ), timeout=5)
    with pytest.raises(grpc.RpcError) as e:
        list(stub.PersistentStream(iter([persist_resume(stream_id)]), metadata=md(toks["aq"], AQ), timeout=5))
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    with pytest.raises(grpc.RpcError) as e:
        list(stub.PersistentStream(iter([persist_resume("mock-stream-999")]), metadata=md(toks["aq"], AQ), timeout=5))
    assert e.value.code() == grpc.StatusCode.NOT_FOUND


def test_persistent_protobuf_streams_are_supported(channel, fresh, toks):
    from google.protobuf import descriptor_pb2
    FD = descriptor_pb2.FieldDescriptorProto
    msgd = descriptor_pb2.DescriptorProto(name="PR")
    msgd.field.add(name="device_name", number=1, type=FD.TYPE_STRING, label=FD.LABEL_OPTIONAL)
    msgd.field.add(name="temp", number=2, type=FD.TYPE_INT32, label=FD.LABEL_OPTIONAL)
    from google.protobuf import descriptor_pool, message_factory
    pool = descriptor_pool.DescriptorPool()
    fdp = descriptor_pb2.FileDescriptorProto(name="pr.proto", package="zbt", syntax="proto2", message_type=[msgd])
    pool.Add(fdp)
    cls = message_factory.GetMessageClass(pool.FindMessageTypeByName("zbt.PR"))
    create = pb.PersistentStreamRequest(create_stream=pb.CreatePersistentStreamRequest(create_stream=pb.CreateIngestStreamRequest(
        table_name=PR_TABLE, record_type=pb.PROTO, descriptor_proto=msgd.SerializeToString())))
    rec = pb.PersistentStreamRequest(ingest_record=pb.IngestRecordRequest(
        offset_id=0, proto_encoded_record=cls(device_name="pp", temp=3).SerializeToString()))
    stub = pb_grpc.ZerobusStub(channel)
    out = list(stub.PersistentStream(iter([create, rec]), metadata=md(toks["proto_readings"], PR_TABLE), timeout=10))
    assert acks(out) == [0]
    assert fresh.tables.values(PR_TABLE)[0]["device_name"] == "pp"

