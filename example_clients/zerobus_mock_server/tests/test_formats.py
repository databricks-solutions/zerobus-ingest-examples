"""Avro over the gRPC stream, with raw datums: the writer schema, decoding, and trailing bytes."""

import io
import json

import fastavro
import grpc

from helpers import AQ, get_token
from zerobus_mock_server.pb import zerobus_service_pb2 as pb
from zerobus_mock_server.pb import zerobus_service_pb2_grpc as pb_grpc

AVRO_AQ = {"type": "record", "name": "AirQuality", "fields": [
    {"name": "device_name", "type": "string"},
    {"name": "temp", "type": ["null", "int"], "default": None},
    {"name": "humidity", "type": ["null", "long"], "default": None}]}


def _datum(obj) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(AVRO_AQ), obj)
    return buf.getvalue()


def _run(channel, token, records):
    requests = [pb.EphemeralStreamRequest(create_stream=pb.CreateIngestStreamRequest(
        table_name=AQ, record_type=pb.AVRO, avro_schema_json=json.dumps(AVRO_AQ)))]
    requests += [pb.EphemeralStreamRequest(ingest_record=pb.IngestRecordRequest(offset_id=i, avro_encoded_record=raw))
                 for i, raw in enumerate(records)]
    md = [("authorization", f"Bearer {token}"), ("x-databricks-zerobus-table-name", AQ)]
    stub = pb_grpc.ZerobusStub(channel)
    acks, error = [], None
    try:
        for resp in stub.EphemeralStream(iter(requests), metadata=md, timeout=10):
            if resp.WhichOneof("payload") == "ingest_record_response":
                acks.append(resp.ingest_record_response.durability_ack_up_to_offset)
    except grpc.RpcError as err:
        error = err
    return acks, error


def test_avro_datums_are_stored_with_the_writer_schema(channel, fresh, rest):
    token = get_token(rest, AQ)
    acks, error = _run(channel, token, [_datum({"device_name": "a", "temp": 1, "humidity": 9}),
                                        _datum({"device_name": "b", "temp": None, "humidity": None})])
    assert error is None and acks == [0, 1]
    assert fresh.tables.rows(AQ) == [{"device_name": "a", "temp": 1, "humidity": 9},
                                     {"device_name": "b", "temp": None, "humidity": None}]


def test_trailing_bytes_after_a_datum_are_refused(channel, fresh, rest):
    token = get_token(rest, AQ)
    acks, error = _run(channel, token, [_datum({"device_name": "a"}) + b"\x00"])
    assert acks == [] and error.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "trailing bytes" in error.details()
    assert fresh.tables.rows(AQ) == []
