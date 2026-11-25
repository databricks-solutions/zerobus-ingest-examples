"""SDK matrix client, Python (databricks-zerobus-ingest-sdk, pinned in the Dockerfile).

    python client.py <format> <scenario> --table T --endpoint E --uc U --client-id ID --client-secret SEC

formats: json, proto, arrow, avro. scenarios: normal, abort, token401, mismatch.
Prints one line at the end: ZB_RESULT {"ok": bool, "error": str, "acked": int}.
"""

import argparse
import json
import sys

import pyarrow as pa
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from zerobus.sdk.shared import TableProperties
from zerobus.sdk.sync import ZerobusSdk

N = 20
MISMATCH_AT = 5
FD = descriptor_pb2.FieldDescriptorProto


def rows(mismatch):
    out = []
    for i in range(N):
        r = {"device_name": f"m-{i}", "temp": i, "humidity": 2}
        if mismatch and i == MISMATCH_AT:
            r["region"] = "eu"
        out.append(r)
    return out


def arrow_schema(mismatch):
    fields = [pa.field("device_name", pa.string(), nullable=False), pa.field("temp", pa.int32()),
              pa.field("humidity", pa.int64())]
    if mismatch:
        fields.append(pa.field("region", pa.string()))
    return pa.schema(fields)


def avro_schema(mismatch):
    fields = [{"name": "device_name", "type": "string"}, {"name": "temp", "type": ["null", "int"], "default": None},
              {"name": "humidity", "type": ["null", "long"], "default": None}]
    if mismatch:
        fields.append({"name": "region", "type": "string"})
    return {"type": "record", "name": "SdkMatrix", "fields": fields}


def proto_descriptor(mismatch):
    msg = descriptor_pb2.DescriptorProto(name="SdkMatrix")
    msg.field.add(name="device_name", number=1, type=FD.TYPE_STRING, label=FD.LABEL_OPTIONAL)
    msg.field.add(name="temp", number=2, type=FD.TYPE_INT32, label=FD.LABEL_OPTIONAL)
    msg.field.add(name="humidity", number=3, type=FD.TYPE_INT64, label=FD.LABEL_OPTIONAL)
    if mismatch:
        msg.field.add(name="region", number=4, type=FD.TYPE_STRING, label=FD.LABEL_OPTIONAL)
    return msg


def run(args):
    sdk = ZerobusSdk(args.endpoint, args.uc)
    mismatch = args.scenario == "mismatch"
    acked = 0
    if args.format == "json":
        s = sdk.create_stream(args.client_id, args.client_secret, TableProperties(args.table))
        for rec in rows(mismatch):
            s.ingest_record_offset(rec)
            acked += 1
        s.flush()
        s.close()
    elif args.format == "avro":
        props = TableProperties(args.table, avro_schema=json.dumps(avro_schema(mismatch)))
        s = sdk.create_stream(args.client_id, args.client_secret, props)
        for rec in rows(mismatch):
            s.ingest_record_offset(rec)
            acked += 1
        s.flush()
        s.close()
    elif args.format == "proto":
        msg = proto_descriptor(mismatch)
        fdp = descriptor_pb2.FileDescriptorProto(name="sdk_matrix.proto", package="zbmatrix", syntax="proto2", message_type=[msg])
        pool = descriptor_pool.DescriptorPool()
        pool.Add(fdp)
        cls = message_factory.GetMessageClass(pool.FindMessageTypeByName("zbmatrix.SdkMatrix"))
        s = sdk.create_stream(args.client_id, args.client_secret,
                              TableProperties(args.table, cls.DESCRIPTOR.file.serialized_pb))
        for rec in rows(False):  # the record itself never carries region
            m = cls(device_name=rec["device_name"], temp=rec["temp"], humidity=rec["humidity"])
            s.ingest_record_offset(m)
            acked += 1
        s.flush()
        s.close()
    elif args.format == "arrow":
        schema = arrow_schema(mismatch)
        s = sdk.create_arrow_stream(args.table, schema, args.client_id, args.client_secret)
        data = rows(False)
        for start in range(0, N, 4):
            chunk = data[start:start + 4]
            batch = pa.record_batch({"device_name": [r["device_name"] for r in chunk],
                                     "temp": [r["temp"] for r in chunk],
                                     "humidity": [r["humidity"] for r in chunk]},
                                    schema=arrow_schema(False))
            s.ingest_batch(batch)
            acked += len(chunk)
        s.flush()
        s.close()
    else:
        raise SystemExit(f"unknown format {args.format}")
    return acked


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("format")
    ap.add_argument("scenario")
    ap.add_argument("--table", required=True)
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--uc", required=True)
    ap.add_argument("--client-id", required=True)
    ap.add_argument("--client-secret", required=True)
    args = ap.parse_args(argv)
    try:
        acked = run(args)
        result = {"ok": True, "error": "", "acked": acked}
    except Exception as exc:  # noqa: BLE001 - the verdict reads the SDK's message
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}", "acked": 0}
    print("ZB_RESULT " + json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
