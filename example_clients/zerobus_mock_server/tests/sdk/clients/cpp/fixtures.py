"""Arrow IPC fixtures for the C++ client (the C++ SDK takes Arrow as IPC bytes; there is no Arrow C++ here).

Writes into the directory given as argv[1]:
  schema.bin           IPC stream holding the schema only (normal scenario)
  schema_mismatch.bin  the same plus a "region" column (mismatch scenario)
  batch_<k>.bin        IPC stream (schema + one record batch) for rows 4k..4k+3
"""

import os
import sys

import pyarrow as pa
import pyarrow.ipc as ipc


def schema(mismatch):
    fields = [pa.field("device_name", pa.string(), nullable=False), pa.field("temp", pa.int32()),
              pa.field("humidity", pa.int64())]
    if mismatch:
        fields.append(pa.field("region", pa.string()))
    return pa.schema(fields)


def write(path, data):
    with open(path, "wb") as fh:
        fh.write(data)


def main(out):
    os.makedirs(out, exist_ok=True)
    for name, mismatch in (("schema.bin", False), ("schema_mismatch.bin", True)):
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, schema(mismatch)):
            pass
        write(os.path.join(out, name), sink.getvalue().to_pybytes())
    for k in range(5):
        rows = range(4 * k, 4 * k + 4)
        batch = pa.record_batch({"device_name": [f"m-{i}" for i in rows], "temp": [i for i in rows],
                                 "humidity": [2] * 4}, schema=schema(False))
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, schema(False)) as writer:
            writer.write_batch(batch)
        write(os.path.join(out, f"batch_{k}.bin"), sink.getvalue().to_pybytes())
    print("fixtures written to", out)


if __name__ == "__main__":
    main(sys.argv[1])
