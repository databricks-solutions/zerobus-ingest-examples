// SDK matrix client, TypeScript (@databricks/zerobus-ingest-sdk 1.5.0, from npm).
// Usage: node client.js <format> <scenario> --table T --endpoint E --uc U --client-id ID --client-secret SEC
'use strict';
const sdk = require('@databricks/zerobus-ingest-sdk');
const { tableToIPC, RecordBatch, Schema, Field, Utf8, Int32, Int64, Struct, Table, makeData, makeVector, vectorFromArray } = require('apache-arrow');
const avro = require('avsc');

const N = 20;
const MISMATCH_AT = 5;

function args() {
  const [format, scenario, ...rest] = process.argv.slice(2);
  const opt = {};
  for (let i = 0; i + 1 < rest.length; i += 2) opt[rest[i].slice(2)] = rest[i + 1];
  return { format, scenario, opt };
}

// --- protobuf wire encoding for the fixed SdkMatrix message (no protobufjs needed)
function varint(n) {
  let v = BigInt(n);
  const out = [];
  while (v >= 0x80n) { out.push(Number(v & 0x7fn) | 0x80); v >>= 7n; }
  out.push(Number(v));
  return Buffer.from(out);
}
const key = (f, wt) => varint((f << 3) | wt);
const lenField = (f, buf) => Buffer.concat([key(f, 2), varint(buf.length), buf]);
const strField = (f, s) => lenField(f, Buffer.from(s, 'utf8'));
const intField = (f, v) => Buffer.concat([key(f, 0), varint(v)]);

function fieldDescriptor(name, number, type) {
  // FieldDescriptorProto: name=1, number=3, label=4 (1 = optional), type=5
  return Buffer.concat([strField(1, name), intField(3, number), intField(4, 1), intField(5, type)]);
}
const TYPE_STRING = 9, TYPE_INT32 = 5, TYPE_INT64 = 3;

function protoDescriptor(mismatch) {
  // DescriptorProto: name=1, field=2
  const parts = [strField(1, 'SdkMatrix'),
    lenField(2, fieldDescriptor('device_name', 1, TYPE_STRING)),
    lenField(2, fieldDescriptor('temp', 2, TYPE_INT32)),
    lenField(2, fieldDescriptor('humidity', 3, TYPE_INT64))];
  if (mismatch) parts.push(lenField(2, fieldDescriptor('region', 4, TYPE_STRING)));
  return Buffer.concat(parts);
}

function protoRecord(i) {
  return Buffer.concat([strField(1, `m-${i}`), intField(2, i), intField(3, 2)]);
}

const AVRO_FIELDS = [
  { name: 'device_name', type: 'string' },
  { name: 'temp', type: ['null', 'int'], default: null },
  { name: 'humidity', type: ['null', 'long'], default: null },
];

function arrowSchemaFields(mismatch) {
  // device_name is NOT NULL in the matrix table, so the Arrow field must be non-nullable too (docs: nullability matches).
  const f = [{ name: 'device_name', dataType: sdk.ArrowDataType.Utf8, nullable: false },
    { name: 'temp', dataType: sdk.ArrowDataType.Int32, nullable: true },
    { name: 'humidity', dataType: sdk.ArrowDataType.Int64, nullable: true }];
  if (mismatch) f.push({ name: 'region', dataType: sdk.ArrowDataType.Utf8, nullable: true });
  return f;
}

function arrowBatchIpc(start, rows) {
  const schema = new Schema([new Field('device_name', new Utf8(), false), new Field('temp', new Int32(), true),
    new Field('humidity', new Int64(), true)]);
  const dev = vectorFromArray(Array.from({ length: rows }, (_, j) => `m-${start + j}`), new Utf8());
  const temp = makeVector(Int32Array.from({ length: rows }, (_, j) => start + j));
  const hum = makeVector(BigInt64Array.from({ length: rows }, () => 2n));
  const data = makeData({ type: new Struct(schema.fields), length: rows, children: [dev.data[0], temp.data[0], hum.data[0]] });
  return Buffer.from(tableToIPC(new Table(new RecordBatch(schema, data)), 'stream'));
}

// The SDK can report a failed ingest as a generic "Stream closed" error, and the stream's own
// failure (with the server's message) comes back from flush(). Flush once after a failed ingest so
// the real cause is what gets reported.
async function ingestAll(stream, records, onEach) {
  let acked = 0;
  try {
    for (const rec of records) {
      await stream.ingestRecordOffset(rec);
      acked++;
    }
  } catch (ingestErr) {
    try {
      await stream.flush();
    } catch (flushErr) {
      throw flushErr;
    }
    throw ingestErr;
  }
  return acked;
}

async function run(format, scenario, opt) {
  const mismatch = scenario === 'mismatch';
  const client = new sdk.ZerobusSdk(opt.endpoint, opt.uc);
  const id = opt['client-id'];
  const secret = opt['client-secret'];
  const table = opt.table;
  let acked = 0;
  if (format === 'json') {
    const stream = await client.createStream({ tableName: table }, id, secret, { recordType: sdk.RecordType.Json });
    const recs = [];
    for (let i = 0; i < N; i++) {
      const rec = { device_name: `m-${i}`, temp: i, humidity: 2 };
      if (mismatch && i === MISMATCH_AT) rec.region = 'eu';
      recs.push(rec);
    }
    acked = await ingestAll(stream, recs);
    await stream.flush();
    await stream.close();
  } else if (format === 'proto') {
    const stream = await client.createStream({ tableName: table, descriptorProto: protoDescriptor(mismatch).toString('base64') },
      id, secret, { recordType: sdk.RecordType.Proto });
    acked = await ingestAll(stream, Array.from({ length: N }, (_, i) => protoRecord(i)));
    await stream.flush();
    await stream.close();
  } else if (format === 'avro') {
    const fields = AVRO_FIELDS.concat(mismatch ? [{ name: 'region', type: 'string' }] : []);
    const avroSchema = JSON.stringify({ type: 'record', name: 'SdkMatrix', fields });
    const stream = await client.createStream({ tableName: table, avroSchema }, id, secret, { recordType: sdk.RecordType.Avro });
    const recs = Array.from({ length: N }, (_, i) => {
      const rec = { device_name: `m-${i}`, temp: i, humidity: 2 };
      if (mismatch) rec.region = 'eu';
      return rec;
    });
    acked = await ingestAll(stream, recs);
    await stream.flush();
    await stream.close();
  } else if (format === 'arrow') {
    const stream = await client.createArrowStream({ tableName: table, schemaFields: arrowSchemaFields(mismatch) }, id, secret);
    for (let start = 0; start < N; start += 4) {
      await stream.ingestBatch(arrowBatchIpc(start, 4));
      acked += 4;
    }
    await stream.flush();
    await stream.close();
  } else {
    throw new Error(`unknown format ${format}`);
  }
  return acked;
}

(async () => {
  const { format, scenario, opt } = args();
  let result;
  try {
    const acked = await run(format, scenario, opt);
    result = { ok: true, error: '', acked };
  } catch (exc) {
    result = { ok: false, error: `${exc.name || 'Error'}: ${exc.message}`, acked: 0 };
  }
  console.log('ZB_RESULT ' + JSON.stringify(result));
})();
