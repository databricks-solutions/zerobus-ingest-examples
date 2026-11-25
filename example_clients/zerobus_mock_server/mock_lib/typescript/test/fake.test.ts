// Unit tests for the TypeScript mock lib: the controls, the caller-visible outcomes, and scenarios.json.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { FakeMock, FakeZerobusSdk, SCENARIOS } from '../src/fake.ts';

const TABLE = { tableName: 'main.default.events' };
const CONTRACT = JSON.parse(readFileSync(new URL('../../scenarios.json', import.meta.url), 'utf8')).scenarios as Record<
  string,
  { where: string | null; message: string | null; retried_by_sdk: boolean }
>;

async function streamFor(mock = new FakeMock()) {
  return { mock, stream: await new FakeZerobusSdk(mock).createStream(TABLE, 'id', 'secret') };
}

test('the scenario table matches scenarios.json', () => {
  assert.deepEqual(Object.keys(SCENARIOS).sort(), Object.keys(CONTRACT).sort());
  for (const [name, want] of Object.entries(CONTRACT)) {
    const have = SCENARIOS[name];
    assert.equal(have.where, want.where, `${name} where`);
    assert.equal(have.retriedBySdk, want.retried_by_sdk, `${name} retried`);
    if (want.message !== null) {
      assert.ok(have.text.includes(want.message), `${name}: '${have.text}' lacks the contract message`);
    }
  }
});

test('records are recorded in order with offsets, and batches return the last offset', async () => {
  const { stream } = await streamFor();
  assert.equal(await stream.ingestRecordOffset({ n: 0 }), 0n);
  assert.equal(await stream.ingestRecordOffset({ n: 1 }), 1n);
  assert.equal(await stream.ingestRecordsOffset([{ n: 2 }, { n: 3 }]), 3n);
  assert.equal(await stream.ingestRecordsOffset([]), null);
  await stream.flush();
  assert.deepEqual(stream.ingested(), [{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }]);
  assert.deepEqual(stream.stored(), stream.ingested());
  assert.deepEqual(await stream.getUnackedRecords(), []);
});

test('failNext with after skips matching calls, then fails once (schema-mismatch)', async () => {
  const { mock, stream } = await streamFor();
  mock.failNext('schema-mismatch', { after: 2 });
  await stream.ingestRecordOffset('a');
  await stream.ingestRecordOffset('b');
  await assert.rejects(stream.ingestRecordOffset('c'), /does not fit/);
  assert.equal(await stream.ingestRecordOffset('d'), 2n, 'one-shot: the next call succeeds');
  assert.deepEqual(stream.ingested(), ['a', 'b', 'd']);
});

test('record-too-large raises and the record is not stored', async () => {
  const { mock, stream } = await streamFor();
  mock.failNext('record-too-large');
  await assert.rejects(stream.ingestRecordOffset('big'), /maximum message size/);
  assert.deepEqual(stream.stored(), []);
});

test('stream-abort-fatal is terminal for that stream; earlier records are kept', async () => {
  const { mock, stream } = await streamFor();
  await stream.ingestRecordOffset('kept');
  mock.failNext('stream-abort-fatal');
  await assert.rejects(stream.ingestRecordOffset('x'), /terminal stream error/);
  await assert.rejects(stream.ingestRecordOffset('after'), /terminal stream error/);
  await assert.rejects(stream.flush(), /terminal stream error/);
  assert.deepEqual(stream.stored(), ['kept']);
  assert.equal(await (await new FakeZerobusSdk(mock).createStream(TABLE, 'id', 'secret')).ingestRecordOffset('new'), 0n);
});

test('auth-failure and table-not-found reject at create', async () => {
  const mock = new FakeMock();
  const sdk = new FakeZerobusSdk(mock);
  mock.failNext('auth-failure');
  await assert.rejects(sdk.createStream(TABLE, 'id', 'secret'), (e: Error & { retriedBySdk: boolean }) => {
    assert.match(e.message, /invalid_client/);
    assert.equal(e.retriedBySdk, false);
    return true;
  });
  mock.failNext('table-not-found');
  await assert.rejects(sdk.createStream(TABLE, 'id', 'secret'), /cannot be found/);
});

test('server-unavailable holds at create until cleared', async () => {
  const mock = new FakeMock();
  const sdk = new FakeZerobusSdk(mock);
  mock.setScenario('server-unavailable');
  await assert.rejects(sdk.createStream(TABLE, 'id', 'secret'), /UNAVAILABLE/);
  await assert.rejects(sdk.createStream(TABLE, 'id', 'secret'), /UNAVAILABLE/, 'still held');
  mock.clearScenario();
  assert.equal(await (await sdk.createStream(TABLE, 'id', 'secret')).ingestRecordOffset('ok'), 0n);
});

test('token expiry: create refreshes and succeeds; mid-stream the stream fails for good', async () => {
  const mock = new FakeMock();
  mock.expireToken();
  const stream = await new FakeZerobusSdk(mock).createStream(TABLE, 'id', 'secret');
  mock.expireToken();
  await assert.rejects(stream.ingestRecordOffset('late'), /expired/);
  await assert.rejects(stream.ingestRecordOffset('later'), /expired/);
  assert.equal(await (await new FakeZerobusSdk(mock).createStream(TABLE, 'id', 'secret')).ingestRecordOffset('x'), 0n);
});

test('no-acks: flush times out and records are kept; clearing lets the next flush succeed', async () => {
  const { mock, stream } = await streamFor();
  mock.setScenario('no-acks');
  await stream.ingestRecordOffset('a');
  await assert.rejects(stream.flush(), /timed out/);
  assert.deepEqual(stream.stored(), ['a']);
  assert.deepEqual(await stream.getUnackedRecords(), ['a']);
  mock.clearScenario();
  await stream.flush();
  assert.deepEqual(await stream.getUnackedRecords(), []);
});

test('graceful-close raises nothing and stores the record', async () => {
  const { mock, stream } = await streamFor();
  mock.failNext('graceful-close');
  assert.equal(await stream.ingestRecordOffset('g'), 0n);
  await stream.flush();
  await stream.close();
  assert.deepEqual(stream.stored(), ['g']);
});

test('stream-abort-retryable: no error, and the resend stores the record twice', async () => {
  const { mock, stream } = await streamFor();
  mock.failNext('stream-abort-retryable', { after: 1 });
  await stream.ingestRecordOffset('once');
  assert.equal(await stream.ingestRecordOffset('twice'), 1n);
  await stream.flush();
  assert.deepEqual(stream.stored(), ['once', 'twice', 'twice']);
  assert.deepEqual(stream.ingested(), ['once', 'twice']);
});

test('schema-change-while-open raises nothing and routes the rows to fallback', async () => {
  const { mock, stream } = await streamFor();
  await stream.ingestRecordOffset('before');
  mock.failNext('schema-change-while-open');
  await stream.ingestRecordOffset('during');
  await stream.flush();
  assert.deepEqual(stream.stored(), ['before']);
  assert.deepEqual(stream.fallback(), ['during']);
});

test('reset clears fail-next budgets, the held scenario and the expiry budget', async () => {
  const mock = new FakeMock();
  mock.failNext('auth-failure');
  mock.setScenario('no-acks');
  mock.expireToken(2);
  mock.reset();
  const { stream } = await streamFor(mock);
  assert.equal(await stream.ingestRecordOffset('fine'), 0n);
  await stream.flush();
});

test('an unknown scenario is rejected, and token-expired cannot be held', () => {
  const mock = new FakeMock();
  assert.throws(() => mock.failNext('no-such'), /unknown scenario/);
  assert.throws(() => mock.setScenario('token-expired'), /one-shot/);
});
