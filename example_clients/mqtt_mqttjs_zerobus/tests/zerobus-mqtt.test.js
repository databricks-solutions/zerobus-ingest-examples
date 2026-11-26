'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { createZerobusConnection } = require('../src/zerobus-mqtt');

// A stand-in for an mqtt.js client: it records the properties each CONNECT would send.
// Each CONNECT is acknowledged at once ('connect') unless a test sets autoConnack to false.
function fakeClient(options) {
  const client = new EventEmitter();
  client.options = { ...options };
  client.connects = [];
  client.publishes = [];
  client.autoConnack = true;
  const connect = () => {
    client.connects.push(client.options.properties?.userProperties?.Authorization);
    if (client.autoConnack) client.emit('connect');
  };
  client.connect = connect;
  client.reconnect = connect;
  client.publish = (topic, payload, opts, cb) => {
    client.publishes.push(payload);
    cb(null);
  };
  client.removeOutgoingMessage = () => {};
  client.end = (force, cb) => cb();
  return client;
}

test('every connect and reconnect carries the token fetched for that attempt', async () => {
  const tokens = ['token-1', 'token-2', 'token-3'];
  let issued = 0;
  const getToken = async () => tokens[issued++];
  let client;
  const conn = createZerobusConnection({
    host: 'example.invalid',
    tableName: 'main.default.dice_rolls',
    getToken,
    reconnectDelayMs: 1,
    log: () => {},
    connectFn: (options) => {
      client = fakeClient(options);
      return client;
    },
  });
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(client.connects, ['Bearer token-1']);

  client.emit('close');
  await new Promise((r) => setTimeout(r, 20));
  assert.deepEqual(client.connects, ['Bearer token-1', 'Bearer token-2']);

  client.emit('close');
  await new Promise((r) => setTimeout(r, 20));
  assert.deepEqual(client.connects, ['Bearer token-1', 'Bearer token-2', 'Bearer token-3']);
  await conn.close();
});

test('the CONNECT user properties name the token and the table', async () => {
  let client;
  createZerobusConnection({
    host: 'example.invalid',
    tableName: 'main.default.dice_rolls',
    getToken: async () => 'abc',
    log: () => {},
    connectFn: (options) => {
      client = fakeClient(options);
      return client;
    },
  });
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(client.options.properties.userProperties, {
    Authorization: 'Bearer abc',
    'x-databricks-zerobus-table-name': 'main.default.dice_rolls',
  });
});

test('a PUBACK with reason 0x99 rejects that publish and the connection stays up', async () => {
  let client;
  const conn = createZerobusConnection({
    host: 'example.invalid',
    tableName: 'main.default.dice_rolls',
    getToken: async () => 'abc',
    reconnectDelayMs: 1,
    log: () => {},
    connectFn: (options) => {
      client = fakeClient(options);
      // Payload format invalid (0x99 = 153): the server answers with an error PUBACK.
      client.publish = (topic, payload, opts, cb) => {
        const err = new Error('Publish error: Payload format invalid');
        err.code = 153;
        cb(err);
      };
      return client;
    },
  });
  await new Promise((r) => setImmediate(r));
  await assert.rejects(conn.publish('not json {'), /Payload format invalid/);

  // No close, no reconnect: the same connection carries the next record.
  assert.equal(client.connects.length, 1);
  client.publish = (topic, payload, opts, cb) => cb(null);
  await conn.publish({ player_name: 'Ada', die_sides: 6, roll: 4, rolled_at: 1 });
  assert.equal(client.connects.length, 1);
  await conn.close();
});

// Short timeout for the tests. A publish that never gets a PUBACK is simulated by keeping the
// publish callback and calling it (or not) later.
const TEST_ACK_TIMEOUT_MS = 20;

function connectWithStubPublish(publishFn, extra = {}) {
  let client;
  const conn = createZerobusConnection({
    host: 'example.invalid',
    tableName: 'main.default.dice_rolls',
    getToken: async () => 'abc',
    reconnectDelayMs: 10_000,
    publishAckTimeoutMs: TEST_ACK_TIMEOUT_MS,
    log: () => {},
    connectFn: (options) => {
      client = fakeClient(options);
      client.publish = publishFn;
      return client;
    },
    ...extra,
  });
  return { conn, client: () => client };
}

test('no PUBACK within the timeout rejects the publish and removes its outgoing message', async () => {
  const acks = [];
  const removed = [];
  const { conn, client } = connectWithStubPublish((topic, payload, opts, cb) => acks.push(cb));
  await new Promise((r) => setImmediate(r));
  client().removeOutgoingMessage = (messageId) => {
    // mqtt.js calls the publish callback with this error when a message is removed.
    removed.push(messageId);
    acks[acks.length - 1](new Error('Message removed'));
  };

  const settled = conn.publish('not json {');
  client().emit('packetsend', { cmd: 'publish', messageId: 7 });
  await assert.rejects(settled, /no PUBACK within 20 ms; the record was not acknowledged/);
  assert.deepEqual(removed, [7]);
  // The connection stays open: no close, no reconnect.
  assert.equal(client().connects.length, 1);
  await conn.close();
});

test('a PUBACK before the timeout settles the publish and clears the timer', async (t) => {
  const realSetTimeout = globalThis.setTimeout;
  const realClearTimeout = globalThis.clearTimeout;
  const ackTimers = [];
  const cleared = [];
  t.mock.method(globalThis, 'setTimeout', (fn, ms, ...rest) => {
    const handle = realSetTimeout(fn, ms, ...rest);
    if (ms === TEST_ACK_TIMEOUT_MS) ackTimers.push(handle);
    return handle;
  });
  t.mock.method(globalThis, 'clearTimeout', (handle) => {
    cleared.push(handle);
    return realClearTimeout(handle);
  });

  let ack;
  const removed = [];
  const { conn, client } = connectWithStubPublish((topic, payload, opts, cb) => {
    ack = cb;
  });
  await new Promise((r) => setImmediate(r));
  client().removeOutgoingMessage = (messageId) => removed.push(messageId);

  const settled = conn.publish({ player_name: 'Ada', die_sides: 6, roll: 4, rolled_at: 1 });
  client().emit('packetsend', { cmd: 'publish', messageId: 8 });
  ack(null);
  await settled;

  assert.equal(ackTimers.length, 1);
  assert.ok(cleared.includes(ackTimers[0]), 'the ack timer was cleared on PUBACK');
  assert.deepEqual(removed, []);
  await conn.close();
});

test('a late PUBACK after the timeout is ignored and does not throw', async () => {
  const acks = [];
  const { conn, client } = connectWithStubPublish((topic, payload, opts, cb) => acks.push(cb));
  await new Promise((r) => setImmediate(r));
  client().removeOutgoingMessage = (messageId) => acks[acks.length - 1](new Error('Message removed'));

  const first = conn.publish({ player_name: 'Ada', die_sides: 6, roll: 2, rolled_at: 1 });
  client().emit('packetsend', { cmd: 'publish', messageId: 9 });
  await assert.rejects(first, /no PUBACK within/);

  // The server's PUBACK arrives after the timeout: both success and error must be ignored.
  assert.doesNotThrow(() => acks[0](null));
  assert.doesNotThrow(() => acks[0](new Error('Publish error: Payload format invalid')));

  // The connection still carries the next record, and that publish settles normally.
  client().publish = (topic, payload, opts, cb) => {
    acks.push(cb);
    cb(null);
  };
  await conn.publish({ player_name: 'Grace', die_sides: 6, roll: 3, rolled_at: 2 });
  assert.equal(client().connects.length, 1);
  await conn.close();
});

test('closing the connection settles the pending publish and clears its timer', async (t) => {
  const realSetTimeout = globalThis.setTimeout;
  const realClearTimeout = globalThis.clearTimeout;
  const ackTimers = [];
  const cleared = [];
  t.mock.method(globalThis, 'setTimeout', (fn, ms, ...rest) => {
    const handle = realSetTimeout(fn, ms, ...rest);
    if (ms === TEST_ACK_TIMEOUT_MS) ackTimers.push(handle);
    return handle;
  });
  t.mock.method(globalThis, 'clearTimeout', (handle) => {
    cleared.push(handle);
    return realClearTimeout(handle);
  });

  const removed = [];
  const { conn, client } = connectWithStubPublish(() => {});
  await new Promise((r) => setImmediate(r));
  client().removeOutgoingMessage = (messageId) => removed.push(messageId);

  const settled = conn.publish('not json {');
  client().emit('packetsend', { cmd: 'publish', messageId: 10 });
  client().emit('close');
  await assert.rejects(settled, /connection closed before PUBACK/);

  assert.deepEqual(removed, [10]);
  assert.ok(cleared.includes(ackTimers[0]), 'the ack timer was cleared on close');
  await conn.close();
});

// Like connectWithStubPublish, but the first CONNECT is not acknowledged until the test emits 'connect'.
function connectOffline() {
  let client;
  const conn = createZerobusConnection({
    host: 'example.invalid',
    tableName: 'main.default.dice_rolls',
    getToken: async () => 'abc',
    reconnectDelayMs: 1,
    publishAckTimeoutMs: TEST_ACK_TIMEOUT_MS,
    log: () => {},
    connectFn: (options) => {
      client = fakeClient(options);
      client.autoConnack = false;
      return client;
    },
  });
  return { conn, client: () => client };
}

test('a publish while disconnected waits, then is sent once after the connection is up', async () => {
  const { conn, client } = connectOffline();
  await new Promise((r) => setImmediate(r)); // the first CONNECT is out, not yet acknowledged
  assert.equal(client().publishes.length, 0);

  const record = { player_name: 'Ada', die_sides: 6, roll: 4, rolled_at: 1 };
  const settled = conn.publish(record);
  assert.equal(client().publishes.length, 0, 'mqtt.js is not given the record while the connection is down');

  // A failed attempt closes the connection while the record waits: the record keeps waiting.
  client().emit('close');
  client().emit('connect');
  await settled;
  assert.deepEqual(client().publishes, [JSON.stringify(record)]);
  await conn.close();
});

test('a record that times out while disconnected is never sent after reconnect', async () => {
  const { conn, client } = connectOffline();
  await new Promise((r) => setImmediate(r));
  const removed = [];
  client().removeOutgoingMessage = (messageId) => removed.push(messageId);

  const timedOut = conn.publish({ player_name: 'Grace', die_sides: 6, roll: 2, rolled_at: 2 });
  await assert.rejects(timedOut, /no connection within 20 ms; the record was not sent/);
  assert.equal(client().publishes.length, 0);

  // The connection comes up. The record the caller was told failed must not be sent.
  client().emit('close');
  client().emit('connect');
  await new Promise((r) => setImmediate(r));
  assert.equal(client().publishes.length, 0, 'the rejected record was not sent after reconnect');
  assert.deepEqual(removed, []);

  // The next record on the new connection is sent normally.
  await conn.publish({ player_name: 'Linus', die_sides: 6, roll: 5, rolled_at: 3 });
  assert.equal(client().publishes.length, 1);
  assert.match(client().publishes[0], /"player_name":"Linus"/);
  await conn.close();
});
