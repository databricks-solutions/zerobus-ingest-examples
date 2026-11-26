'use strict';

const mqtt = require('mqtt');

const MQTT_PORT = 8883;
const TABLE_PROPERTY = 'x-databricks-zerobus-table-name';
// How long publish() gives one record: the wait for a connection plus the wait for its PUBACK.
const PUBLISH_ACK_TIMEOUT_MS = 30_000;

// MQTT 5 User Properties sent in CONNECT.
function connectProperties(token, tableName) {
  return {
    userProperties: {
      Authorization: `Bearer ${token}`,
      [TABLE_PROPERTY]: tableName,
    },
  };
}

/**
 * One Zerobus Ingest MQTT connection that gets a fresh OAuth token before every CONNECT.
 *
 * mqtt.js copies `client.options.properties` into each CONNECT packet when connect() or
 * reconnect() runs. Setting that object to a new token is the per-connection refresh hook.
 *
 * Automatic reconnect is off (reconnectPeriod 0) because mqtt.js emits its 'reconnect'
 * event and connects in the same synchronous step. The token request is async, so the
 * 'close' handler fetches the token first and then calls reconnect().
 *
 * publish() sends one QoS 1 record and resolves on a successful PUBACK. It rejects when the
 * PUBACK has an error reason code, when the connection closes before the PUBACK arrives, or
 * when the record doesn't finish within publishAckTimeoutMs. If the connection is down,
 * publish() waits for the next connection before it hands the record to mqtt.js, because
 * mqtt.js would queue a QoS 1 record and send it after a reconnect even after publish() had
 * rejected it. A record that times out while it waits is never sent. The connection can stay
 * open when a PUBACK times out.
 * Call it one record at a time: the connection tracks one unacknowledged record.
 */
function createZerobusConnection({
  host,
  tableName,
  getToken,
  connectFn = mqtt.connect,
  reconnectDelayMs = 2000,
  maxReconnectDelayMs = 30_000,
  publishAckTimeoutMs = PUBLISH_ACK_TIMEOUT_MS,
  log = console.log,
}) {
  let stopped = false;
  let retryTimer = null;
  let attempt = 0;
  let firstConnect = true;
  let ready = false; // true from a 'connect' event until the next 'close'
  let connectWaiters = []; // functions that run on the next 'connect' event
  let lastDisconnectReason = null;
  let pending = null; // { messageId, sent, resolve, reject, timer } for the one record in flight

  const client = connectFn({
    protocol: 'mqtts',
    host,
    port: MQTT_PORT,
    servername: host,
    protocolVersion: 5,
    clientId: `zerobus-mqttjs-${process.pid}-${Date.now()}`,
    clean: true,
    keepalive: 60,
    reconnectPeriod: 0,
    manualConnect: true,
    connectTimeout: 30_000,
  });

  async function connectWithFreshToken() {
    const token = await getToken();
    // mqtt.js reads this object when it builds the next CONNECT packet.
    client.options.properties = connectProperties(token, tableName);
    if (firstConnect) {
      firstConnect = false;
      client.connect();
    } else {
      client.reconnect();
    }
  }

  function scheduleReconnect() {
    if (stopped || retryTimer) return;
    const delay = Math.min(reconnectDelayMs * 2 ** attempt, maxReconnectDelayMs);
    attempt += 1;
    log(`reconnecting in ${delay} ms`);
    retryTimer = setTimeout(async () => {
      retryTimer = null;
      try {
        await connectWithFreshToken();
      } catch (err) {
        log(`token request failed: ${err.message}`);
        scheduleReconnect();
      }
    }, delay);
  }

  // mqtt.js emits 'connect' after it has resent its stored packets, so a publish started here
  // is sent at once and never waits behind a resend.
  client.on('connect', () => {
    attempt = 0;
    lastDisconnectReason = null;
    ready = true;
    log('CONNACK success');
    const waiters = connectWaiters;
    connectWaiters = [];
    waiters.forEach((run) => run());
  });
  client.on('packetsend', (packet) => {
    if (packet.cmd === 'publish' && pending && pending.sent && pending.messageId === undefined) {
      pending.messageId = packet.messageId;
    }
  });
  client.on('packetreceive', (packet) => {
    if (packet.cmd === 'disconnect') lastDisconnectReason = packet.reasonCode;
  });
  client.on('reconnect', () => log('reconnect: CONNECT with the token set for this attempt'));
  client.on('error', (err) => log(`MQTT error: ${err.message}`));
  client.on('close', () => {
    log('connection closed');
    ready = false;
    // A record that is still waiting for a connection stays waiting. It is sent on the next
    // connection, or it times out.
    if (pending && pending.sent) {
      // Without this, mqtt.js would resend the record on the next connection.
      const { reject, messageId, timer } = pending;
      pending = null;
      clearTimeout(timer);
      if (messageId !== undefined) client.removeOutgoingMessage(messageId);
      const reason = lastDisconnectReason === null ? '' : ` (server DISCONNECT reason ${lastDisconnectReason})`;
      reject(new Error(`connection closed before PUBACK${reason}`));
    }
    scheduleReconnect();
  });

  // Resolves on the next successful CONNACK, or at once if the connection is up.
  function waitForConnect() {
    if (ready) return Promise.resolve();
    return new Promise((resolve) => connectWaiters.push(resolve));
  }

  connectWithFreshToken().catch((err) => {
    log(`token request failed: ${err.message}`);
    scheduleReconnect();
  });

  // `record` is a JSON-ready object. A string is sent as-is, so a caller can send a payload that isn't JSON.
  function publish(record) {
    const payload = typeof record === 'string' ? record : JSON.stringify(record);
    return new Promise((resolve, reject) => {
      const entry = { messageId: undefined, sent: false, resolve, reject, timer: null };
      const send = () => {
        if (pending !== entry) return; // timed out while it waited: never hand it to mqtt.js
        entry.sent = true;
        client.publish(tableName, payload, { qos: 1 }, (err) => {
          // A late callback after a timeout or a close finds pending settled and is ignored.
          if (pending !== entry) return;
          clearTimeout(entry.timer);
          pending = null;
          if (err) reject(err);
          else resolve();
        });
      };
      pending = entry;
      entry.timer = setTimeout(() => {
        if (pending !== entry) return; // already settled
        // Clear pending before removeOutgoingMessage: it calls the publish callback with an
        // error, and that callback must find nothing to settle.
        pending = null;
        if (entry.sent) {
          if (entry.messageId !== undefined) client.removeOutgoingMessage(entry.messageId);
          reject(new Error(`no PUBACK within ${publishAckTimeoutMs} ms; the record was not acknowledged`));
        } else {
          connectWaiters = connectWaiters.filter((run) => run !== send);
          reject(new Error(`no connection within ${publishAckTimeoutMs} ms; the record was not sent`));
        }
      }, publishAckTimeoutMs);
      if (ready) send();
      else connectWaiters.push(send);
    });
  }

  function close() {
    stopped = true;
    clearTimeout(retryTimer);
    return new Promise((resolve) => client.end(false, () => resolve()));
  }

  return { client, waitForConnect, publish, close };
}

module.exports = { createZerobusConnection, connectProperties, TABLE_PROPERTY, PUBLISH_ACK_TIMEOUT_MS };
