'use strict';

const { fetchZerobusToken } = require('./token');
const { nextRoll } = require('./dice');
const { createZerobusConnection } = require('./zerobus-mqtt');

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Set the ${name} environment variable (see config.example.env)`);
  return value;
}

function readConfig(env = process.env) {
  return {
    workspaceUrl: required('DATABRICKS_WORKSPACE_URL').replace(/\/$/, ''),
    workspaceId: required('DATABRICKS_WORKSPACE_ID'),
    clientId: required('DATABRICKS_CLIENT_ID'),
    clientSecret: required('DATABRICKS_CLIENT_SECRET'),
    tableName: required('DATABRICKS_TABLE_NAME'),
    mqttHost: required('ZEROBUS_MQTT_HOST'),
    intervalMs: Number(env.ROLL_INTERVAL_MS || 1000),
    count: Number(env.ROLL_COUNT || 0),
  };
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  const config = readConfig();
  let stopping = false;
  process.on('SIGINT', () => {
    stopping = true;
  });

  const connection = createZerobusConnection({
    host: config.mqttHost,
    tableName: config.tableName,
    getToken: () => fetchZerobusToken(config),
  });
  await connection.waitForConnect();

  for (let i = 0; !stopping && (config.count === 0 || i < config.count); i += 1) {
    const roll = nextRoll(i);
    try {
      await connection.publish(roll);
      console.log(`PUBACK ${roll.player_name} rolled ${roll.roll} on a d${roll.die_sides}`);
    } catch (err) {
      console.log(`publish not acknowledged: ${err.message}`);
    }
    await sleep(config.intervalMs);
  }
  await connection.close();
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
