#!/bin/sh
# Start Node-RED with the example flow. Run from the example folder, after filling in config.env.
# Node-RED writes runtime files next to flows.json, so it runs from a copy in ./data.
# The editor is published on the host's loopback only (127.0.0.1), so open it at http://127.0.0.1:${PORT}.
# Do not add --network host: with host networking the -p publish is ignored.
set -eu
set -a; . ./config.env; set +a
mkdir -p data
cp node-red/flows.json node-red/settings.js data/
docker run -d --name zerobus-node-red \
  -e PORT -e DATABRICKS_WORKSPACE_URL -e DATABRICKS_WORKSPACE_ID \
  -e DATABRICKS_CLIENT_ID -e DATABRICKS_CLIENT_SECRET \
  -e DATABRICKS_TABLE_NAME -e ZEROBUS_MQTT_HOST \
  -p "127.0.0.1:${PORT}:${PORT}" \
  -v "$(pwd)/data:/data" \
  nodered/node-red:latest
