#!/bin/sh
# Creates the Filament objects for the demo: a CLI context for the local server, two PostgreSQL sources
# (one with replication for CDC), the Zerobus sink, and the two pipelines. Run it once, after the stack
# is up and create_table.sql has run. It reads its settings from the environment (see the README), so
# load them first: set -a; . ./.env; set +a
set -eu

: "${DATABRICKS_WORKSPACE_URL:?set DATABRICKS_WORKSPACE_URL (see config.example.env)}"
: "${ZEROBUS_ENDPOINT:?set ZEROBUS_ENDPOINT}"
: "${DATABRICKS_CLIENT_ID:?set DATABRICKS_CLIENT_ID}"
: "${DATABRICKS_CLIENT_SECRET:?set DATABRICKS_CLIENT_SECRET}"
: "${SQL_WAREHOUSE_ID:?set SQL_WAREHOUSE_ID}"
: "${TARGET_CATALOG:?set TARGET_CATALOG}"
: "${TARGET_SCHEMA:?set TARGET_SCHEMA}"
: "${TARGET_CDC_SCHEMA:?set TARGET_CDC_SCHEMA}"
: "${SOURCE_DSN:?set SOURCE_DSN}"

FILAMENT_BIN=${FILAMENT_BIN:-filament}
FILAMENT_SERVER_URL=${FILAMENT_SERVER_URL:-http://127.0.0.1:8080}
TABLE_PREFIX=${TABLE_PREFIX:-}

"$FILAMENT_BIN" context add zerobus-demo --server "$FILAMENT_SERVER_URL"
"$FILAMENT_BIN" context use zerobus-demo
"$FILAMENT_BIN" status

"$FILAMENT_BIN" source create shop --source-connector postgres --source-connection-method url --source-dsn "$SOURCE_DSN"
"$FILAMENT_BIN" source discover shop
"$FILAMENT_BIN" source create shop-cdc --source-connector postgres --source-connection-method url --source-dsn "$SOURCE_DSN" --source-replication cdc

"$FILAMENT_BIN" sink create zb --sink-connector databrickszerobus --sink-workspace-url "$DATABRICKS_WORKSPACE_URL" --sink-zerobus-endpoint "$ZEROBUS_ENDPOINT" --sink-client-id "$DATABRICKS_CLIENT_ID" --sink-client-secret "$DATABRICKS_CLIENT_SECRET" --sink-catalog "$TARGET_CATALOG" --sink-warehouse-id "$SQL_WAREHOUSE_ID"

"$FILAMENT_BIN" pipeline create orders-to-zb --source shop --sink zb --resources "${TABLE_PREFIX}orders" --sync-mode full --write-mode append --sink-schema "$TARGET_SCHEMA"
"$FILAMENT_BIN" pipeline create payments-cdc-to-zb --source shop-cdc --sink zb --resources "${TABLE_PREFIX}payments" --sync-mode cdc --write-mode append --sink-schema "$TARGET_CDC_SCHEMA"

echo "Created: sources shop and shop-cdc, sink zb, pipelines orders-to-zb and payments-cdc-to-zb."
