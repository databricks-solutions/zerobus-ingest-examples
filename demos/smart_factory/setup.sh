#!/bin/bash
set -e

# SmartFactory Demo — One-time setup script
# Usage: ./setup.sh <databricks-cli-profile> <catalog_name> [schema_name] [warehouse_id]
#
# Prerequisites:
#   - Databricks CLI v0.288+ installed (/opt/homebrew/bin/databricks)
#   - Profile configured with workspace access
#   - Node.js + npm installed (for frontend build)

CLI="/opt/homebrew/bin/databricks"
PROFILE="${1:?Usage: ./setup.sh <cli-profile> <catalog_name> [schema_name] [warehouse_id]}"
CATALOG="${2:?Usage: ./setup.sh <cli-profile> <catalog_name> [schema_name] [warehouse_id]}"
SCHEMA="${3:-smartfactory}"
WAREHOUSE_ID="${4:-}"
PIPELINE_NAME="smartfactory-sdp"

echo "========================================="
echo "SmartFactory Demo Setup"
echo "========================================="
echo "Profile:  $PROFILE"
echo "Catalog:  $CATALOG"
echo "Schema:   $SCHEMA"
echo ""

# --- Step 1: Find or start a SQL warehouse ---
echo "[1/8] Selecting SQL warehouse..."
if [ -z "$WAREHOUSE_ID" ]; then
    WAREHOUSE_ID=$($CLI -p "$PROFILE" warehouses list --output json 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for w in data:
    print(w['id'])
    break
" 2>/dev/null)
fi

if [ -z "$WAREHOUSE_ID" ]; then
    echo "ERROR: No SQL warehouse found. Create one in the workspace first."
    exit 1
fi
echo "  Warehouse: $WAREHOUSE_ID"

# Start warehouse if stopped
$CLI -p "$PROFILE" warehouses start "$WAREHOUSE_ID" > /dev/null 2>&1 || true
echo "  Warehouse starting (or already running)..."
sleep 5

# --- Step 2: Create schema and landing table ---
echo "[2/8] Creating schema and landing table..."
run_sql() {
    $CLI -p "$PROFILE" api post /api/2.0/sql/statements --json "{
        \"warehouse_id\": \"$WAREHOUSE_ID\",
        \"statement\": \"$1\",
        \"wait_timeout\": \"30s\"
    }" 2>&1 | python3 -c "import sys,json; d=json.load(sys.stdin); s=d.get('status',{}).get('state','?'); e=d.get('status',{}).get('error',{}).get('message',''); print(f'{s} {e}' if e else s)"
}

run_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}"
run_sql "CREATE TABLE IF NOT EXISTS ${CATALOG}.${SCHEMA}.raw_sensor_events (machine_id STRING NOT NULL, machine_type STRING NOT NULL, sensor_name STRING NOT NULL, value DOUBLE NOT NULL, unit STRING NOT NULL, timestamp TIMESTAMP NOT NULL, is_fault BOOLEAN) USING DELTA COMMENT 'Raw IoT sensor events ingested via ZeroBus'"

# --- Step 3: Build frontend ---
echo "[3/8] Building frontend..."
cd frontend && npm ci --silent && npm run build 2>&1 | tail -1
cd ..

# --- Step 4: Update databricks.yml with correct values ---
echo "[4/8] Configuring bundle..."

# Detect current user for dev schema prefix
WORKSPACE_USER=$($CLI -p "$PROFILE" current-user me --output json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('userName',''))")
SCHEMA_USER=$(printf '%s' "${WORKSPACE_USER%%@*}" | tr '.-' '__')
DEV_SCHEMA="dev_${SCHEMA_USER}_${SCHEMA}"
BUNDLE_SOURCE_PATH="/Workspace/Users/${WORKSPACE_USER}/.bundle/smartfactory-demo/dev/files"

echo "  Dev schema will be: ${CATALOG}.${DEV_SCHEMA}"

render_templates() {
    local pipeline_id="${1:-}"
    CATALOG_VALUE="$CATALOG" \
    LANDING_SCHEMA_VALUE="$SCHEMA" \
    PIPELINE_SCHEMA_VALUE="$DEV_SCHEMA" \
    WAREHOUSE_ID_VALUE="$WAREHOUSE_ID" \
    PIPELINE_ID_VALUE="$pipeline_id" \
    python3 - <<'PY'
import os
from pathlib import Path

values = {
    "__CATALOG__": os.environ["CATALOG_VALUE"],
    "__LANDING_SCHEMA__": os.environ["LANDING_SCHEMA_VALUE"],
    "__PIPELINE_SCHEMA__": os.environ["PIPELINE_SCHEMA_VALUE"],
    "__WAREHOUSE_ID__": os.environ["WAREHOUSE_ID_VALUE"],
    "__PIPELINE_ID__": os.environ["PIPELINE_ID_VALUE"],
}

def render(source, destination):
    content = Path(source).read_text()
    for placeholder, value in values.items():
        content = content.replace(placeholder, value)
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)

render("app.yaml.template", "app.yaml")
render("pipeline/bronze.sql.template", ".generated/pipeline/bronze.sql")
render("dashboards/smartfactory.lvdash.json.template", ".generated/dashboards/smartfactory.lvdash.json")
PY
}

render_templates

# Write a local env file for the setup
cat > .env.setup <<EOF
PROFILE=$PROFILE
CATALOG=$CATALOG
SCHEMA=$SCHEMA
WAREHOUSE_ID=$WAREHOUSE_ID
DEV_SCHEMA=$DEV_SCHEMA
WORKSPACE_USER=$WORKSPACE_USER
EOF

# --- Step 5: Deploy bundle ---
echo "[5/8] Deploying bundle..."
$CLI bundle deploy -t dev -p "$PROFILE" --var="warehouse_id=$WAREHOUSE_ID" --var="catalog_name=$CATALOG" --var="schema_name=$SCHEMA"

# --- Step 6: Get app service principal and grant permissions ---
echo "[6/8] Granting permissions to app service principal..."
SP_ID=$($CLI -p "$PROFILE" apps get smartfactory-app --output json 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
# The SP client ID is in the app's service principal
sp = d.get('service_principal_client_id', d.get('effective_service_principal', {}).get('client_id', ''))
print(sp)
" 2>/dev/null)

if [ -z "$SP_ID" ]; then
    echo "  WARNING: Could not detect app service principal. You may need to:"
    echo "  1. Start the app: $CLI -p $PROFILE apps start smartfactory-app"
    echo "  2. Re-run this script, or manually grant permissions"
else
    echo "  App SP: $SP_ID"

    # Warehouse access
    $CLI -p "$PROFILE" warehouses set-permissions "$WAREHOUSE_ID" --json "{\"access_control_list\":[{\"service_principal_name\":\"$SP_ID\",\"permission_level\":\"CAN_USE\"}]}" > /dev/null 2>&1

    # Catalog + schema + table access
    for stmt in \
        "GRANT USE CATALOG ON CATALOG ${CATALOG} TO \`${SP_ID}\`" \
        "GRANT USE SCHEMA ON SCHEMA ${CATALOG}.${SCHEMA} TO \`${SP_ID}\`" \
        "GRANT CREATE TABLE ON SCHEMA ${CATALOG}.${SCHEMA} TO \`${SP_ID}\`" \
        "GRANT MODIFY ON TABLE ${CATALOG}.${SCHEMA}.raw_sensor_events TO \`${SP_ID}\`" \
        "GRANT SELECT ON TABLE ${CATALOG}.${SCHEMA}.raw_sensor_events TO \`${SP_ID}\`" \
        "GRANT USE SCHEMA ON SCHEMA ${CATALOG}.${DEV_SCHEMA} TO \`${SP_ID}\`" \
        "GRANT SELECT ON SCHEMA ${CATALOG}.${DEV_SCHEMA} TO \`${SP_ID}\`"; do
        run_sql "$stmt" > /dev/null
    done
    echo "  Catalog/schema/table permissions granted"
fi

# --- Step 7: Start app and deploy code ---
echo "[7/8] Starting and deploying app..."
$CLI -p "$PROFILE" apps start smartfactory-app > /dev/null 2>&1 || true
echo "  Waiting for app compute..."
sleep 30

$CLI -p "$PROFILE" apps deploy smartfactory-app \
    --source-code-path "$BUNDLE_SOURCE_PATH" 2>&1 | tail -1

# --- Step 8: Find pipeline ID, grant SP permissions, set continuous ---
echo "[8/8] Configuring pipeline..."
PIPELINE_ID=$($CLI -p "$PROFILE" api get /api/2.0/pipelines 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
for p in d.get('statuses', []):
    if 'smartfactory-sdp' in p.get('name', ''):
        print(p['pipeline_id'])
        break
" 2>/dev/null)

if [ -n "$PIPELINE_ID" ]; then
    echo "  Pipeline: $PIPELINE_ID"

    # Grant SP pipeline permissions
    if [ -n "$SP_ID" ]; then
        $CLI -p "$PROFILE" pipelines update-permissions "$PIPELINE_ID" \
            --json "{\"access_control_list\":[{\"service_principal_name\":\"$SP_ID\",\"permission_level\":\"CAN_MANAGE\"}]}" > /dev/null 2>&1
        echo "  Pipeline permissions granted to SP"
    fi

    # Set continuous mode via API
    PIPELINE_SPEC=$($CLI -p "$PROFILE" api get "/api/2.0/pipelines/$PIPELINE_ID" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
spec = d.get('spec', d)
spec['continuous'] = True
for k in ['pipeline_id','state','creator_user_name','latest_updates','cause','cluster_id','run_as_user_name','last_modified','budget_policy_id']:
    spec.pop(k, None)
print(json.dumps(spec))
")
    echo "$PIPELINE_SPEC" > /tmp/smartfactory_pipeline.json
    $CLI -p "$PROFILE" api put "/api/2.0/pipelines/$PIPELINE_ID" --json @/tmp/smartfactory_pipeline.json > /dev/null 2>&1
    echo "  Pipeline set to continuous mode"

    # Render the discovered pipeline ID into the app configuration and redeploy.
    render_templates "$PIPELINE_ID"
    $CLI -p "$PROFILE" apps deploy smartfactory-app \
        --source-code-path "$BUNDLE_SOURCE_PATH" > /dev/null
    echo "  App configuration updated"
else
    echo "  WARNING: Pipeline not found. Deploy may still be in progress."
fi

# --- Done ---
APP_URL=$($CLI -p "$PROFILE" apps get smartfactory-app --output json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null)

echo ""
echo "========================================="
echo "Setup complete!"
echo "========================================="
echo ""
echo "App URL:     $APP_URL"
echo "Catalog:     $CATALOG"
echo "Schema:      $CATALOG.$SCHEMA (landing)"
echo "Pipeline:    $CATALOG.$DEV_SCHEMA (pipeline tables)"
echo "Warehouse:   $WAREHOUSE_ID"
echo "Pipeline ID: $PIPELINE_ID"
echo ""
echo "Next steps:"
echo "  1. Open the app URL above"
echo "  2. Click 'Start Pipeline' to begin continuous processing"
echo "  3. Inject faults and watch data flow!"
echo ""
echo "To redeploy after code changes:"
echo "  cd frontend && npm run build && cd .."
echo "  $CLI bundle deploy -t dev -p $PROFILE --var='warehouse_id=$WAREHOUSE_ID' --var='catalog_name=$CATALOG' --var='schema_name=$SCHEMA'"
echo "  $CLI -p $PROFILE apps deploy smartfactory-app --source-code-path $BUNDLE_SOURCE_PATH"
