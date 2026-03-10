#!/bin/bash
set -e

export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
TARGET="${DATABRICKS_TARGET:-dev}"
APP_NAME="data-drifter-regatta-v3"

echo "🚀 Deploying Data Drifter Regatta (Profile: $DATABRICKS_CONFIG_PROFILE, Target: $TARGET)"
echo ""

# Copy config.toml to app folder for deployment
# This ensures the app can access config.toml in its working directory
echo "📋 Copying config.toml to app folder..."
cp config.toml app/config.toml
if [ $? -eq 0 ]; then
    echo "✅ Config copied to app/"
else
    echo "❌ Failed to copy config.toml"
    exit 1
fi

# Parse config.toml for notebook parameters only
# The app reads config.toml directly, but notebooks need parameters
echo "📋 Parsing config for notebook parameters..."
if ! eval "$(python3 scripts/parse_config.py)" > /dev/null 2>&1; then
    echo "❌ Failed to parse config.toml"
    exit 1
fi
echo "✅ Config parsed"

# Deploy bundle
# Only pass variables needed by notebooks (not by the app)
echo "📦 Deploying bundle..."

databricks bundle deploy \
  --target "$TARGET" \
  --profile="$DATABRICKS_CONFIG_PROFILE" \
  --var="telemetry_table=$TELEMETRY_TABLE" \
  --var="weather_table=$WEATHER_TABLE" \
  --var="catalog_name=$CATALOG_NAME" \
  --var="schema_name=$SCHEMA_NAME" \
  --var="sql_warehouse_id=$WAREHOUSE_ID"

echo "✅ Bundle deployed"

# Create tables
echo "🏗️  Creating tables..."
if databricks bundle run create_tables --target "$TARGET" --profile="$DATABRICKS_CONFIG_PROFILE"; then
    echo "✅ Tables created"
else
    echo "❌ Table creation failed"
    exit 1
fi


# Deploy app
echo "📱 Deploying app..."
CURRENT_USER=$(databricks current-user me --profile="$DATABRICKS_CONFIG_PROFILE" --output json 2>/dev/null | jq -r '.userName')
BUNDLE_NAME="data_drifter_regatta"
APP_SOURCE_PATH="/Workspace/Users/$CURRENT_USER/.bundle/$BUNDLE_NAME/$TARGET/files/app"

if databricks apps get "$APP_NAME" --profile="$DATABRICKS_CONFIG_PROFILE" --output json 2>/dev/null | jq -e '.name' > /dev/null; then
    # Wait for any active deployment to complete
    WAIT_COUNT=0
    while [ $WAIT_COUNT -lt 12 ]; do
        DEPLOYMENT_STATE=$(databricks apps get "$APP_NAME" --profile="$DATABRICKS_CONFIG_PROFILE" --output json 2>/dev/null | jq -r '.active_deployment.deployment_state // "NONE"')
        if [ "$DEPLOYMENT_STATE" = "NONE" ] || [ "$DEPLOYMENT_STATE" = "null" ]; then
            break
        fi
        echo "⏳ Waiting for active deployment to complete... ($((WAIT_COUNT * 5))s)"
        sleep 5
        WAIT_COUNT=$((WAIT_COUNT + 1))
    done

    COMPUTE_STATUS=$(databricks apps get "$APP_NAME" --profile="$DATABRICKS_CONFIG_PROFILE" --output json 2>/dev/null | jq -r '.compute_status.state')

    if [ "$COMPUTE_STATUS" = "ACTIVE" ] || [ "$COMPUTE_STATUS" = "STARTING" ]; then
        echo "Updating running app..."
        databricks apps deploy "$APP_NAME" --source-code-path "$APP_SOURCE_PATH" --profile="$DATABRICKS_CONFIG_PROFILE"
    else
        echo "Deploying and starting app..."
        databricks apps deploy "$APP_NAME" --source-code-path "$APP_SOURCE_PATH" --profile="$DATABRICKS_CONFIG_PROFILE"
        databricks apps start "$APP_NAME" --profile="$DATABRICKS_CONFIG_PROFILE"
    fi
else
    echo "Creating new app..."
    databricks apps deploy "$APP_NAME" --source-code-path "$APP_SOURCE_PATH" --profile="$DATABRICKS_CONFIG_PROFILE"
    databricks apps start "$APP_NAME" --profile="$DATABRICKS_CONFIG_PROFILE"
fi
echo "✅ App deployed"


# Grant permissions
echo "🔐 Granting permissions..."
sleep 2

if APP_SP_ID=$(databricks apps get "$APP_NAME" --profile="$DATABRICKS_CONFIG_PROFILE" --output json 2>/dev/null | jq -r '.service_principal_client_id'); then
    if databricks bundle run grant_permissions --notebook-params="app_service_principal_id=$APP_SP_ID" --profile="$DATABRICKS_CONFIG_PROFILE"; then
        echo "✅ Permissions granted"
    else
        echo "⚠️  Permission grant failed"
    fi
else
    echo "⚠️  Skipping permissions grant (no service principal ID)"
fi


# Summary
echo ""
echo "✅ Deployment complete!"
echo ""
if APP_URL=$(databricks apps get "$APP_NAME" --profile="$DATABRICKS_CONFIG_PROFILE" --output json 2>/dev/null | jq -r '.url'); then
    echo "App URL: $APP_URL"
fi
echo ""
echo "Next: python3 main.py (start telemetry generator)"
