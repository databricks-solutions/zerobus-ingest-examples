# Deployment Guide

## Overview

This project uses **Databricks Asset Bundles (DAB)** for complete infrastructure-as-code deployment. The deployment process is fully automated through a single script that:

1. Parses configuration from `config.toml` (source of truth)
2. Deploys all resources via Databricks Asset Bundle
3. Creates Delta tables via Python notebook jobs
4. Grants permissions via Python notebook jobs
5. Deploys the Streamlit app

**Note:** We use Python notebooks instead of SQL files for DDL and GRANT statements to avoid parameter quoting issues with SQL tasks in DAB.

## Architecture

```
config.toml (source of truth)
     ↓
deploy.sh (orchestrator)
     ↓
databricks.yml (bundle definition)
     ↓
Deployed Resources:
  • Streamlit App (data-drifter-regatta)
  • Job: [Setup] Create Tables
  • Job: [Setup] Grant Permissions
  • Job: Sailboat Performance Analysis
```

## Quick Deploy

### One-Command Deployment

```bash
./deploy.sh
```

This single command handles everything:
- ✅ Parses `config.toml`
- ✅ Deploys bundle (apps + jobs)
- ✅ Creates tables (telemetry + weather station)
- ✅ Grants permissions to app service principal
- ✅ Restarts app to apply new configuration
- ✅ Displays deployment URLs

### Environment Variables

Customize deployment with environment variables:

```bash
# Using a different Databricks CLI profile
DATABRICKS_CONFIG_PROFILE=my-profile ./deploy.sh

# Using a different target (dev, prod, etc.)
DATABRICKS_TARGET=prod ./deploy.sh

# Customize both
DATABRICKS_CONFIG_PROFILE=my-profile DATABRICKS_TARGET=prod ./deploy.sh
```

**Defaults:**
- Profile: Uses your default Databricks CLI profile
- Target: `dev`

## What Gets Deployed

### 1. Streamlit App
- **Resource**: `data-drifter-regatta`
- **Source**: `app/` directory
- **SQL Warehouse**: Configured via `config.toml`
- **Permissions**: Managed via bundle

### 2. Setup Jobs

#### Job: [Setup] Create Tables
- Creates `sailboat_telemetry` table (29 fields)
- Creates `weather_station` table (10 fields)
- Uses Python notebooks in `notebooks/` directory (avoids SQL parameter quoting issues)
- Runs automatically during deployment

#### Job: [Setup] Grant Permissions
- Grants USE CATALOG on catalog
- Grants USE SCHEMA on schema
- Grants SELECT on telemetry table (app reads)
- Grants SELECT, MODIFY on weather table (app may write)
- Runs automatically after app creation

### 3. Analysis Job

**Job: Sailboat Performance Analysis**
- 4 notebooks analyzing race performance
- Manual execution: `databricks bundle run sailboat_analysis`

## Configuration

### Source of Truth: `config.toml`

**Key sections:**

```toml
[zerobus]
table_name = "catalog.schema.sailboat_telemetry"
weather_station_table_name = "catalog.schema.weather_station"

[warehouse]
sql_warehouse_id = "your-warehouse-id"
```

**Important:**
- Edit only the root `config.toml`
- `app/config.toml` is auto-generated during deployment
- All variables are extracted and passed to the bundle

### Databricks Asset Bundle: `databricks.yml`

Defines all resources:
- **Apps**: Streamlit app with SQL warehouse resource
- **Jobs**: Table creation, permissions, analysis workflows
- **Variables**: Populated from `config.toml` via `deploy.sh`
- **Sync**: What files get deployed to workspace

## Deployment Process (Detailed)

### Step 1: Parse Configuration

```bash
python3 scripts/parse_config.py
```

Extracts from `config.toml`:
- Table names
- Catalog and schema
- SQL Warehouse ID

### Step 2: Deploy Bundle

```bash
databricks bundle deploy \
  --target dev \
  --var="telemetry_table=$TELEMETRY_TABLE" \
  --var="weather_table=$WEATHER_TABLE" \
  --var="catalog_name=$CATALOG" \
  --var="schema_name=$SCHEMA" \
  --var="warehouse_id=$WAREHOUSE_ID"
```

Creates:
- App resource
- Job resources
- Syncs code to workspace

### Step 3: Run Table Creation

```bash
databricks bundle run create_tables
```

Executes Python notebooks:
- `notebooks/create_telemetry_table.py`
- `notebooks/create_weather_station_table.py`

### Step 4: Grant Permissions

```bash
databricks bundle run grant_permissions \
  --notebook-params="app_service_principal_id=$APP_SP_ID"
```

Executes Python notebook:
- `notebooks/grant_permissions.py`

Grants app service principal access to:
- Catalog (USE CATALOG)
- Schema (USE SCHEMA)
- Tables (SELECT, MODIFY)

## Manual Operations

### View Deployed Resources

```bash
# List all bundle resources
databricks bundle resources --target dev

# View app details
databricks apps get data-drifter-regatta

# View job details
databricks jobs list
```

### Re-run Setup Jobs

```bash
# Re-create tables
databricks bundle run create_tables

# Re-grant permissions
APP_SP_ID=$(databricks apps get data-drifter-regatta --output json | jq -r '.service_principal_client_id')
databricks bundle run grant_permissions --var app_service_principal_id=$APP_SP_ID
```

### Run Analysis

```bash
databricks bundle run sailboat_analysis
```

## Troubleshooting

### Deployment Fails

**Problem:** Warning: "unknown field: app" when running DAB deployment
- **Solution:** Your Databricks CLI version is outdated and doesn't support the `apps` resource type
- **Fix (Homebrew):** `brew upgrade databricks`
- **Fix (Other):** See [Databricks CLI installation guide](https://docs.databricks.com/en/dev-tools/cli/install.html)
- **Verify:** `databricks --version` (should be 0.210.0 or newer)
- **Note:** The `apps` resource was added in Databricks CLI v0.210.0 (January 2024)

**Problem:** `config.toml` parsing error
- **Solution:** Check `[zerobus]` and `[warehouse]` sections are complete
- **Validate:** `python3 scripts/parse_config.py`

**Problem:** Table creation fails
- **Solution:** Check Unity Catalog permissions (need CREATE TABLE)
- **Retry:** `databricks bundle run create_tables`

**Problem:** Permission grant fails
- **Solution:** App may still be initializing, wait 30 seconds
- **Get SP ID:** `databricks apps get data-drifter-regatta --output json | jq -r '.service_principal_client_id'`
- **Retry:** `databricks bundle run grant_permissions --var app_service_principal_id=<SP_ID>`

### App Issues

**Problem:** App not accessible
- **Solution:** Check app is running: `databricks apps get data-drifter-regatta`
- **Check logs:** Look for startup errors in app logs

**Problem:** App has no data
- **Solution:** Start telemetry generator: `python3 main.py`
- **Verify tables:** Check tables have data

## Rollback

If deployment fails, rollback to previous approach:

```bash
# Restore old deployment scripts
cp tmp_old_scripts/deploy.sh.old deploy.sh
cp tmp_old_scripts/*.py scripts/

# Run old deployment
./deploy.sh
```

See `tmp_old_scripts/README.md` for detailed rollback instructions.

## File Structure

```
data_drifter/
├── config.toml                    # Source of truth (user edits this)
├── databricks.yml                 # Bundle definition
├── deploy.sh                      # Deployment orchestrator
│
├── scripts/
│   └── parse_config.py                 # Extracts variables from config.toml
│
├── app/
│   ├── app.py                          # Streamlit application
│   └── config.toml                     # Auto-generated from root config.toml
│
├── notebooks/
│   ├── create_telemetry_table.py      # Setup: Create telemetry table
│   ├── create_weather_station_table.py # Setup: Create weather table
│   ├── grant_permissions.py            # Setup: Grant Unity Catalog permissions
│   ├── 01_boat_performance.py          # Analysis: Boat performance
│   ├── 02_wind_conditions.py           # Analysis: Wind conditions
│   ├── 03_race_progress.py             # Analysis: Race progress
│   └── 04_race_summary.py              # Analysis: Race summary
│
└── src/                          # Telemetry generator (runs on laptop)
    └── ...
```

## Key Principles

### 1. Single Source of Truth
- `config.toml` contains all user configuration
- No hardcoded values in bundle or scripts
- Easy to customize per environment

### 2. Infrastructure as Code
- All resources defined in `databricks.yml`
- SQL files for table schemas and permissions
- Version controlled, reviewable, repeatable

### 3. Automated Setup
- Tables created automatically
- Permissions granted automatically
- No manual SQL execution required

### 4. Idempotent Operations
- SQL uses `CREATE TABLE IF NOT EXISTS`
- GRANT statements are idempotent
- Safe to re-run deployment

### 5. Rollback Ready
- Old scripts preserved in `tmp_old_scripts/`
- Can revert at any time
- 30-day safety period before cleanup

## Best Practices

1. **Always test in dev first**
   ```bash
   DATABRICKS_TARGET=dev ./deploy.sh
   ```

2. **Validate config before deploy**
   ```bash
   python3 scripts/parse_config.py
   ```

3. **Check bundle syntax**
   ```bash
   databricks bundle validate
   ```

4. **Review changes before deploy**
   ```bash
   databricks bundle deploy --dry-run --target dev
   ```

5. **Monitor deployment**
   ```bash
   databricks bundle deploy --verbose --target dev
   ```

## Next Steps

After successful deployment:

1. **Start telemetry generator:**
   ```bash
   python3 main.py
   ```

2. **Open app in browser** (URL from deployment output)

3. **Run analysis after race:**
   ```bash
   databricks bundle run sailboat_analysis
   ```

---

For more details:
- [README.md](README.md) - Quick start guide
- [databricks.yml](databricks.yml) - Bundle definition
- [notebooks/](notebooks/) - Python notebooks for setup and analysis
- [tmp_old_scripts/README.md](tmp_old_scripts/README.md) - Rollback guide
