# 🌊 DATA DRIFTER REGATTA ⛵

**Real-time sailboat race tracking powered by Databricks Zerobus Ingest**

Simulates a fleet of sailboats racing and visualizes their positions in real-time using a Databricks App. Features multi-protocol data ingestion (SDK via gRPC for telemetry, REST API for weather station), real-time visualization, and post-race analysis notebooks.

---

## 🚀 Quick Start

**Get from zero to running in 5 minutes!**

### Prerequisites

- Python 3.8+
- Databricks workspace with Apps enabled
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) installed
- Unity Catalog access with permission to create tables

### Step 1: Clone and Install Dependencies

```bash
git clone <repository-url>
cd data_drifter

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configure Databricks CLI

```bash
# Configure authentication (if not already done)
databricks configure --token

# You'll be prompted for:
# - Databricks Host: https://your-workspace.cloud.databricks.com
# - Token: (generate from User Settings → Developer → Access Tokens)
```

### Step 3: Configure Your Deployment

Edit `config.toml` with your configuration:

```toml
[zerobus]
# Zerobus Ingest endpoint
server_endpoint = "your-zerobus-endpoint.cloud.databricks.com"
workspace_url = "https://your-workspace.cloud.databricks.com"

# Unity Catalog table names (catalog.schema.table format)
table_name = "your_catalog.your_schema.sailboat_telemetry"
weather_station_table_name = "your_catalog.your_schema.weather_station"

[warehouse]
# SQL Warehouse ID (get from: databricks sql warehouses list)
sql_warehouse_id = "your-warehouse-id"
```

**Important:** The `config.toml` file at the project root is your single source of truth. During deployment, `deploy.sh` automatically copies it to the `app/` folder. **Never modify `app/config.toml` directly** - always edit the root-level `config.toml` and redeploy.

### Step 4: Deploy Everything with One Command

```bash
./deploy.sh
```

**What this does automatically:**
1. ✅ Copies `config.toml` to `app/` folder (for app access)
2. ✅ Parses your `config.toml` for notebook parameters
3. ✅ Deploys Databricks Asset Bundle (apps + jobs)
4. ✅ Creates Delta tables (telemetry + weather station)
5. ✅ Grants all permissions to app service principal
6. ✅ Deploys Streamlit app with config
7. ✅ Sets up analysis jobs

**Output:**
```
═══════════════════════════════════════════════════════════════════
  ✅ Deployment Complete!
═══════════════════════════════════════════════════════════════════

📱 Streamlit App:
   URL: https://your-app-url.cloud.databricks.com

📊 Analysis Job:
   Run with: databricks bundle run sailboat_analysis

🚀 Next Steps:
   1. Start telemetry generator: python3 main.py --client-id <id> --client-secret <secret>
   2. Open app in browser (URL above)
```

### Step 5: Start the Race!

```bash
# Generate sailboat telemetry (pass OAuth credentials as CLI arguments)
python3 main.py \
  --client-id your-service-principal-client-id \
  --client-secret your-service-principal-secret
```

**You should see:**
```
🌊 Starting Data Drifter Regatta Telemetry Generator 🌊
⛵ Fleet: 6 boats
📊 Telemetry Table: your_catalog.your_schema.sailboat_telemetry
🌤️  Weather Station: your_catalog.your_schema.weather_station
...
✓ Sent 60 records via SDK (gRPC)
✓ Weather emitted via REST API (frontal_passage event)
```

### Step 6: View the Race

Open the app URL from deployment output in your browser. You'll see:
- 🗺️ **Live Map** - Real-time boat positions with race course
- 🏆 **Leaderboard** - Current rankings and statistics
- 🌤️ **Captain's Weather Watch** - Live weather conditions from the IoT weather station

---

## 📊 Post-Race Analysis

After the race, run the analysis notebooks:

```bash
databricks bundle run sailboat_analysis
```

This runs 4 notebooks in sequence:
1. **Boat Performance** - Rankings, completion times, VMG analysis, weather event performance
2. **Wind Conditions** - Performance by wind conditions, point of sail analysis, weather event timeline
3. **Race Progress** - Position changes throughout race, mark rounding analysis
4. **Race Summary** - Executive summary with key insights

---

## 🏗️ Architecture

### Data Flow

```
Sailboat Fleet (20 boats)          Weather Station (IoT device)
        ↓ (gRPC/SDK)                        ↓ (REST API)
        └──────────→  Zerobus Ingest  ←─────────┘
                            ↓
                    Delta Lake Tables
                   (Unity Catalog)
                            ↓
                ┌───────────┴───────────┐
                ↓                       ↓
         Streamlit App          Analysis Notebooks
      (Real-time viz)           (Post-race insights)
```

### Key Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Telemetry Generator** | Python (runs on laptop) | Simulates fleet of sailboats racing (configurable size) |
| **Weather Station** | Python REST API | Simulates IoT weather station (event-driven) |
| **Ingestion** | Zerobus Ingest | Multi-protocol endpoint (gRPC + REST) |
| **Storage** | Delta Lake + Unity Catalog | ACID-compliant table storage with governance |
| **App** | Databricks Apps (Streamlit) | Real-time race visualization |
| **Analysis** | Databricks Notebooks | Post-race performance analysis |
| **Deployment** | Databricks Asset Bundles | Infrastructure as code |

---

## 🔧 Advanced Usage

### Customize Race Configuration

Edit `config.toml` to change:
- Fleet size (`[fleet] num_boats`)
- Race course marks (`[race_course] marks`)
- Wind conditions (`[weather]` section)
- Race duration and playback speed (`[telemetry]` section)

See inline comments in `config.toml` for all options.

### Manual Table Creation (Optional)

Tables are created automatically by `deploy.sh`, but if needed:

```bash
databricks bundle run create_tables
```

### Manual Permissions Grant (Optional)

Permissions are granted automatically, but if needed:

```bash
# Get app service principal ID
APP_SP_ID=$(databricks apps get data-drifter-regatta --output json | jq -r '.service_principal_client_id')

# Grant permissions
databricks bundle run grant_permissions --var app_service_principal_id=$APP_SP_ID
```

### View Deployed Resources

```bash
# List all bundle resources
databricks bundle resources --target dev

# View app details
databricks apps get data-drifter-regatta

# View jobs
databricks jobs list
```

---

## 🛠️ Troubleshooting

### Deployment Issues

**Problem:** Warning: "unknown field: app" when running DAB deployment
- **Solution:** Your Databricks CLI version is outdated and doesn't support the `apps` resource type
- **Fix (Homebrew):** `brew upgrade databricks`
- **Fix (Other):** See [Databricks CLI installation guide](https://docs.databricks.com/en/dev-tools/cli/install.html)
- **Verify:** `databricks --version` (should be 0.210.0 or newer)

**Problem:** `deploy.sh` fails with "table_name not found"
- **Solution:** Check `[zerobus]` section in `config.toml` - ensure `table_name` and `weather_station_table_name` are set

**Problem:** Permission errors during deployment
- **Solution:** Ensure you have CREATE TABLE permissions in Unity Catalog

**Problem:** App service principal ID not found
- **Solution:** Wait a few moments for app to initialize, then re-run:
  ```bash
  databricks bundle run grant_permissions --var app_service_principal_id=<SP_ID>
  ```

### Telemetry Generator Issues

**Problem:** Missing required argument: --client-id or --client-secret
- **Solution:** Pass OAuth credentials as command-line arguments:
  ```bash
  python3 main.py --client-id <your-client-id> --client-secret <your-secret>
  ```

**Problem:** OAuth authentication fails
- **Solution:** Verify your `client_id` and `client_secret` are correct
- **Solution:** Ensure service principal has `SELECT` and `MODIFY` permissions on tables

**Problem:** Connection timeout
- **Solution:** Check `server_endpoint` and `workspace_url` in `config.toml`

### App Issues

**Problem:** App doesn't show data
- **Solution:** Ensure telemetry generator is running and sending data
- **Solution:** Check app has SELECT permissions on tables

**Problem:** App shows "No data"
- **Solution:** Wait 10-30 seconds for initial data to arrive
- **Solution:** Check tables have data: `SELECT COUNT(*) FROM your_table`

## 🎯 Key Features

### Multi-Protocol Ingestion
- ✅ **SDK (gRPC)** - High-throughput telemetry from fleet (continuous streaming)
- ✅ **REST API** - Event-driven weather station data (only when conditions change)

### Automatic Optimization
- ✅ Schema inference and evolution
- ✅ Write optimization (batching, file sizing)
- ✅ At-least-once delivery semantics
- ✅ Automatic retries with backoff

### Unity Catalog Governance
- ✅ Table-level ACLs enforced
- ✅ Audit logging for all access
- ✅ Data lineage automatically tracked
- ✅ Change Data Feed enabled

### Developer Experience
- ✅ Single command deployment (`./deploy.sh`)
- ✅ Config-driven setup (`config.toml` is source of truth)
- ✅ Infrastructure as code (Databricks Asset Bundles)
- ✅ Zero infrastructure to manage

---

## 🤝 Contributing

This is a demonstration project showcasing Databricks Zerobus Ingest capabilities. For issues or questions:

1. Check the troubleshooting section above
2. Review the documentation in `docs/`
3. Check Databricks Asset Bundle logs: `databricks bundle deploy --verbose`

---

**Built with:**
- Databricks Zerobus Ingest (multi-protocol streaming)
- Databricks Apps (Streamlit hosting)
- Delta Lake + Unity Catalog (storage + governance)
- Databricks Asset Bundles (infrastructure as code)
- Python 3.11+ (telemetry generation)

---

*Zerobus Ingest: Streaming made simple. Focus on your data, not your infrastructure.*
