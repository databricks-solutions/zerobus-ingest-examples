# SmartFactory — IoT Streaming Demo

A customer-facing demo showing how Databricks turns factory sensor data into actionable insights — from machine floor to decision — with no Kafka, no infrastructure to manage, and full governance out of the box.

## What This Demonstrates

| Capability | Business Value |
|---|---|
| **ZeroBus Ingest** | Eliminate Kafka and message bus infrastructure. Sensor data flows directly into governed Delta tables. |
| **SDP Streaming Pipeline** | Catch equipment anomalies as they happen, not in tomorrow's batch report. Continuous, serverless, pure SQL. |
| **ML in the Pipeline** | Every sensor reading scored for anomalies inline — predictive maintenance without a separate ML platform. |
| **Live Operations Dashboard** | Plant managers and technicians see machine health the moment it changes. Faster response, less downtime. |
| **Unity Catalog Governance** | Every table governed from the first byte. Lineage, access control, audit — ready for compliance on day one. |
| **Databricks Apps** | Full-stack app deployed and managed by Databricks. No separate hosting. |
| **DABs** | Entire demo — app, pipeline, dashboard — deploys with a single command. |

## Architecture

![Architecture](docs/architecture.png)

## The Machines

| Machine | Sensors | Fault Scenario |
|---|---|---|
| **CNC Mill** | Temperature, Vibration, Spindle RPM | Overheating, vibration spike |
| **Hydraulic Press** | Pressure, Temperature, Cycle Count | Pressure surge, cycle slowdown |
| **Conveyor Belt** | Belt Speed, Load Weight, Motor Current | Speed drop, overcurrent |

## Demo Story (6 minutes)

> **Pre-flight**: Start streaming + pipeline 30s before presenting. Confirm dashboard has data.

### Act 1 — "This is your factory" (IoT Simulation tab)
> "3 machines, IoT sensors streaming every 2 seconds, directly into Databricks. No Kafka."

- Gauges are already updating, event feed scrolling
- Point out "Streaming" and "Pipeline Running" in the header
- Expand ZeroBus info panel — highlight ≤200ms ack, 10 GB/s, Joby Aviation quote

### Act 2 — "Here's the pipeline" (SDP in Databricks UI)
> "Declarative SQL. Streaming and batch in one pipeline. Fully serverless."

- Switch to Databricks workspace, open the SDP pipeline DAG
- Show Bronze → Silver → Gold with streaming indicators
- Click into Silver SQL — "anomaly detection is a SQL JOIN. Any SQL developer can own this."
- Three SDP benefits: declarative, streaming+batch unified, serverless

### Act 3 — "Let's break something" (Inject a fault)
> "Watch what happens when the CNC Mill starts overheating."

- Click **Fault: CNC Mill** — watch temperature climb, gauges go red
- Event feed lights up with warnings and criticals
- Switch to **Operations Dashboard** — health scores dropping, anomaly log filling

### Act 4 — "Everything is governed" (Unity Catalog)
> "Every table governed. Full lineage from raw sensor event to dashboard."

- Open Catalog Explorer, click Gold table → show lineage graph
- "One command to deploy. No Kafka. No ML infrastructure. Just push and go."

### Act 5 — "Clear the fault" (Resolution)
- Click **Clear All** — readings normalize, health scores recover

See [docs/demo-script.md](docs/demo-script.md) for the full script with talking points and objection handling.

## Quick Start

### Prerequisites
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) v0.288+ installed
- CLI profile configured for your target workspace
- Node.js 18+ and npm installed
- An existing Unity Catalog catalog with cloud storage configured

### One-command setup
```bash
git clone <repo-url>
cd smartfactory-demo
./setup.sh <databricks-cli-profile> <catalog_name>
```

This script handles everything:
1. Finds and starts a SQL warehouse
2. Creates the `smartfactory` schema and landing table
3. Builds the React frontend
4. Deploys all resources via DABs (app, pipeline, dashboard)
5. Detects the app service principal and grants all permissions
6. Starts the app and deploys code
7. Sets the SDP pipeline to continuous mode

### After setup
1. Open the app URL printed by the script
2. Click **Streaming** in the header to start the data simulator
3. Click **Start Pipeline** to begin continuous SDP processing
4. Switch between **IoT Simulation** and **Operations Dashboard** tabs
5. Inject faults and watch anomalies flow through the pipeline

> **Important: When you're done demoing, stop the streaming and pipeline!**
> Both consume compute resources. Click **Streaming** (to pause) and **Pipeline Running** (to stop) in the header bar.
> The simulator and pipeline start paused by default — you must manually start them each demo session.

### Redeploying after code changes
```bash
cd frontend && npm run build && cd ..
databricks bundle deploy -t dev
databricks apps deploy smartfactory-app \
  --source-code-path /Workspace/Users/<you>/.bundle/smartfactory-demo/dev/files
```

## Project Structure

```
smartfactory-demo/
├── setup.sh                  # One-command setup script
├── databricks.yml            # DABs bundle (app + pipeline + dashboard)
├── app.yaml                  # Databricks App config
├── app.py                    # FastAPI backend (WebSocket + REST + pipeline control)
├── simulator.py              # 3-machine IoT sensor simulator with fault injection
├── zerobus_client.py         # ZeroBus SDK wrapper with SQL INSERT fallback
├── pipeline/
│   ├── bronze.sql            # Validated ingestion (streaming table)
│   ├── silver.sql            # Anomaly scoring via threshold JOIN (streaming table)
│   └── gold.sql              # Health KPIs + anomaly timeline (materialized views)
├── frontend/
│   ├── src/
│   │   ├── App.tsx           # Tabbed layout (IoT Simulation + Dashboard)
│   │   ├── components/
│   │   │   ├── FactoryFloor  # SVG machine visuals with live sensor readouts
│   │   │   ├── MachineCard   # Per-machine gauge cards
│   │   │   ├── SensorGauge   # Circular SVG gauge component
│   │   │   ├── ControlPanel  # Fault injection buttons
│   │   │   ├── EventFeed     # Live scrolling event log
│   │   │   ├── DashboardView # Charts, KPI tables, anomaly log
│   │   │   └── PipelineBanner# Pipeline flow + UC governance badge
│   │   └── hooks/
│   │       └── useWebSocket  # Auto-reconnecting WebSocket hook
│   └── dist/                 # Pre-built frontend (deployed with app)
├── dashboard.lvdash.json     # Lakeview dashboard definition
└── CLAUDE.md                 # Development notes and known issues
```

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React 18, TypeScript, Vite, TailwindCSS, Recharts |
| Backend | FastAPI, Uvicorn, WebSocket |
| Ingestion | ZeroBus SDK (with SQL INSERT fallback) |
| Pipeline | SDP (Spark Declarative Pipelines), serverless |
| ML | SQL threshold-based anomaly detection in SDP Silver layer |
| Governance | Unity Catalog (lineage, access control, audit) |
| Deployment | Databricks Asset Bundles (DABs) |
| Dashboard | Lakeview (AI/BI) + in-app React dashboard |
