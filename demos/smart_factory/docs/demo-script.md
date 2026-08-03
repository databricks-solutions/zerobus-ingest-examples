# SmartFactory Demo Guide
## 5-7 Minute Live Demo — ZeroBus + SDP Streaming + Unity Catalog

**Audience**: Manufacturing/IoT decision makers, data leaders, platform engineers
**Goal**: Show how Databricks turns factory sensor data into actionable insights — catch anomalies as they happen, reduce downtime, and govern everything from day one. No Kafka, no infrastructure, one command to deploy.

---

## Before You Start

- [ ] App is open and on the **IoT Simulation** tab
- [ ] **Streaming is ON** — click "Streaming" so data is flowing
- [ ] **Pipeline is ON** — click "Start Pipeline" so SDP is processing
- [ ] **Wait ~30s** for data to flow through all layers
- [ ] Confirm **Operations Dashboard** tab has charts with data
- [ ] Switch back to **IoT Simulation** — this is your starting view
- [ ] Databricks workspace open in another tab (Catalog Explorer + Pipeline ready)
- [ ] All machines showing green/NORMAL — no faults injected yet

---

## Act 1 — Set the Scene

**Where**: IoT Simulation tab — gauges updating, event feed scrolling

**Key points to hit**:
- 3 machines, each with IoT sensors, streaming every 2 seconds
- Data is already flowing into Databricks — point to the live gauges
- Streaming and pipeline indicators in the header show everything is live
- Transition: "Let me show you how this data gets here"

**Personas**: Operations teams, plant floor staff — "the kind of data your operations teams deal with every day"

---

## Act 2 — ZeroBus: Eliminate the Message Bus

**Where**: Expand the ZeroBus info panel on the IoT Simulation tab

**Key points to hit**:
- No message bus. Data pushes directly into governed Delta tables. No staging, no ETL, no waiting.
- Anything with an internet connection and a few lines of code can push data — just like this web app
- What this means: one less system to manage, one less team to hire, faster time to insight
- Bosch: 33% cost savings, 40% faster data transmission
- Joby Aviation: days of telemetry latency to minutes
- Scales effortlessly — thousands of devices, gigabytes per second, no infrastructure to tune

**Personas**: **Platform engineers** (eliminate infra), **IoT developers** (simple SDK), **VP Data** (lower TCO)

---

## Act 3 — The Pipeline (SDP)

**Where**: Pipeline banner at the bottom of the factory floor

**Key points to hit**:
- Walk the flow: Factory Floor → ZeroBus → Bronze → Silver (ML) → Gold → Dashboard
- Continuous pipeline — processes data as it arrives, no batch windows
- Silver layer scores every reading for anomalies — catch equipment issues before they become downtime
- Gold layer aggregates health KPIs for operations teams
- All in SQL. Serverless. No Spark expertise needed.

**Personas**: **Data engineers** (build the pipeline), **SQL analysts** (can modify thresholds)

---

## Act 3b — SDP Deep Dive

**Where**: Switch to Databricks workspace — open the SDP pipeline

**Key points to hit**:
- Show the pipeline DAG: Bronze → Silver → Gold with streaming indicators
- "Three things data engineers love about this":
  1. **Declarative** — SQL says what, Databricks handles how. No Spark code.
  2. **Streaming + batch unified** — same SQL for continuous processing and historical reprocessing
  3. **Fully serverless** — no clusters, auto-scales, pay per use
- ML runs inline in Silver — no separate ML platform, no serving endpoints
- Real-time streaming pipeline — no Flink, no stitching together five different tools
- Tie to business value: catching a bearing failure before it takes down a production line

**Personas**: **Data engineers** (build), **SQL analysts** (contribute without Spark expertise)

---

## Act 4 — Break Something

**Where**: Back to the app — IoT Simulation tab

**Key points to hit**:
- Inject a fault on the CNC Mill — click the fault button
- Gauges show raw sensor data drifting from green → amber → red
- Switch to **Operations Dashboard** — this is what the pipeline produced from that raw data
- Sensor trends spike, health scores drop, anomaly log fills with ML-scored anomalies
- "This isn't a batch report. Your maintenance team responds in minutes, not the next morning."

**Personas**: **Plant managers / reliability engineers** (see the dashboard), **maintenance technicians** (get alerted immediately), **operations analysts** (drill into the anomaly log)

---

## Act 5 — Governance

**Where**: Databricks workspace — Catalog Explorer

**Key points to hit**:
- Every table governed from the moment data lands — no "ingest first, govern later"
- Click a Gold table → show lineage back to the raw sensor landing table
- Role-based access control, audit logs for every query
- "When someone asks 'where did this number come from?' — one click."

**Personas**: **Data governance / compliance** (lineage, audit), **IT security** (RBAC, encryption)

---

## Act 6 — The Close

**Where**: Back to the app

**Key points to hit**:
- Clear the fault — show readings normalize, health recovers
- Rule of three:
  1. **No message bus** — sensor data straight to governed Delta tables. Entire infrastructure layer eliminated.
  2. **ML and analytics in one pipeline, real time** — 30-50% less unplanned downtime when moving from batch to real-time detection
  3. **Governed from day one** — lineage, access control, audit. Compliance covered.
- Closer: "Databricks brings it all together — ingestion, intelligence, and governance — so your teams can focus on outcomes, not infrastructure."

---

## Objection Handling

### "We already have Kafka / Confluent"
It works — but it's a separate system to manage. ZeroBus collapses that layer. Data goes straight to Delta. Fewer moving parts, lower cost. Bosch saw 33% cost savings.

### "Our ML team uses Python models, not SQL thresholds"
This demo uses SQL for simplicity, but the Silver layer can call any MLflow model registered in Unity Catalog — Isolation Forest, XGBoost, neural nets. You can also use `ai_query()` to hit a serving endpoint directly from the pipeline. Same SDP, any model.

### "How does this compare to AWS IoT / Azure IoT Hub?"
Those get data into the cloud. ZeroBus gets data into your lakehouse — directly into governed Delta tables. No intermediate storage, no ETL, no separate governance. Plus you get time travel, schema evolution, and ACID out of the box.

### "What about edge processing?"
ZeroBus handles cloud-side ingestion. For edge, pair it with your existing gateway — Greengrass, IoT Edge, or custom. The ZeroBus SDKs run anywhere your edge compute does.

### "What's the cost?"
$0.05 per GB ingested. A factory streaming 1,000 sensors at 1 reading/sec generates ~2-3 GB/day. That's about $0.15/day for real-time ingestion into governed Delta tables.

### "Is this production-ready?"
ZeroBus is GA as of February 2026. Toyota and Joby Aviation run production IoT workloads on it. SDP is GA and serverless. Unity Catalog governs thousands of production environments.

---

## Power Moves (Extra Time)

### Show UC Lineage Graph
Catalog Explorer → Gold table → Lineage tab → full graph from landing to dashboard.

### Multi-Machine Chaos
Fault all three machines at once. Dashboard becomes a sea of red. Clear them one by one.

### Show the SQL
Open `pipeline/silver.sql` — the anomaly detection is a SQL JOIN. Simplicity is the point.

### Show the Bundle Config
Open `databricks.yml` — entire infrastructure in ~50 lines of YAML.

### Show the Setup Script
`setup.sh` — one script, new workspace, full demo running in 5 minutes.
