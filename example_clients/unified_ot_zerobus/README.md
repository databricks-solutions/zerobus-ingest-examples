# Unified OT Zerobus Connector Client

This tree is included as a **Zerobus Ingest example client** in [databricks-solutions/zerobus-ingest-examples](https://github.com/databricks-solutions/zerobus-ingest-examples). The upstream project lives at [pravinva/unified-ot-zerobus-connector](https://github.com/pravinva/unified-ot-zerobus-connector) (`unified_connector/`). For the latest changes, prefer that repository or submit improvements there and refresh this copy as needed.

---

Enterprise-grade connector for streaming OT/IoT data from industrial protocols (OPC-UA, MQTT, Modbus) to Databricks Delta tables using ZeroBus.

## Features

### Protocol Support
- **OPC-UA**: Auto-discovery, polling/subscription modes, tag normalization, OPC UA 10101 security
- **MQTT**: Sparkplug B support, QoS levels, TLS/SSL, topic filtering
- **Modbus TCP/RTU**: Register mapping, data type conversion, multiple slave support

### Data Processing
- **Tag Normalization**: ISA-95/Purdue Model hierarchical path building
- **Quality Mapping**: Protocol-specific quality codes to unified quality levels
- **Data Type Inference**: Automatic type detection and conversion
- **Metadata Enrichment**: Site/area/line/equipment context
- **Numeric Edge Compression**: NONE / CHANGE_ONLY / DEADBAND / SDT (Swinging Door Trending)
- **Per-Source Overrides**: Source-level compression policy and tag regex rules

### ZeroBus Integration
- **Per-Source Targets**: Route different sources to different Delta tables
- **Batch Optimization**: Configurable batch size and timeout
- **Auto-Reconnection**: Exponential backoff with jitter
- **Credential Encryption**: Fernet AES-256-CBC for OAuth2 secrets

### Resilience
- **Backpressure Management**: Memory queue + disk spooling + DLQ
- **Auto-Reconnection**: Protocol clients automatically reconnect on failure
- **Health Monitoring**: Prometheus metrics, health check endpoints
- **Circuit Breaker**: Protects ZeroBus from overload

### Management
- **Web UI**: Configuration, monitoring, discovery management
- **REST API**: Programmatic control
- **Protocol Discovery**: Automatic network scanning for OPC-UA/MQTT/Modbus servers

### Diagnostics & Observability
- **Vendor Format Detection**: Automatic identification of Kepware, Sparkplug B, Honeywell, and generic formats
- **Pipeline Visibility**: Visual representation of data transformation stages
- **OPC UA Browse Paths**: Full hierarchical path capture for vendor-specific structures
- **Per-Vendor Metrics**: Track message counts by detected vendor format
- **Enhanced Web UI**: Real-time dashboards with vendor breakdown and pipeline stats

## Diagnostics & Pipeline Visibility

### Vendor Format Detection

The connector automatically detects and tracks vendor-specific data formats in real-time:

**Supported Vendor Formats:**
- **Kepware**: Channel.Device.Tag structure (e.g., `Siemens_S7_Crushing.Crusher_01.MotorPower`)
- **Sparkplug B**: MQTT topics with NBIRTH/DBIRTH/NDATA/DDATA, bdSeq fields
- **Honeywell Experion**: Composite points (.PV/.SP/.OP/.MODE), FIM/AIM/PID modules
- **Generic**: Simple path-based structures
- **Unknown**: Unrecognized formats

**Detection Metrics (REST API):**
```bash
curl http://localhost:8001/api/metrics | jq '.bridge.vendor_formats'
{
  "kepware": 1250,
  "sparkplug_b": 847,
  "honeywell": 503,
  "generic": 11451,
  "unknown": 0
}
```

### Data Transformation Pipeline

The connector transforms industrial protocol messages through multiple stages:

**Without Normalization (Vendor Format Preserved):**
```
1. Protocol Source (Kepware/Sparkplug B/Honeywell)
   ↓
2. Protocol Client (OPC UA/MQTT/Modbus)
   ↓
3. ProtocolRecord (vendor-specific format)
   ↓
4. Vendor Format Detection (adds vendor_format tag)
   ↓
5. record.to_dict() (converts to dictionary)
   ↓
6. Backpressure Queue (memory → disk spool if needed)
   ↓
7. ZeroBus Batcher (max 1000 records or 5sec timeout)
   ↓
8. Delta Table (Databricks)
```

**With ISA-95 Normalization (Enabled):**
```
1. Protocol Source
   ↓
2. Protocol Client
   ↓
3. ProtocolRecord
   ↓
4. Vendor Format Detection
   ↓
5. ISA-95 Normalizer ← STANDARDIZATION HAPPENS HERE
   ├─ Extracts site_id, line_id, equipment_id, signal_type
   ├─ Builds normalized path: "{site_id}/{line_id}/{equipment_id}/{signal_type}"
   ├─ Maps quality codes to standard values
   ├─ Generates tag_id hash
   └─ Adds engineering units metadata
   ↓
6. Normalized Tag (ISA-95 compliant)
   ↓
7. Backpressure Queue
   ↓
8. ZeroBus Batcher
   ↓
9. Delta Table
```

### OPC UA Browse Path Enhancement

**Full Hierarchical Path Capture:**

The OPC UA client now captures complete browse paths during node discovery, enabling proper vendor format detection:

**Implementation** (`unified_connector/protocols/opcua_client.py`):
- Added `_node_paths` dictionary to store node_id → full_browse_path mappings
- Modified `_discover_variables()` to store complete hierarchical paths
- Updated `subscribe()` and `_poll_loop()` to use stored browse paths

**Example Paths:**
- Kepware: `VendorModes/Kepware/Siemens_S7_Crushing/Crusher_01/CrusherMotorPower`
- Honeywell: `VendorModes/Honeywell/MINE_A_EXPERION_PKS/FIM_01/CRUSHER_01_POWER/PV`
- Sparkplug B: `VendorModes/Sparkplug B/DatabricksDemo/OTSimulator01/MiningAssets/mining_crusher_1_motor_power`
- Generic: `IndustrialSensors/Agriculture/barn_ventilation_fan_speed`

This enables the connector to accurately detect vendor formats based on path patterns rather than just leaf node names.

### Web UI Enhancements

**Enhanced Diagnostics Dashboard:**

The Web UI (http://localhost:8001) now includes:

1. **Vendor Format Breakdown**
   - Real-time counts per vendor format
   - Percentage distribution pie chart
   - Sample paths per format

2. **Pipeline Performance Metrics**
   - Messages/sec at each stage
   - Queue depths (memory/disk)
   - Backpressure events
   - ZeroBus batch sizes

3. **Protocol-Specific Stats**
   - OPC UA: Browse path distribution, node counts by vendor structure
   - MQTT: Topic hierarchy analysis, QoS distribution
   - Modbus: Register address utilization

4. **Data Quality Indicators**
   - Quality code distribution per protocol
   - Normalization success rate
   - Data type inference accuracy

**API Endpoints:**
```bash
# Vendor format metrics
GET /api/metrics

# Sample records per vendor format
GET /api/vendor-samples?format=kepware&limit=10

# Pipeline stage statistics
GET /api/pipeline-stats

# Recent records with vendor detection
GET /api/records?limit=100
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Unified OT Zerobus Connector Client                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │   OPC-UA     │  │     MQTT     │  │    Modbus    │        │
│  │   Client     │  │   Client     │  │    Client    │        │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘        │
│         │                  │                  │                 │
│         └──────────────────┴──────────────────┘                │
│                           │                                     │
│                  ┌────────▼────────┐                           │
│                  │ Tag Normalization│                           │
│                  │  (ISA-95 Paths)  │                           │
│                  └────────┬────────┘                           │
│                           │                                     │
│                  ┌────────▼────────┐                           │
│                  │   Backpressure  │                           │
│                  │ Memory + Disk   │                           │
│                  └────────┬────────┘                           │
│                           │                                     │
│                  ┌────────▼────────┐                           │
│                  │  Batch Processor │                           │
│                  │  (per target)    │                           │
│                  └────────┬────────┘                           │
│                           │                                     │
│         ┌─────────────────┼─────────────────┐                 │
│         │                 │                 │                  │
│   ┌─────▼──────┐   ┌─────▼──────┐   ┌─────▼──────┐          │
│   │  ZeroBus   │   │  ZeroBus   │   │  ZeroBus   │          │
│   │  Target 1  │   │  Target 2  │   │  Target 3  │          │
│   └─────┬──────┘   └─────┬──────┘   └─────┬──────┘          │
│         │                 │                 │                  │
└─────────┼─────────────────┼─────────────────┼─────────────────┘
          │                 │                 │
          ▼                 ▼                 ▼
    ┌──────────┐      ┌──────────┐      ┌──────────┐
    │ Databricks│      │ Databricks│      │ Databricks│
    │ Catalog 1 │      │ Catalog 2 │      │ Catalog 3 │
    │ Delta     │      │ Delta     │      │ Delta     │
    │ Table     │      │ Table     │      │ Table     │
    └──────────┘      └──────────┘      └──────────┘
```

## Quick Start

### 1. Prerequisites

- Python 3.11+
- Docker + Colima (for containers) OR local Python environment
- Databricks workspace with ZeroBus enabled
- OAuth2 M2M credentials for Databricks

### 2. Installation

#### Local Development
```bash
cd unified_connector
pip install -r requirements.txt

# Generate master password for credential encryption
python -m unified_connector.core.credential_manager generate-password
export CONNECTOR_MASTER_PASSWORD='<generated-password>'

# Run connector
python -m unified_connector
```

#### Docker/Colima
```bash
# Start Colima (if not running)
colima start

# Build and run with docker-compose
docker-compose -f docker-compose.unified.yml up -d

# View logs
docker-compose -f docker-compose.unified.yml logs -f unified_connector

# Stop
docker-compose -f docker-compose.unified.yml down
```

### 3. Configuration

Edit `config/config.yaml`:

```yaml
# Enable protocol discovery
discovery:
  enabled: true
  network:
    scan_enabled: true
    subnets:
      - "192.168.1.0/24"
      - "172.17.0.0/24"

# Configure ZeroBus target
zerobus:
  enabled: true  # Start disabled, enable via Web UI
  default_target:
    workspace_host: "https://adb-xxx.azuredatabricks.net"
    catalog: "main"
    schema: "iot_data"
    table: "sensor_readings"
  auth:
    client_id: "${credential:zerobus.client_id}"
    client_secret: "${credential:zerobus.client_secret}"

# Enable tag normalization
normalization:
  enabled: true
  path_template: "{site}/{area}/{line}/{equipment}/{signal_type}/{tag_name}"
```

### 4. Store Credentials Securely

```bash
# Store Databricks OAuth2 credentials
python -m unified_connector.core.credential_manager store zerobus.client_id "<your-client-id>"
python -m unified_connector.core.credential_manager store zerobus.client_secret "<your-client-secret>"

# List stored credentials
python -m unified_connector.core.credential_manager list
```

### 5. Access Web UI

Open browser: `http://localhost:8080`

**Features:**
- Discover protocol servers on network
- Add/remove data sources
- Configure ZeroBus targets
- Monitor data flow metrics
- View connection status

## Configuration Guide

### Protocol Source Configuration

#### OPC-UA
```yaml
sources:
  - name: "plant_opcua_server"
    protocol: "opcua"
    endpoint: "opc.tcp://192.168.1.100:4840"
    enabled: true

    # Security (optional)
    security_mode: "SignAndEncrypt"
    security_policy: "Basic256Sha256"
    username: "admin"
    password: "password123"

    # Normalization context
    site: "plant1"
    area: "production"
    line: "line1"
    equipment: "plc1"

    # Performance tuning
    polling_mode: true
    polling_interval_ms: 500
    variable_limit: 500
```

#### MQTT
```yaml
sources:
  - name: "plant_mqtt_broker"
    protocol: "mqtt"
    endpoint: "mqtt://192.168.1.101:1883"
    enabled: true

    # Topics to subscribe
    topics:
      - "sensors/#"
      - "alarms/#"

    # Authentication (optional)
    username: "mqtt_user"
    password: "mqtt_password"

    # TLS (optional)
    use_tls: false
```

#### Modbus
```yaml
sources:
  - name: "plant_modbus_plc"
    protocol: "modbus"
    endpoint: "modbus://192.168.1.102:502"
    enabled: true

    # Slave configuration
    slave_id: 1

    # Register mapping
    registers:
      - address: 0
        count: 100
        type: "holding"
        data_type: "float"
```

### Per-Source ZeroBus Targets

Route different sources to different Delta tables:

```yaml
zerobus:
  enabled: true

  # Default target for sources without specific target
  default_target:
    workspace_host: "https://adb-xxx.azuredatabricks.net"
    catalog: "main"
    schema: "iot_data"
    table: "sensor_readings"

  # Per-source targets
  source_targets:
    plant_opcua_server:
      catalog: "manufacturing"
      schema: "plant1"
      table: "opcua_data"

    plant_mqtt_broker:
      catalog: "manufacturing"
      schema: "plant1"
      table: "mqtt_data"

    plant_modbus_plc:
      catalog: "manufacturing"
      schema: "plant1"
      table: "modbus_data"
```

### Tag Normalization

ISA-95/Purdue Model hierarchical paths:

```yaml
normalization:
  enabled: true

  # Path template
  path_template: "{site}/{area}/{line}/{equipment}/{signal_type}/{tag_name}"

  # Default context (overridden by source config)
  default_context:
    site: "plant1"
    area: "production"
    line: "line1"
    equipment: "default"

  # Quality mapping
  quality_mapping:
    opcua:
      good: [0]
      uncertain: [64, 65, 66]
      bad: [128, 129, 130]
```

**Example normalized paths:**
- `plant1/production/line1/plc1/temperature/reactor_temp`
- `plant1/production/line2/robot1/speed/conveyor_speed`
- `site2/utilities/boiler1/pressure/steam_pressure`

### Numeric Compression (SDT/Deadband/Change-Only)

Compression is applied after normalization and before backpressure enqueue.
Timestamp source for SDT decisions is the source `event_time_ms` (not ingest time).

```yaml
numeric_compression:
  mode: sdt            # none | change_only | deadband | sdt
  change_only: true    # applies to NONE mode + all non-numeric values
  deadband: 0.0
  sdt_deviation: 0.25
  sdt_max_interval_ms: 600000
  sdt_min_interval_ms: 0
  rules:
    - tag_path_regex: ".*temperature.*"
      mode: sdt
      sdt_deviation: 0.2
      sdt_max_interval_ms: 300000
    - tag_path_regex: ".*status.*"
      mode: change_only
```

Per-source override (source config wins over global defaults):

```yaml
sources:
  - name: plant_opcua_server
    protocol: opcua
    endpoint: opc.tcp://192.168.1.100:4840
    numeric_compression:
      mode: sdt
      change_only: true
      sdt_deviation: 0.1
      sdt_max_interval_ms: 300000
```

## REST API

### Discovery

**List discovered servers:**
```bash
GET /api/discovery/servers?protocol=opcua

Response:
{
  "servers": [
    {
      "protocol": "opcua",
      "host": "192.168.1.100",
      "port": 4840,
      "endpoint": "opc.tcp://192.168.1.100:4840",
      "name": "OT Data Simulator",
      "reachable": true
    }
  ],
  "count": 1
}
```

**Trigger discovery scan:**
```bash
POST /api/discovery/scan

Response:
{
  "status": "ok",
  "message": "Discovery scan started"
}
```

### Sources

**List sources:**
```bash
GET /api/sources

Response:
{
  "sources": [...]
}
```

**Add source:**
```bash
POST /api/sources
Content-Type: application/json

{
  "name": "plant_opcua_server",
  "protocol": "opcua",
  "endpoint": "opc.tcp://192.168.1.100:4840",
  "enabled": true
}

Response:
{
  "status": "ok",
  "message": "Source 'plant_opcua_server' added"
}
```

**Remove source:**
```bash
DELETE /api/sources/plant_opcua_server

Response:
{
  "status": "ok",
  "message": "Source 'plant_opcua_server' removed"
}
```

### Metrics

**Get metrics:**
```bash
GET /api/metrics

Response:
{
  "bridge": {
    "records_received": 12543,
    "records_normalized": 12543,
    "records_enqueued": 12543,
    "records_dropped": 0,
    "batches_sent": 13,
    "reconnections": 0
  },
  "backpressure": {...},
  "zerobus_targets": {...},
  "clients": {...}
}
```

## Testing with OT Simulator

The project includes `ot_simulator` for generating test data:

```bash
# Start OT Simulator (generates OPC-UA, MQTT, Modbus data)
docker-compose -f docker-compose.unified.yml up -d ot_simulator

# Verify simulator is running
curl http://localhost:8989/health

# Start unified connector
docker-compose -f docker-compose.unified.yml up -d unified_connector

# Trigger discovery (should find simulator on 172.20.0.0/16 network)
curl -X POST http://localhost:8080/api/discovery/scan

# Check discovered servers
curl http://localhost:8080/api/discovery/servers
```

## Deployment

### Production Considerations

1. **Security:**
   - Set strong `CONNECTOR_MASTER_PASSWORD`
   - Use TLS/SSL for protocol connections
   - Rotate OAuth2 credentials regularly
   - Run containers as non-root user (UID 1000)

2. **Performance:**
   - Tune batch size based on network latency
   - Adjust backpressure queue size for bursty data
   - Enable disk spooling for resilience
   - Monitor Prometheus metrics

3. **Reliability:**
   - Use Docker restart policies
   - Monitor health check endpoints
   - Configure appropriate timeouts
   - Enable disk spooling for data loss prevention

### Environment Variables

```bash
# Required
CONNECTOR_MASTER_PASSWORD=<strong-password>

# Optional (can be set via Web UI or config)
CONNECTOR_ZEROBUS_CLIENT_ID=<databricks-oauth2-client-id>
CONNECTOR_ZEROBUS_CLIENT_SECRET=<databricks-oauth2-client-secret>

# Logging
CONNECTOR_LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR
```

### Colima Deployment

```bash
# Start Colima with sufficient resources
colima start --cpu 4 --memory 8 --disk 50

# Build images
docker-compose -f docker-compose.unified.yml build

# Start services
docker-compose -f docker-compose.unified.yml up -d

# View logs
docker-compose -f docker-compose.unified.yml logs -f

# Stop services
docker-compose -f docker-compose.unified.yml down

# Clean up volumes (WARNING: deletes data)
docker-compose -f docker-compose.unified.yml down -v
```

## Troubleshooting

### Protocol discovery not finding servers

1. Check network connectivity:
   ```bash
   docker exec -it unified-connector ping ot-simulator
   ```

2. Verify subnets in config match your network:
   ```yaml
   discovery:
     network:
       subnets:
         - "172.20.0.0/16"  # Docker network
   ```

3. Check firewall rules

### ZeroBus connection failures

1. Verify credentials:
   ```bash
   python -m unified_connector.core.credential_manager get zerobus.client_id
   ```

2. Test Databricks connectivity:
   ```bash
   curl -v https://adb-xxx.azuredatabricks.net
   ```

3. Check circuit breaker state:
   ```bash
   curl http://localhost:8080/api/status | jq '.circuit_breaker_state'
   ```

### High memory usage

1. Reduce backpressure queue size:
   ```yaml
   backpressure:
     max_queue_size: 10000  # Reduce from 50000
   ```

2. Enable disk spooling:
   ```yaml
   backpressure:
     disk_spool:
       enabled: true
       max_size_mb: 500
   ```

### Data not flowing to Delta tables

1. Check ZeroBus connection status:
   ```bash
   curl http://localhost:8080/api/status | jq '.zerobus_connected'
   ```

2. Verify table exists in Databricks:
   ```sql
   SELECT * FROM main.iot_data.sensor_readings LIMIT 10;
   ```

3. Check metrics for dropped records:
   ```bash
   curl http://localhost:8080/api/metrics | jq '.bridge.records_dropped'
   ```

## Contributing

This connector follows SOLID principles and DRY architecture:

- **Single Responsibility**: Each component has one clear purpose
- **Open/Closed**: Extensible via plugins without modifying core
- **Liskov Substitution**: Protocol clients implement common interface
- **Interface Segregation**: Minimal, focused interfaces
- **Dependency Inversion**: Depend on abstractions, not implementations

## License

Proprietary - Databricks Internal Use

## Support

For issues and questions, contact the Databricks IoT team.
