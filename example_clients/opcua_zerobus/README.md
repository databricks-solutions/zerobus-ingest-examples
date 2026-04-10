# OPC UA Zerobus Client

Stream OPC UA telemetry directly into Databricks Delta Lake via Zerobus Ingest.

## Architecture Options

This example provides two architectures:

### Simple (Direct)

```
┌───────────────┐     ┌──────────────────────┐     ┌─────────┐     ┌────────────┐
│ OPC UA Server │ ──► │ opcua_zerobus_client │ ──► │ Zerobus │ ──► │ Delta Lake │
└───────────────┘     └──────────────────────┘     └─────────┘     └────────────┘
```

**Best for:** Development, testing, single-machine deployments.

### Advanced (RabbitMQ Buffered)

```
┌───────────────┐     ┌──────────────┐     ┌──────────┐     ┌──────────────────┐     ┌─────────┐     ┌────────────┐
│ OPC UA Server │ ──► │ opcua_client │ ──► │ RabbitMQ │ ──► │ forwarding_agent │ ──► │ Zerobus │ ──► │ Delta Lake │
└───────────────┘     └──────────────┘     └──────────┘     └──────────────────┘     └─────────┘     └────────────┘
```

**Best for:** Production deployments requiring message buffering, network resilience, and independent scaling.

## Prerequisites

- Python 3.10+
- Databricks workspace with Unity Catalog enabled
- OPC UA server (or use the included simulator)

## Quick Start (Simple)

### 1. Install dependencies

```bash
cd opcua_zerobus
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Create the configuration file

```bash
cp .env.example .env
```

### 3. Create a service principal and get credentials

See [Databricks documentation](https://docs.databricks.com/aws/en/admin/users-groups/manage-service-principals) for full details.

1. In your Databricks workspace, click your username in the top bar → **Settings**
2. Click the **Identity and access** tab
3. Next to Service principals, click **Manage**
4. Click **Add service principal** → **Add new**
5. Enter a name (e.g., `opcua-zerobus-client`) and click **Add**
6. Select the service principal you just created
7. Click the **Secrets** tab → **Generate secret**
8. Set the lifetime (up to 730 days) and click **Generate**
9. Copy the **Secret** and **Client ID** immediately (the secret is only shown once)

### 4. Fill in your .env file

Edit `.env` with your values:
- `DATABRICKS_CLIENT_ID`: The Client ID from step 9
- `DATABRICKS_CLIENT_SECRET`: The Secret from step 9
- `DATABRICKS_WORKSPACE_URL`: Your workspace URL (e.g., `https://dbc-abc123.cloud.databricks.com`)
- `DATABRICKS_INGEST_ENDPOINT`: Zerobus endpoint (e.g., `abc123.zerobus.us-east-1.cloud.databricks.com`)
- `DATABRICKS_TABLE_NAME`: Target table (e.g., `my_catalog.my_schema.opcua_telemetry`)

### 5. Create the Delta table and grant permissions

Run `create_table.sql` in your Databricks SQL editor. Replace `<catalog>`, `<schema>`, and `<service_principal_name>` with your actual values (use the service principal name from step 5).

### 6. Start the OPC UA simulator

In a separate terminal (skip if you have your own OPC UA server):

```bash
python simulator/opcua_server.py
```

### 7. Run the client

```bash
python opcua_zerobus_client.py
```

## Quick Start (Advanced with Docker)

**Requires:** Docker Desktop, Colima, or another Docker-compatible runtime.

### 1. Complete Simple setup steps 3-5

Create the service principal, configure credentials, and create the Delta table as described above.

### 2. Start the OPC UA simulator

The simulator must run on your host machine (outside Docker). In a separate terminal:

```bash
python simulator/opcua_server.py
```

Skip this if you have your own OPC UA server (update `OPCUA_URL` in `.env` accordingly).

### 3. Configure environment

```bash
cd advanced
cp .env.example .env
```

Edit `.env` with the same Databricks credentials, plus RabbitMQ settings if customizing.

### 4. Start the stack

```bash
docker compose up -d
```

### 5. Monitor logs

```bash
docker compose logs -f
```

## Configuration Reference

### Simple Client Environment Variables

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `DATABRICKS_CLIENT_ID` | Yes | Service principal client ID | - |
| `DATABRICKS_CLIENT_SECRET` | Yes | Service principal client secret | - |
| `DATABRICKS_WORKSPACE_URL` | Yes | Workspace URL (e.g., `https://xxx.cloud.databricks.com`) | - |
| `DATABRICKS_INGEST_ENDPOINT` | Yes | Zerobus endpoint (e.g., `xxx.zerobus.region.cloud.databricks.com`) | - |
| `DATABRICKS_TABLE_NAME` | Yes | Target table (e.g., `catalog.schema.opcua_telemetry`) | - |
| `OPCUA_URL` | Yes | OPC UA server endpoint | `opc.tcp://localhost:4840/freeopcua/server/` |
| `OPCUA_POLL_INTERVAL` | No | Polling interval in seconds | `5` |
| `OPCUA_NAMESPACE` | No | OPC UA namespace index | `2` |
| `TAGS_CONFIG_FILE` | No | Path to tag configuration YAML | `tags_config.yaml` |
| `BATCH_SIZE` | No | Records per Zerobus batch | `50` |
| `BATCH_TIMEOUT` | No | Max seconds before flushing batch | `2.0` |
| `BACKUP_PATH` | No | JSONL backup file for outages | `backup_opcua.jsonl` |
| `OPCUA_SECURITY_POLICY` | No | OPC UA security (e.g., `Basic256Sha256,SignAndEncrypt`) | - |
| `OPCUA_CERT_PATH` | No | Path to X.509 certificate | - |
| `OPCUA_KEY_PATH` | No | Path to private key | - |

### Advanced Client Additional Variables

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `RABBITMQ_HOST` | Yes | RabbitMQ hostname | `localhost` |
| `RABBITMQ_PORT` | No | RabbitMQ port | `5672` |
| `RABBITMQ_USERNAME` | No | RabbitMQ username | `guest` |
| `RABBITMQ_PASSWORD` | No | RabbitMQ password | `guest` |
| `RABBITMQ_QUEUE_NAME` | Yes | Queue name | `opcua_data` |
| `RABBITMQ_MESSAGE_TTL` | No | Message TTL in milliseconds | `3600000` |
| `FORWARDING_BATCH_SIZE` | No | Forwarding agent batch size | `50` |
| `FORWARDING_BATCH_TIMEOUT` | No | Forwarding agent batch timeout | `2.0` |

## File Reference

| File | Description |
|------|-------------|
| `opcua_zerobus_client.py` | Simple direct OPC UA → Zerobus client |
| `tags_config.yaml` | Tag definitions (lines and tags to poll) |
| `create_table.sql` | Delta table DDL with grants |
| `requirements.txt` | Python dependencies |
| `.env.example` | Environment variable template |
| `simulator/opcua_server.py` | Test OPC UA server with synthetic data |
| `advanced/` | RabbitMQ-buffered architecture files |

## How It Works

### Tag Resolution

Tags are resolved using the KEPServerEX convention:
```
ns={namespace};s={device_path}.{line}.{tag_name}
```

Example: `ns=2;s=Channel1.Device1.LineA.Temperature`

### Data Format

Each poll produces one JSON record per line:
```json
{
  "line": "LineA",
  "Temperature": 72.5,
  "Humidity": 45.2,
  "Pressure": 1013.25,
  "Status": "Running",
  "Running": true,
  "ts": 1707145200000000
}
```

### Resilience Features

**Simple Client:**
- JSONL disk backup when Zerobus is unreachable
- Automatic backup drain on reconnect
- Recovery-enabled Zerobus stream
- Graceful shutdown on SIGTERM

**Advanced Client (adds):**
- RabbitMQ provides durable message buffering
- Publisher confirms ensure messages reach the broker
- Unacked messages auto-requeue on disconnect
- Independent scaling of OPC UA polling and Zerobus ingestion

## Troubleshooting

### OPC UA Connection Failed

```
OPC UA server unreachable — retrying in 5s...
```

- Verify `OPCUA_URL` is correct
- Check if the OPC UA server is running
- Ensure firewall allows port 4840 (or configured port)

### Zerobus Authentication Failed

```
Zerobus stream creation failed
```

- Verify service principal credentials
- Check service principal has `MODIFY` permission on the target table
- Ensure `DATABRICKS_INGEST_ENDPOINT` doesn't include `https://`

### Tags Not Found

```
BadNodeIdUnknown
```

- Verify `tags_config.yaml` matches your OPC UA server's tag structure
- Check namespace index (`OPCUA_NAMESPACE`)
- Use an OPC UA browser (e.g., UAExpert) to verify tag paths

### RabbitMQ Connection Failed (Advanced)

```
RabbitMQ connect failed — retrying...
```

- Verify RabbitMQ is running: `docker compose ps`
- Check credentials in `.env`
- Ensure port 5672 is accessible

### Data Not Appearing in Delta Table

- Check Zerobus logs for ingestion errors
- Verify table exists and schema matches
- Check service principal permissions

## Customizing Tags

Edit `tags_config.yaml` to match your OPC UA server:

```yaml
opcua:
  namespace: 2
  device_path: Channel1.Device1

lines:
  MyLine:
    tags:
      - name: MyTag
        type: float
      - name: AnotherTag
        type: string
```

Then update `create_table.sql` with matching columns.

## License

See [LICENSE](../../LICENSE) in the repository root.
