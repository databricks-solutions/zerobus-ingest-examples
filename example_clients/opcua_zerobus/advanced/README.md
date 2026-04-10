# Advanced OPC UA Zerobus Client (RabbitMQ Buffered)

This directory contains the advanced, production-ready architecture using RabbitMQ as a message buffer between OPC UA polling and Zerobus ingestion.

## Architecture

```
┌───────────────┐     ┌──────────────┐     ┌──────────┐     ┌──────────────────┐     ┌─────────┐     ┌────────────┐
│ OPC UA Server │ ──► │ opcua_client │ ──► │ RabbitMQ │ ──► │ forwarding_agent │ ──► │ Zerobus │ ──► │ Delta Lake │
└───────────────┘     └──────────────┘     └──────────┘     └──────────────────┘     └─────────┘     └────────────┘
```

## Benefits

- **Message durability**: RabbitMQ persists messages to disk
- **Decoupled components**: OPC UA client and forwarding agent can restart independently
- **Backpressure handling**: RabbitMQ buffers data during Zerobus outages
- **Independent scaling**: Run multiple forwarding agents for higher throughput

## Quick Start

```bash
# Configure environment
cp .env.example .env
# Edit .env with your credentials

# Start the stack
docker compose up -d

# Start the simulator (on host)
cd ..
python simulator/opcua_server.py

# View logs
cd advanced
docker compose logs -f
```

## Components

| File | Description |
|------|-------------|
| `opcua_client.py` | Polls OPC UA server, publishes to RabbitMQ |
| `forwarding_agent.py` | Consumes from RabbitMQ, ingests to Zerobus |
| `docker-compose.yml` | Docker stack definition |
| `Dockerfile` | Container image for Python services |
| `requirements.txt` | Python dependencies (includes pika for RabbitMQ) |
| `.env.example` | Environment variable template |

## RabbitMQ Management

Access the RabbitMQ management UI at http://localhost:15672 (default: guest/guest).
