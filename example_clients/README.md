# Example Clients

Reusable reference implementations of Zerobus Ingest clients. Each example client demonstrates how to connect a specific protocol or data source to Zerobus, providing a foundation users can take and build upon.

## What belongs here

An example client is a **focused, minimal integration** between a data source or protocol and Zerobus Ingest. Unlike demos, these are not full end-to-end showcases — they are building blocks that handle the ingestion side and leave downstream consumption to the user.

### Expected structure

Each example client lives in its own directory under `example_clients/` and should try follow this layout:

```
example_clients/<client_name>/
├── README.md                  # Overview, setup, configuration, usage, protocol details
├── config.example.toml        # Example configuration (never commit real credentials)
├── requirements.txt           # Dependencies (or go.mod, package.json, etc.)
├── src/                       # Client source code
│   ├── client.py              # Core client implementation
│   └── ...
├── scripts/                   # Helper scripts (table setup, auth bootstrap, etc.)
└── tests/                     # Tests or validation scripts
```

### Guidelines

- **Minimal and focused**: Each client should do one thing well — bridge a specific protocol or source to Zerobus. Avoid bundling visualization, analysis, or app code.
- **Language flexibility**: Clients can be written in any language appropriate for the protocol (Python, Go, Java, etc.). Include clear dependency and build instructions.
- **Configuration**: Provide an example config file with placeholder values. Document every configuration option in the README. Never commit real credentials or endpoints.
- **Authentication examples**: Show how to authenticate with both Zerobus (OAuth/service principal) and the upstream data source. Reference the Databricks docs for service principal setup.
- **Error handling and resilience**: Demonstrate production-ready patterns — retries with backoff, graceful shutdown, connection recovery. These are templates users will build on.
- **Schema documentation**: Document the schema of data being sent to Zerobus, including field names, types, and any conventions.
- **README structure**: Each client README should include:
  - Overview of the protocol/source and why you'd use this client
  - Prerequisites (dependencies, accounts, access)
  - Configuration reference
  - Quick start / usage instructions
  - Schema of ingested data
  - How to extend or customize

### Example Client vs. Demo

If your contribution is a **complete, narrative-driven showcase** with data generation, ingestion, visualization, and analysis (e.g., a marine telemetry race simulation), it belongs in [`demos/`](../demos/) instead. Example clients are composable building blocks; demos are end-to-end stories.

## Planned Clients

| Client | Protocol / Source | Language | Description |
|--------|-------------------|----------|-------------|
| `mqtt_client` | MQTT | TBD | Bridges MQTT broker messages to Zerobus Ingest |
| `salesforce_client` | Salesforce Pub/Sub API | TBD | Subscribes to Salesforce Change Data Capture events and forwards to Zerobus |
| `opcua_client` | OPC-UA | TBD | Connects to OPC-UA servers (industrial IoT) and streams telemetry to Zerobus |
