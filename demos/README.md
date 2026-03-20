# Demos

Fully encapsulated, end-to-end examples that showcase Zerobus Ingest in action. Each demo highlights a particular pattern, industry use case, or integration scenario and is designed to be deployed and run independently.

## What belongs here

A demo is a **complete, self-contained project** that tells a story. It should include everything needed to go from zero to a working deployment: data generation, ingestion, storage, and visualization or analysis.

### Expected structure

Each demo lives in its own directory under `demos/` and should follow this layout:

```
demos/<demo_name>/
├── README.md                  # Overview, quick start, architecture, troubleshooting
├── config.toml                # Single source of truth for all configuration
├── requirements.txt           # Python dependencies (or equivalent for other languages)
├── main.py                    # Entry point for the data generator / producer
├── deploy.sh                  # One-command deployment script
├── databricks.yml             # Databricks Asset Bundle definition
├── src/                       # Core source code (data generation, simulation logic)
├── app/                       # Databricks App code (Streamlit, Gradio, etc.)
│   ├── app.py
│   ├── app.yml
│   └── requirements.txt
├── notebooks/                 # Databricks notebooks (analysis, table setup, etc.)
├── scripts/                   # Utility scripts (table creation, config parsing, etc.)
└── docs/                      # Extended documentation (schema, troubleshooting, etc.)
```

### Guidelines

- **Self-contained**: A user should be able to clone the repo, `cd` into the demo directory, and follow the README to deploy everything. No implicit dependencies on other demos or directories.
- **Config-driven**: Use a single config file (e.g., `config.toml`) as the source of truth. Avoid scattering configuration across multiple files.
- **One-command deploy**: Provide a `deploy.sh` (or equivalent) that handles table creation, permissions, asset bundle deployment, and app setup.
- **Multi-protocol when possible**: Demos are a great place to showcase different Zerobus ingestion protocols (SDK/gRPC, REST API) side by side.
- **Documentation**: Every demo needs a README with at minimum: overview, prerequisites, quick start steps, architecture diagram, and troubleshooting. Extended docs go in a `docs/` subfolder.
- **Databricks Asset Bundles**: Use DABs for infrastructure-as-code deployment of apps, jobs, and notebooks.

### Demo vs. Example Client

If your contribution is a **reusable client** that customers can take as a starting point and build upon (e.g., an MQTT-to-Zerobus bridge), it belongs in [`example_clients/`](../example_clients/) instead. Demos are narrative-driven showcases; example clients are building blocks.

## Current Demos

| Demo | Industry / Pattern | Protocols Used |
|------|--------------------|----------------|
| [Data Drifter Regatta](./data_drifter/) | Marine telemetry / IoT | SDK (gRPC) + REST API |
