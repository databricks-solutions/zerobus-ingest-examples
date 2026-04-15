# Zerobus Ingest Examples


Welcome to the world of streamlined ingestion!
In this repo, you will find examples and demos of Zerobus Ingest, a push-based API that streamlines streaming ingestion into the Lakehouse.

* Learn more [here](https://docs.databricks.com/aws/en/ingestion/zerobus-overview)
* This repository is not for Zerobus Ingest SDKs, view the SDKs [here](https://github.com/databricks/zerobus-sdk)

## Repository Structure

| Folder | Description |
|--------|-------------|
| [`demos/`](./demos/) | Fully encapsulated, end-to-end examples that showcase Zerobus Ingest in action. Each demo highlights a particular pattern or industry use case and includes everything needed to deploy and run independently. |
| [`example_clients/`](./example_clients/) | Reusable reference implementations of Zerobus Ingest clients. Each example client demonstrates how to connect a specific protocol or data source to Zerobus, providing a foundation users can take and build upon. |
| [`observability/`](./observability/) | Databricks Asset Bundles for monitoring and observability. Deploy dashboards and alerting tools that surface Zerobus Ingest health and throughput data from system tables. |

## Demos
* [Data Drifter Regatta](./demos/data_drifter/) - Real-time sailboat race tracking with marine telemetry (SDK/gRPC + REST API)

## Example Clients
* [Salesforce Zerobus](./example_clients/salesforce_zerobus/) - Stream Salesforce CDC events to Delta tables via the Pub/Sub API (Python & Go)
* [GitHub Zerobus SDP OCSF](./example_clients/github_zerobus_sdp_ocsf/) - Push GitHub public events via Zerobus + SDP integration following Cyber Lakehouse OCSF Medallion Architecture blueprint
* [syslog-ng Zerobus](./example_clients/syslog-ng-zerobus/) - Forward syslog-ng log streams to a Delta table via OTLP/gRPC with automatic OAuth2 token management
* [OPC UA Zerobus](./example_clients/opcua_zerobus/) - Stream OPC UA telemetry to Delta tables with simple (direct) and advanced (RabbitMQ-buffered) architectures

*Coming soon* - MQTT and more.

## Observability
* [Zerobus Ingest Monitoring Dashboard](./observability/zerobus_ingest_monitoring_dashboard/) - AI/BI dashboard deployed as a DAB that surfaces stream health, ingest throughput, error rates, and protocol distribution from `system.lakeflow` system tables

## How to get help

Databricks support doesn't cover this content. For questions or bugs, please open a GitHub issue and the team will help on a best effort basis.


## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
