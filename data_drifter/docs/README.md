# Documentation

Comprehensive documentation for the Data Drifter Regatta project.

## Getting Started

New to the project? Start here:

1. **[Main README](../README.md)** - Quick start guide (10 minutes to running app)
2. **[Configuration Guide](CONFIGURATION.md)** - Understand and customize settings
3. **[App Usage Guide](APP_USAGE.md)** - Learn how to use the app

## Reference Documentation

### [Telemetry Schema](TELEMETRY_SCHEMA.md)
Complete database schema documentation including:
- Table structure and field descriptions
- Data types and constraints
- Sample data
- Update frequency

### [Configuration Guide](CONFIGURATION.md)
Detailed configuration options covering:
- Zerobus connection settings
- Race course setup
- Fleet composition
- Wind conditions
- Telemetry generation parameters
- Racing strategies

### [App Usage Guide](APP_USAGE.md)
Complete app feature guide including:
- User interface walkthrough
- Map interactions
- Leaderboard interpretation
- Performance tips
- Advanced features
- Customization options

### [Troubleshooting Guide](TROUBLESHOOTING.md)
Solutions for common issues:
- Installation problems
- Configuration errors
- Permission issues
- Deployment failures
- Runtime errors
- Performance problems

## Quick Links

### Common Tasks

**Setup Tasks:**
- [Create Unity Catalog table](../README.md#step-4-create-the-table)
- [Grant permissions](../README.md#step-5-grant-permissions)
- [Configure OAuth](../README.md#step-2-configure-oauth-service-principal)

**Configuration:**
- [Minimal config.toml](CONFIGURATION.md#example-simple-configuration)
- [Custom race course](CONFIGURATION.md#race-course-configuration)
- [Fleet settings](CONFIGURATION.md#fleet-configuration)

**Deployment:**
- [Quick deploy](../README.md#step-8-deploy-the-app)
- [Manual deploy](../DEPLOYMENT.md#manual-deploy)
- [Environment variables](../DEPLOYMENT.md#quick-deploy)

**Troubleshooting:**
- [No data in app](TROUBLESHOOTING.md#no-data-being-generated)
- [Permission errors](TROUBLESHOOTING.md#permission-denied)
- [Deployment fails](TROUBLESHOOTING.md#bundle-deployment-fails)

## Document Structure

```
docs/
├── README.md              # This file - documentation index
├── TELEMETRY_SCHEMA.md   # Database schema reference
├── CONFIGURATION.md      # Configuration options
├── APP_USAGE.md          # App usage guide
└── TROUBLESHOOTING.md    # Problem solving guide
```

## Additional Resources

- [Databricks Apps Documentation](https://docs.databricks.com/apps/index.html)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Unity Catalog Documentation](https://docs.databricks.com/unity-catalog/index.html)
- [Databricks SQL Warehouses](https://docs.databricks.com/sql/admin/sql-endpoints.html)

## Contributing

When updating documentation:

1. Keep the main README focused on quick start
2. Put detailed information in this docs/ folder
3. Update this index when adding new documentation
4. Use clear headers and links between documents
5. Include code examples where helpful

## Feedback

Found an issue with the documentation?
- Check the troubleshooting guide first
- Review Databricks documentation
- Search community forums
