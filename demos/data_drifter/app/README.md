# Data Drifter Regatta - Live Race Tracker App

Real-time Streamlit app for visualizing sailboat race telemetry data.

## Features

- 🗺️ Interactive map with boat positions and tracks
- 🏆 Live leaderboard showing race standings
- 📊 Real-time statistics and wind conditions
- 🌬️ Wind direction and speed indicator
- 📈 Race progress tracking

## Quick Deploy

From the project root directory:

```bash
./deploy.sh
```

Or with custom profile:

```bash
DATABRICKS_PROFILE=my-profile ./deploy.sh
```

## What This App Does

1. **Connects to SQL Warehouse** - Queries telemetry data from Unity Catalog table
2. **Displays Interactive Map** - Shows boat positions, tracks, and race course
3. **Updates Leaderboard** - Ranks boats by distance remaining to finish
4. **Shows Statistics** - Speed, VMG, marks rounded, and race status

## App Architecture

```
SQL Warehouse → Query telemetry table → Streamlit App → Browser
```

The app queries the latest 10,000 telemetry records and visualizes them on an interactive map using Folium.

## Requirements

- Databricks SQL Warehouse (running)
- Service principal with:
  - CAN_USE permission on SQL Warehouse
  - SELECT permission on telemetry table
- Telemetry data in the configured table

## Configuration

The app is configured via:
- `app.yml` - Runtime environment variables
- `config.toml` - Race course and table configuration (copied from parent directory)

## Accessing the App

After deployment, find your app at:
- Databricks UI: **Compute → Apps → Data Drifter Regatta**
- CLI: `databricks apps get data-drifter-regatta`

## Documentation

- [Main README](../README.md) - Quick start guide
- [App Usage Guide](../docs/APP_USAGE.md) - Detailed app features
- [Troubleshooting](../docs/TROUBLESHOOTING.md) - Common issues
- [Deployment Guide](../DEPLOYMENT.md) - Deployment details

## File Structure

```
app/
├── app.py              # Main Streamlit application
├── app.yml             # Runtime configuration
├── requirements.txt    # Python dependencies
├── config.toml         # Race configuration (auto-copied from parent)
└── README.md           # This file
```

## Development

To test the app locally (requires Databricks auth):

```bash
cd app
streamlit run app.py
```

## Support

See the main project [documentation](../docs/) for detailed guides and troubleshooting.
