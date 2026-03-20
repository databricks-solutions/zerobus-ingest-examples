# App Usage Guide

## Overview

The Data Drifter Regatta app is a Streamlit-based web application that visualizes real-time sailboat race telemetry data.

## Accessing the App

After deployment, access your app via:

1. **Databricks UI:**
   - Navigate to **Compute → Apps**
   - Find "Data Drifter Regatta - Live Race Tracker"
   - Click to view the app URL

2. **CLI:**
   ```bash
   databricks apps get data-drifter-regatta --profile <your-profile>
   ```

## Main Interface

### Race Map (Left Panel)

Interactive geographic map showing:
- **Gray dashed line**: Race course connecting all marks
- **Green marker**: Start line
- **Red marker**: Finish line
- **Orange markers**: Course marks to be rounded
- **Colored lines**: Boat tracks (each boat has unique color)
- **Boat icons**: Current position of each boat
  - Racing boats: Boat-shaped icon pointing in direction of travel
  - Finished boats: Green star ⭐
  - DNF boats: Red X ❌

**Map Controls:**
- **Zoom**: Scroll or use +/- buttons
- **Pan**: Click and drag
- **Hover**: View boat details in popup

### Leaderboard (Right Panel)

Shows all boats ranked by distance remaining to complete the course:
- **Rank**: Current position (1 = leader)
- **Name**: Boat name
- **Distance (nm)**: Total distance traveled

Boats with least distance remaining rank highest.

### Progress Bar

At the top of the page:
- **Progress bar**: Visual race completion indicator
- **Progress %**: Percentage of race time elapsed
- **Total boats**: Fleet size
- **Racing**: Boats actively racing
- **Done**: Boats that have finished
- **DNF**: Boats that did not finish
- **Waiting**: Boats not yet started

### Wind Indicator

Top-left corner of map shows:
- **Wind direction arrow**: Points in direction wind is blowing
- **Wind speed**: Current speed in knots
- **Cardinal direction**: N, NE, E, etc.

## Boat Positions & Statistics Table

Detailed table below the map showing:
- **Pos**: Current rank
- **Boat**: Boat name
- **Type**: Boat class
- **Status**: racing, finished, or dnf
- **Speed (kt)**: Current speed
- **Distance (nm)**: Total distance traveled
- **To Dest (nm)**: Distance remaining along course
- **VMG (kt)**: Velocity Made Good
- **Marks**: Marks rounded / total marks

## Sidebar Controls

### Refresh Settings
- **Manual refresh mode**: Click "Refresh Now" to update data
- Automatic refresh can be enabled by changing `auto_refresh` in the code

### Race Configuration
Shows:
- Start time
- Race duration
- Time acceleration factor
- Number of course marks
- Fleet size

### Data Source
Displays:
- Table name being queried
- Last update timestamp

### Debug Information
When `DEBUG=true`, shows:
- Environment variables
- Connection status
- Credential availability

## Understanding the Visualization

### Boat Tracks

Each boat's track shows its path through the race:
- Track color matches the boat's legend entry
- Line thickness indicates recency (thicker = more recent)
- Gaps in tracks may indicate GPS issues or boat stopping

### Race Status Colors

Status badges use color coding:
- **Yellow**: Racing
- **Green**: Finished
- **Red**: DNF
- **Gray**: Not started

### VMG (Velocity Made Good)

VMG shows effective speed toward the next mark:
- Positive VMG: Boat making progress toward mark
- Negative VMG: Boat moving away from mark (e.g., during tack)
- High VMG: Efficient sailing
- Low VMG: Inefficient sailing or adverse conditions

## Data Refresh

The app loads the most recent 10,000 telemetry records by default. This ensures:
- Fast query performance
- Recent data visibility
- Reasonable memory usage

For longer races, you may want to:
1. Increase the limit in `query_telemetry(limit=10000)`
2. Add time-based filtering to show only recent data
3. Implement pagination for historical data

## Performance Tips

### For Large Fleets
- Consider reducing query limit
- Filter by specific boats
- Use time-based filtering

### For Slow Connections
- Disable auto-refresh
- Use manual refresh only
- Reduce map detail

### For Long Races
- Archive old data periodically
- Query only recent time windows
- Use aggregated views for historical analysis

## Troubleshooting

### No Data Showing

**Check:**
1. Telemetry generator is running (`python main.py`)
2. Data is being written to the table
3. Table name in app matches actual table
4. SQL warehouse is running

**Debug steps:**
```bash
# Check if data exists in table
databricks sql execute \
  --warehouse-id <warehouse-id> \
  "SELECT COUNT(*) FROM <catalog>.<schema>.<table>"
```

### Map Not Loading

**Possible causes:**
- No GPS coordinates in data
- Coordinates out of valid range
- JavaScript errors in browser console

**Solutions:**
- Verify latitude/longitude values are valid
- Check browser console for errors
- Refresh the page

### Slow Performance

**Optimizations:**
- Reduce query limit
- Add indexes on timestamp column
- Enable result caching on SQL warehouse
- Use serverless SQL warehouse for auto-scaling

### Connection Errors

**Check:**
1. SQL warehouse is running
2. Service principal has CAN_USE permission on warehouse
3. Service principal has SELECT permission on table
4. Network connectivity to Databricks

## Advanced Features

### Custom Filtering

Edit `query_telemetry()` to add filters:

```python
query = f"""
SELECT * FROM {TABLE_NAME}
WHERE race_status = 'racing'  -- Only show racing boats
  AND timestamp > UNIX_MICROS(CURRENT_TIMESTAMP() - INTERVAL 1 HOUR)  -- Last hour only
ORDER BY timestamp DESC
LIMIT {limit}
"""
```

## Sharing

To share the app:
1. Get the app URL from Databricks UI
2. Share with users who have Databricks workspace access
3. Ensure users have permission to use the SQL warehouse
4. Users must be authenticated to your workspace
