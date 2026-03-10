"""
DATA DRIFTER REGATTA
Real-Time Sailboat Race Visualization

Powered by Databricks Zerobus Ingest & Lakeflow Connect
"""

import streamlit as st
import pandas as pd
import os
import time
import sys
from datetime import datetime
import logging

# Handle TOML for different Python versions
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from streamlit_folium import st_folium

import db_connection
import components

# Load configuration from config.toml file
@st.cache_resource
def load_config():
    """Load configuration from TOML file"""
    try:
        logger.info("Loading config from config.toml")

        # Read config.toml from app directory
        # Note: config.toml is copied to app/ by deploy.sh during deployment
        # The source of truth is config.toml at project root - never modify app/config.toml directly
        config_path = "config.toml"

        try:
            with open(config_path, "rb") as f:
                config_data = tomllib.load(f)
            logger.info(f"Configuration loaded successfully from: {config_path}")
        except FileNotFoundError:
            error_msg = f"Config file not found: {config_path}"
            st.error(f"{error_msg}")
            logger.error(error_msg)

            # Show current working directory for debugging
            import os
            cwd = os.getcwd()
            st.info(f"Current working directory: {cwd}")
            logger.error(f"Current working directory: {cwd}")

            # List files in current directory
            try:
                files = os.listdir(".")
                st.info(f"Files in current directory: {', '.join(files[:10])}")
                logger.error(f"Files in current directory: {files}")
            except:
                pass

            st.warning("Ensure deploy.sh successfully copied config.toml to app/ directory")
            return None

        # Extract and structure configuration
        config = {
            "workspace_url": config_data["zerobus"]["workspace_url"],
            "table_name": config_data["zerobus"]["table_name"],
            "weather_table_name": config_data["zerobus"]["weather_station_table_name"],
            "warehouse_id": config_data["warehouse"]["sql_warehouse_id"],
            "race_start_time": config_data["telemetry"]["race_start_time"],
            "race_duration_seconds": config_data["telemetry"]["race_duration_seconds"],
            "real_time_duration_seconds": config_data["telemetry"]["real_time_duration_seconds"],
            "race_course_start_lat": config_data["race_course"]["start_lat"],
            "race_course_start_lon": config_data["race_course"]["start_lon"],
            "race_course_marks": config_data["race_course"]["marks"],
            "num_boats": config_data["fleet"]["num_boats"],
        }

        logger.info("Configuration loaded successfully from config.toml")
        return config

    except KeyError as e:
        st.error(f"Missing required configuration key: {e}")
        logger.error(f"Config key error: {e}", exc_info=True)
        return None
    except Exception as e:
        st.error(f"Failed to load configuration: {str(e)}")
        logger.error(f"Config load error: {str(e)}", exc_info=True)
        return None

# Page configuration
st.set_page_config(
    page_title="Data Drifter Regatta",
    page_icon="⛵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        background: linear-gradient(90deg, #1E3A8A 0%, #3B82F6 50%, #1E3A8A 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        padding: 1rem 0;
    }
    .subtitle {
        text-align: center;
        color: #6B7280;
        font-size: 1.2rem;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #F3F4F6;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
    }
    .boat-status {
        font-size: 0.9rem;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
        font-weight: 600;
    }
    .status-racing { background-color: #FEF3C7; color: #92400E; }
    .status-finished { background-color: #D1FAE5; color: #065F46; }
    .status-dnf { background-color: #FEE2E2; color: #991B1B; }
</style>
""", unsafe_allow_html=True)

# Title and subtitle
st.markdown('<p class="main-header">🌊 DATA DRIFTER REGATTA ⛵</p>', unsafe_allow_html=True)
st.markdown('<p class="subtitle">Real-Time Sailing Competition • Powered by Lakeflow Connect Zerobus Ingest</p>', unsafe_allow_html=True)

# Load configuration
config = load_config()
if config is None:
    st.error("Failed to load configuration. Please check environment variables.")
    st.stop()

# Configuration from environment variables
TABLE_NAME = config["table_name"]
RACE_START_TIME = datetime.fromisoformat(config["race_start_time"].replace('Z', '+00:00'))
RACE_DURATION_SECONDS = config["race_duration_seconds"]
REAL_TIME_DURATION_SECONDS = config["real_time_duration_seconds"]
TIME_ACCELERATION = RACE_DURATION_SECONDS / REAL_TIME_DURATION_SECONDS if REAL_TIME_DURATION_SECONDS > 0 else 1
REFRESH_INTERVAL = 30  # seconds

# Color palette for boats (distinct colors)
BOAT_COLORS = [
    '#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8',
    '#F7DC6F', '#BB8FCE', '#85C1E2', '#F8B195', '#C06C84',
    '#96CEB4', '#FFEAA7', '#DFE6E9', '#74B9FF', '#A29BFE',
    '#FD79A8', '#FDCB6E', '#E17055', '#00B894', '#00CEC9'
]

# Initialize modules
db_connection.init(config, TABLE_NAME)
components.init(config, BOAT_COLORS)

# Main app
def main():
    try:
        # Debug: Check environment variables (shown in sidebar for debugging)
        debug_mode = os.getenv("DEBUG", "false").lower() == "true"

        # Sidebar
        with st.sidebar:
            st.header("⚙️ Race Controls")

            # Auto-refresh toggle
            auto_refresh = st.toggle("Auto-refresh", value=True)

            # Refresh interval slider (only shown when auto-refresh is on)
            if auto_refresh:
                refresh_interval = st.slider("Refresh interval (seconds)", min_value=5, max_value=60, value=REFRESH_INTERVAL, step=5)
            else:
                refresh_interval = REFRESH_INTERVAL

            # Manual refresh button
            if not auto_refresh:
                if st.button("🔄 Refresh Now"):
                    st.rerun()

            # Start new race
            if st.button("🚀 Start New Race", type="primary"):
                with st.spinner("Clearing all tables..."):
                    try:
                        conn = db_connection.get_connection()
                        if conn:
                            schema_prefix = ".".join(TABLE_NAME.split(".")[:2])
                            truncated = 0
                            errors = []
                            # Get all tables in the schema and truncate them (except race_control)
                            cursor = conn.cursor()
                            cursor.execute(f"SHOW TABLES IN {schema_prefix}")
                            tables = cursor.fetchall()
                            cursor.close()
                            for row in tables:
                                tbl = row[1] if len(row) > 1 else row[0]
                                if tbl == "race_control":
                                    continue
                                full_name = f"{schema_prefix}.{tbl}"
                                try:
                                    c = conn.cursor()
                                    c.execute(f"TRUNCATE TABLE {full_name}")
                                    c.close()
                                    truncated += 1
                                except Exception as e:
                                    errors.append(f"{tbl}: {e}")
                            # Reset speed control to 1.0x
                            try:
                                c = conn.cursor()
                                c.execute(f"""
                                    MERGE INTO {schema_prefix}.race_control AS target
                                    USING (SELECT 1.0 AS speed_multiplier) AS source
                                    ON 1=1
                                    WHEN MATCHED THEN UPDATE SET
                                        speed_multiplier = source.speed_multiplier,
                                        updated_at = current_timestamp(),
                                        updated_by = 'app'
                                    WHEN NOT MATCHED THEN INSERT
                                        (speed_multiplier, updated_at, updated_by)
                                        VALUES (source.speed_multiplier, current_timestamp(), 'app')
                                """)
                                c.close()
                            except Exception as e:
                                errors.append(f"race_control reset: {e}")
                            conn.close()
                            if errors:
                                st.warning(f"Truncated {truncated} tables, {len(errors)} errors: {'; '.join(errors)}")
                            else:
                                st.success(f"Cleared {truncated} tables! Run `python3 main.py` to start a new race.")
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("Could not connect to database")
                    except Exception as e:
                        st.error(f"Failed to clear tables: {e}")

            st.divider()

            # Speed controls
            st.subheader("⚡ Race Speed")
            speed_msg = None
            row1_col1, row1_col2 = st.columns(2)
            row2_col1, row2_col2 = st.columns(2)
            speed_layout = [(row1_col1, "0.5x", 0.5), (row1_col2, "1x", 1.0),
                            (row2_col1, "2x", 2.0), (row2_col2, "4x", 4.0)]

            for col, label, multiplier in speed_layout:
                with col:
                    if st.button(label, use_container_width=True):
                        try:
                            conn = db_connection.get_connection()
                            if conn:
                                schema_prefix = ".".join(TABLE_NAME.split(".")[:2])
                                cursor = conn.cursor()
                                cursor.execute(f"""
                                    MERGE INTO {schema_prefix}.race_control AS target
                                    USING (SELECT {multiplier} AS speed_multiplier) AS source
                                    ON 1=1
                                    WHEN MATCHED THEN UPDATE SET
                                        speed_multiplier = source.speed_multiplier,
                                        updated_at = current_timestamp(),
                                        updated_by = 'app'
                                    WHEN NOT MATCHED THEN INSERT
                                        (speed_multiplier, updated_at, updated_by)
                                        VALUES (source.speed_multiplier, current_timestamp(), 'app')
                                """)
                                cursor.close()
                                conn.close()
                                speed_msg = ("success", f"Speed set to {label}")
                        except Exception as e:
                            speed_msg = ("error", f"Failed to set speed: {e}")

            # Show speed message below all buttons (full width)
            if speed_msg:
                if speed_msg[0] == "success":
                    st.success(speed_msg[1])
                else:
                    st.error(speed_msg[1])

            # Current effective speed (under buttons)
            speed_multiplier = 1.0
            try:
                conn_speed = db_connection.get_connection()
                if conn_speed:
                    schema_prefix = ".".join(TABLE_NAME.split(".")[:2])
                    c = conn_speed.cursor()
                    c.execute(f"SELECT speed_multiplier FROM {schema_prefix}.race_control LIMIT 1")
                    row = c.fetchone()
                    if row:
                        speed_multiplier = float(row[0])
                    c.close()
                    conn_speed.close()
            except Exception:
                pass
            effective_speed = TIME_ACCELERATION * speed_multiplier
            if speed_multiplier != 1.0:
                st.caption(f"Current: {effective_speed:.0f}x ({TIME_ACCELERATION:.0f}x base × {speed_multiplier:.1f}x)")
            else:
                st.caption(f"Current: {TIME_ACCELERATION:.0f}x")

            st.divider()

            # Race configuration
            st.header("🏁 Race Configuration")
            st.markdown(f"**Start Time:** {RACE_START_TIME.strftime('%Y-%m-%d %H:%M UTC')}")

            # Display race duration in human-readable format
            race_days = RACE_DURATION_SECONDS // 86400
            race_hours = (RACE_DURATION_SECONDS % 86400) // 3600
            if race_days > 0:
                st.markdown(f"**Duration:** {race_days}d {race_hours}h (race time)")
            else:
                st.markdown(f"**Duration:** {race_hours}h (race time)")

            # Display real-time duration
            real_minutes = REAL_TIME_DURATION_SECONDS // 60
            st.markdown(f"**Playback:** {real_minutes} min (real time)")

            st.markdown(f"**Course Marks:** {len(config['race_course_marks'])}")
            st.markdown(f"**Fleet Size:** {config['num_boats']} boats")

            st.divider()

            # Data source
            st.header("📊 Data Source")
            st.markdown(f"**Telemetry:** `{TABLE_NAME}`")
            weather_table = config.get("weather_table_name", "")
            if weather_table:
                st.markdown(f"**Weather:** `{weather_table}`")

            # Last update time
            st.markdown(f"**Last Update:** {datetime.now().strftime('%H:%M:%S')}")

            # Debug information
            if debug_mode:
                st.divider()
                st.header("🔍 Debug Info")
                st.markdown(f"**Python:** {sys.version.split()[0]}")
                st.markdown(f"**Streamlit:** {st.__version__}")
                has_credentials = all([
                    os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                    os.getenv("DATABRICKS_HTTP_PATH"),
                    os.getenv("DATABRICKS_TOKEN")
                ])
                st.markdown(f"**DB Credentials:** {'✓' if has_credentials else '✗'}")

        # Use st.fragment for partial refresh - only the race data section re-renders
        @st.fragment(run_every=refresh_interval if auto_refresh else None)
        def race_dashboard():
            # Query data
            with st.spinner("Loading race data..."):
                df = db_connection.query_telemetry()

            if df is not None and len(df) > 0:
                # Convert timestamp for display
                df['datetime'] = pd.to_datetime(df['timestamp'], unit='us')

                # Get latest data for each boat
                latest_positions = df.sort_values('timestamp').groupby('boat_id').last().reset_index()

                # Get latest timestamp
                latest_time = df['datetime'].max()

                # Calculate race progress
                elapsed_race_seconds = (latest_time.timestamp() - RACE_START_TIME.timestamp())
                race_progress_percent = min(100, (elapsed_race_seconds / RACE_DURATION_SECONDS) * 100) if RACE_DURATION_SECONDS > 0 else 0

                # Combined Race Progress & Fleet Status in one line
                st.subheader("🏁 Race Progress & Fleet Status ⛵")

                # Calculate fleet stats
                total_boats = df['boat_id'].nunique()
                racing = len(latest_positions[latest_positions['race_status'] == 'racing'])
                finished = len(latest_positions[latest_positions['race_status'] == 'finished'])
                dnf = len(latest_positions[latest_positions['race_status'] == 'dnf'])
                not_started = len(latest_positions[latest_positions['race_status'] == 'not_started'])

                # Display race time
                elapsed_days = int(elapsed_race_seconds // 86400)
                elapsed_hours = int((elapsed_race_seconds % 86400) // 3600)
                elapsed_minutes = int((elapsed_race_seconds % 3600) // 60)
                time_str = f"{elapsed_days}d {elapsed_hours}h {elapsed_minutes}m" if elapsed_days > 0 else f"{elapsed_hours}h {elapsed_minutes}m"

                # Single row with progress bar and all fleet metrics
                col1, col2, col3, col4, col5, col6, col7 = st.columns([3, 1, 1, 1, 1, 1, 1])

                with col1:
                    st.progress(race_progress_percent / 100.0)
                    st.caption(f"{time_str} elapsed | {latest_time.strftime('%Y-%m-%d %H:%M UTC')}")

                with col2:
                    st.metric("Progress", f"{race_progress_percent:.1f}%")

                with col3:
                    st.metric("Total", total_boats)

                with col4:
                    st.metric("🏃 Racing", racing)

                with col5:
                    st.metric("✅ Done", finished)

                with col6:
                    st.metric("❌ DNF", dnf)

                with col7:
                    st.metric("⏸️ Waiting", not_started)

                st.divider()

                # Weather Station Display
                st.subheader("🌤️ Captain's Weather Watch")

                # Query weather station data
                weather_data = db_connection.query_weather_station()

                if weather_data is not None:
                    # Weather station data available
                    wind_col1, wind_col2, wind_col3, wind_col4, wind_col5 = st.columns(5)

                    # Wind direction name helper
                    def get_wind_direction_name(degrees):
                        directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
                        idx = int((degrees % 360) / 22.5 + 0.5) % 16
                        return directions[idx]

                    with wind_col1:
                        st.metric("Wind Speed", f"{weather_data['wind_speed_knots']:.1f} kt")

                    with wind_col2:
                        wind_dir = weather_data['wind_direction_degrees']
                        st.metric("Wind Direction", f"{wind_dir:.0f}° ({get_wind_direction_name(wind_dir)})")

                    with wind_col3:
                        # Format event type for display
                        event_type = weather_data['event_type'].replace('_', ' ').title()
                        # Add emoji based on event type
                        event_emoji = {
                            'Stable': '😌',
                            'Gradual Shift': '🌀',
                            'Frontal Passage': '⛈️',
                            'Gust': '💨'
                        }.get(event_type, '🌬️')
                        st.metric("Conditions", f"{event_emoji} {event_type}")

                    with wind_col4:
                        if weather_data['in_transition']:
                            st.metric("Status", "⚠️ Changing")
                        else:
                            time_in_state = weather_data['time_in_state_seconds']
                            if time_in_state < 60:
                                st.metric("Status", f"✅ Stable ({time_in_state:.0f}s)")
                            elif time_in_state < 3600:
                                st.metric("Status", f"✅ Stable ({time_in_state/60:.0f}m)")
                            else:
                                st.metric("Status", f"✅ Stable ({time_in_state/3600:.1f}h)")

                    with wind_col5:
                        if weather_data['in_transition']:
                            st.metric("Next Change", "In progress")
                        else:
                            next_change = weather_data['next_change_in_seconds']
                            if next_change < 60:
                                st.metric("Next Change", f"~{next_change:.0f}s")
                            elif next_change < 3600:
                                st.metric("Next Change", f"~{next_change/60:.0f}m")
                            else:
                                st.metric("Next Change", f"~{next_change/3600:.1f}h")

                    # Show weather station info
                    st.caption(f"📡 {weather_data['station_name']} • {weather_data['station_location']}")

                else:
                    # Fallback to telemetry-based wind display
                    st.info("📡 Weather station data not available - showing approximate conditions from boat telemetry")

                    wind_col1, wind_col2, wind_col3, wind_col4 = st.columns(4)

                    # Get average wind conditions from latest telemetry
                    avg_wind_speed = latest_positions['wind_speed_knots'].mean()
                    avg_wind_direction = latest_positions['wind_direction_degrees'].mean()
                    min_wind = latest_positions['wind_speed_knots'].min()
                    max_wind = latest_positions['wind_speed_knots'].max()

                    # Wind direction name
                    def get_wind_direction_name(degrees):
                        directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
                        idx = int((degrees % 360) / 22.5 + 0.5) % 16
                        return directions[idx]

                    with wind_col1:
                        st.metric("Avg Wind Speed", f"{avg_wind_speed:.1f} kt")

                    with wind_col2:
                        st.metric("Wind Direction", f"{avg_wind_direction:.0f}° ({get_wind_direction_name(avg_wind_direction)})")

                    with wind_col3:
                        st.metric("Min Wind", f"{min_wind:.1f} kt")

                    with wind_col4:
                        st.metric("Max Wind", f"{max_wind:.1f} kt")

                # Performance Statistics
                st.subheader("📈 Performance Statistics")
                perf_col1, perf_col2, perf_col3, perf_col4 = st.columns(4)

                with perf_col1:
                    avg_speed = latest_positions['speed_over_ground_knots'].mean()
                    st.metric("Avg Speed", f"{avg_speed:.1f} kt")

                with perf_col2:
                    avg_vmg = latest_positions['vmg_knots'].mean()
                    st.metric("Avg VMG", f"{avg_vmg:.1f} kt")

                with perf_col3:
                    total_distance = latest_positions['distance_traveled_nm'].max()
                    st.metric("Max Distance", f"{total_distance:.1f} nm")

                with perf_col4:
                    avg_marks = latest_positions['marks_rounded'].mean()
                    total_marks = latest_positions['total_marks'].iloc[0]
                    st.metric("Avg Progress", f"{avg_marks:.1f}/{total_marks} marks")

                st.divider()

                # Two-column layout: Race Map (left) and Leaderboard (right)
                map_col, leaderboard_col = st.columns([2, 1])

                with map_col:
                    st.subheader("🗺️ Race Map")
                    folium_map = components.create_race_map(df)
                    if folium_map:
                        # Disable map interaction returns to prevent reruns on zoom/pan
                        st_folium(folium_map, width=None, height=700, returned_objects=[])

                with leaderboard_col:
                    st.subheader("🏆 Leaderboard")
                    components.display_leaderboard(df)

                st.divider()

                # Full boat statistics table
                st.subheader("📋 Boat Positions & Statistics")
                components.display_boat_stats(df)

                # Race timeline info
                st.caption(f"Race time: {latest_time.strftime('%Y-%m-%d %H:%M:%S UTC')} | Total telemetry records: {len(df):,}")

            else:
                st.warning("⚠️ No race data available. Make sure the telemetry generator is running and sending data to the table.")
                st.info("Run `python main.py` to start the race simulation.")

        # Render the auto-refreshing fragment
        race_dashboard()

    except Exception as e:
        st.error(f"Application Error: {str(e)}")
        st.exception(e)
        st.info("Click 'Refresh Now' in the sidebar to retry...")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"Fatal error: {str(e)}")
        st.exception(e)
