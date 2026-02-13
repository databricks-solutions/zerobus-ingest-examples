"""
🌊 DATA DRIFTER REGATTA 🌊
Real-Time Sailboat Race Visualization

Powered by Databricks Zerobus Ingest & Lakeflow Connect
"""

import streamlit as st
import pandas as pd
import os
import time
import sys
from datetime import datetime
import json
import logging
import threading
from databricks.sdk.core import Config, oauth_service_principal
import folium
from folium import plugins
from streamlit_folium import st_folium
import uuid

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

# Import databricks-sql-connector with error handling
try:
    from databricks import sql
    from databricks.sdk import WorkspaceClient
except ImportError as e:
    st.error(f"❌ Failed to import databricks modules: {str(e)}")
    st.info("Please ensure databricks-sql-connector and databricks-sdk are installed in requirements.txt")
    st.stop()

# Import navigation utilities
from navigation_utils import calculate_distance

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
            logger.info(f"✓ Configuration loaded successfully from: {config_path}")
        except FileNotFoundError:
            error_msg = f"Config file not found: {config_path}"
            st.error(f"❌ {error_msg}")
            logger.error(error_msg)

            # Show current working directory for debugging
            import os
            cwd = os.getcwd()
            st.info(f"ℹ️ Current working directory: {cwd}")
            logger.error(f"Current working directory: {cwd}")

            # List files in current directory
            try:
                files = os.listdir(".")
                st.info(f"ℹ️ Files in current directory: {', '.join(files[:10])}")
                logger.error(f"Files in current directory: {files}")
            except:
                pass

            st.warning("💡 Ensure deploy.sh successfully copied config.toml to app/ directory")
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
        st.error(f"❌ Missing required configuration key: {e}")
        logger.error(f"Config key error: {e}", exc_info=True)
        return None
    except Exception as e:
        st.error(f"❌ Failed to load configuration: {str(e)}")
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

def get_connection():
    """Create Databricks SQL connection using workspace authentication"""
    logger.info("Starting SQL warehouse connection attempt")
    debug_mode = os.getenv("DEBUG", "false").lower() == "true"
    start_time = time.time()

    try:
        if debug_mode:
            st.write("🔍 Debug - Step 1: Checking environment variables")
            st.write("Available DATABRICKS/DEFAULT env vars:")
            for key in sorted(os.environ.keys()):
                if 'DATABRICKS' in key or 'DEFAULT' in key:
                    value = os.environ[key]
                    if 'TOKEN' in key or 'SECRET' in key or 'PASSWORD' in key:
                        value = '***' if value else 'None'
                    else:
                        value = value[:80] if value and len(value) > 80 else value
                    st.write(f"  {key} = {value}")

        # Get warehouse ID from config
        warehouse_id = config["warehouse_id"]

        # Extract server hostname from workspace URL (remove https:// prefix)
        workspace_url = config.get("workspace_url", os.getenv("DATABRICKS_HOST", ""))
        if workspace_url.startswith("https://"):
            server_hostname = workspace_url.replace("https://", "")
        elif workspace_url.startswith("http://"):
            server_hostname = workspace_url.replace("http://", "")
        else:
            server_hostname = workspace_url

        if debug_mode:
            st.write(f"🔍 Debug - Step 2: Connection parameters")
            st.write(f"  Server hostname: {server_hostname}")
            st.write(f"  Warehouse ID: {warehouse_id}")
            st.write(f"  HTTP Path: /sql/1.0/warehouses/{warehouse_id}")

        # Method 1: Try using OAuth M2M with client credentials (recommended for Databricks Apps)
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

        if client_id and client_secret:
            logger.info("Attempting OAuth M2M authentication (Method 1)")
            if debug_mode:
                st.write("🔍 Debug - Step 3: Attempting connection with OAuth M2M (client credentials)...")
                st.write(f"  ✅ DATABRICKS_CLIENT_ID found: {client_id[:8]}...")
                st.write("  ✅ DATABRICKS_CLIENT_SECRET found")

            try:
                credential_provider = lambda: oauth_service_principal(Config(
                    host          = f"https://{server_hostname}",
                    client_id     = client_id,
                    client_secret = client_secret))
                
                connection =sql.connect(
                    server_hostname      = server_hostname,
                    http_path            = f"/sql/1.0/warehouses/{warehouse_id}",
                    credentials_provider = credential_provider)

                if debug_mode:
                    st.write("  Connection object created, testing query...")

                # Test the connection
                cursor = connection.cursor()
                cursor.execute("SELECT 'test' as status, current_database() as db")
                result = cursor.fetchone()
                cursor.close()

                if debug_mode:
                    st.success(f"✅ OAuth M2M authentication successful! Result: {result}")

                logger.info(f"OAuth M2M authentication successful in {time.time() - start_time:.2f}s")
                return connection

            except Exception as oauth_error:
                logger.warning(f"OAuth M2M auth failed: {str(oauth_error)}")
                if debug_mode:
                    st.error(f"❌ OAuth M2M auth failed: {str(oauth_error)}")
                    st.write("  Falling back to alternative auth methods...")
        else:
            if debug_mode:
                st.write("🔍 Debug - Step 3: Client credentials not found in environment")
                if not client_id:
                    st.write("  ❌ DATABRICKS_CLIENT_ID not set")
                if not client_secret:
                    st.write("  ❌ DATABRICKS_CLIENT_SECRET not set")
                st.write("  Attempting connection with SDK default auth...")

        # Method 2: Try SDK automatic authentication
        # In Databricks Apps, use the SDK's automatic authentication
        # The app runs as a service principal and credentials are handled automatically
        logger.info("Attempting SDK OAuth authentication (Method 2)")
        try:
            if debug_mode:
                st.write("  Initializing WorkspaceClient for auth...")

            # Initialize WorkspaceClient - it will automatically use the service principal
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.core import ApiClient

            # Create config that will auto-detect credentials
            cfg = Config()

            if debug_mode:
                st.write(f"  Config host: {cfg.host if cfg.host else 'auto-detect'}")
                st.write("  Creating API client...")

            # Create API client for making authenticated requests
            api_client = ApiClient(cfg)

            if debug_mode:
                st.write("  Getting auth headers...")

            # Get authentication headers
            def get_token():
                headers = api_client.do("GET", "/api/2.0/preview/scim/v2/Me",
                                       headers={}, data={}, raw=True).headers
                # Extract token from Authorization header
                auth_header = headers.get('Authorization', '')
                if auth_header.startswith('Bearer '):
                    return auth_header[7:]
                return None

            if debug_mode:
                st.write("  Connecting to SQL Warehouse...")

            # Connect using the SDK's auth mechanism
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                auth_type="databricks-oauth",  # Use OAuth authentication
                _socket_timeout=30
            )

            if debug_mode:
                st.write("  Connection object created, testing query...")

            # Test the connection
            cursor = connection.cursor()
            cursor.execute("SELECT 'test' as status, current_database() as db")
            result = cursor.fetchone()
            cursor.close()

            if debug_mode:
                st.success(f"✅ Connection successful! Result: {result}")

            logger.info(f"SDK OAuth authentication successful in {time.time() - start_time:.2f}s")
            return connection

        except Exception as sdk_error:
            logger.warning(f"SDK OAuth auth failed: {str(sdk_error)}")
            if debug_mode:
                st.error(f"❌ SDK OAuth auth method failed: {str(sdk_error)}")
                st.write(f"  Error type: {type(sdk_error).__name__}")
                st.exception(sdk_error)

        # Method 3: Try WorkspaceClient with better error handling
        logger.info("Attempting WorkspaceClient authentication (Method 3)")
        if debug_mode:
            st.write("🔍 Method 3: Trying WorkspaceClient authentication...")

        try:
            # Initialize WorkspaceClient with explicit host
            w = WorkspaceClient(host=f"https://{server_hostname}")

            if debug_mode:
                st.write(f"  WorkspaceClient created")
                st.write(f"  Host: {w.config.host}")

                # Try to get auth method info
                try:
                    auth_details = str(w.config)
                    st.write(f"  Config: {auth_details[:200]}")
                except:
                    pass

            # Try to authenticate and get token
            if debug_mode:
                st.write("  Attempting authentication...")

            try:
                # Call authenticate to get credentials
                credentials = w.config.authenticate()
                if debug_mode:
                    st.write(f"  Authentication successful, got credentials")

                # Use the credentials with SQL connector
                connection = sql.connect(
                    server_hostname=server_hostname,
                    http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                    credentials_provider=lambda: credentials,
                    _socket_timeout=30
                )

                if debug_mode:
                    st.write("  SQL Connection created, testing...")

                # Test the connection
                cursor = connection.cursor()
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                cursor.close()

                if debug_mode:
                    st.success(f"✅ Method 3 successful! Test query returned: {result}")

                logger.info(f"WorkspaceClient authentication successful in {time.time() - start_time:.2f}s")
                return connection

            except Exception as auth_error:
                if debug_mode:
                    st.error(f"  Authentication failed: {str(auth_error)}")
                    st.write(f"  Error type: {type(auth_error).__name__}")
                raise

        except Exception as wc_error:
            logger.warning(f"WorkspaceClient auth failed: {str(wc_error)}")
            if debug_mode:
                st.error(f"❌ Method 3 failed: {str(wc_error)}")
                st.write(f"  Error type: {type(wc_error).__name__}")
                st.exception(wc_error)

        # Method 4: Try OAuth U2M flow (for apps with attached resources)
        logger.info("Attempting OAuth U2M flow (Method 4)")
        if debug_mode:
            st.write("🔍 Method 4: Trying OAuth U2M flow...")

        try:
            # For Databricks Apps with SQL Warehouse resources, use OAuth U2M
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                auth_type="databricks-oauth",
                _socket_timeout=30
            )

            if debug_mode:
                st.write("  OAuth connection created, testing...")

            # Test the connection
            cursor = connection.cursor()
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            cursor.close()

            if debug_mode:
                st.success(f"✅ Method 4 successful! Test query returned: {result}")

            logger.info(f"OAuth U2M authentication successful in {time.time() - start_time:.2f}s")
            return connection

        except Exception as oauth_error:
            logger.warning(f"OAuth U2M auth failed: {str(oauth_error)}")
            if debug_mode:
                st.error(f"❌ Method 4 failed: {str(oauth_error)}")
                st.write(f"  Error type: {type(oauth_error).__name__}")
                st.exception(oauth_error)

        # All methods failed
        logger.error(f"All connection methods failed after {time.time() - start_time:.2f}s")
        if debug_mode:
            st.error("❌ All connection methods failed!")
            st.write("Troubleshooting suggestions:")
            st.write("1. Check that the SQL Warehouse resource is properly attached")
            st.write("2. Verify the warehouse is running and accessible")
            st.write("3. Ensure the app has CAN_USE permission on the warehouse")
            st.write("4. Check that environment variables are being set correctly")

        raise Exception("Unable to connect to Databricks SQL Warehouse. All authentication methods failed.")

    except Exception as e:
        st.error(f"❌ Failed to connect to Databricks SQL: {str(e)}")
        st.info("💡 Troubleshooting tips:")
        st.info("- Ensure the app has a SQL Warehouse resource with CAN_USE permission")
        st.info("- Check that the warehouse is running and accessible")
        st.info(f"- Warehouse ID: {warehouse_id}")
        if debug_mode:
            st.write("🔍 Full error details:")
            st.exception(e)
        return None

def execute_query_with_timeout(cursor, query, timeout_seconds=60):
    """Execute query with a timeout using threading"""
    result = [None]
    error = [None]

    def run_query():
        try:
            cursor.execute(query)
            result[0] = True
        except Exception as e:
            error[0] = e

    thread = threading.Thread(target=run_query)
    thread.daemon = True
    thread.start()
    thread.join(timeout=timeout_seconds)

    if thread.is_alive():
        raise TimeoutError(f"Query execution exceeded {timeout_seconds} seconds timeout")
    if error[0]:
        raise error[0]

    return result[0]

def query_telemetry(limit=10000):
    """Query latest telemetry data from table"""
    logger.info(f"Starting telemetry query with limit={limit}")
    debug_mode = os.getenv("DEBUG", "false").lower() == "true"
    query_start = time.time()

    try:
        # Step 1: Get connection
        logger.info("Step 1: Establishing connection")
        if debug_mode:
            st.write("🔍 Step 1: Getting connection...")

        conn = get_connection()
        if not conn:
            logger.error("Failed to establish connection")
            st.warning("⚠️ Could not establish database connection")
            return None

        logger.info(f"Step 1 complete in {time.time() - query_start:.2f}s")

        # Step 2: Count rows in table to verify we can query it
        logger.info(f"Step 2: Counting rows in {TABLE_NAME}")
        step2_start = time.time()

        try:
            count_query = f"SELECT COUNT(*) as row_count FROM {TABLE_NAME}"
            cursor = conn.cursor()
            cursor.execute(count_query)
            count_result = cursor.fetchone()
            row_count = count_result[0] if count_result else 0
            cursor.close()

            logger.info(f"Step 2 complete: {row_count:,} rows found in {time.time() - step2_start:.2f}s")

            if row_count == 0:
                logger.warning("Table is empty, no data available")
                st.warning("⚠️ Table is empty. No telemetry data available yet.")
                st.info("💡 Run `python main.py` to start generating telemetry data.")
                return None

        except Exception as count_error:
            logger.error(f"Failed to count rows: {str(count_error)}")
            st.error(f"❌ Failed to count rows in table: {str(count_error)}")
            st.info("💡 Check that:")
            st.info("  - The table exists")
            st.info("  - The service principal has SELECT permission on the table")
            st.info(f"  - Table name is correct: {TABLE_NAME}")
            if debug_mode:
                st.exception(count_error)
            return None

        if debug_mode:
            st.write(f"🔍 Step 3: Querying table: {TABLE_NAME}")

        query = f"""
        SELECT
            boat_id,
            boat_name,
            boat_type,
            timestamp,
            latitude,
            longitude,
            speed_over_ground_knots,
            heading_degrees,
            wind_speed_knots,
            wind_direction_degrees,
            distance_traveled_nm,
            distance_to_destination_nm,
            vmg_knots,
            current_mark_index,
            marks_rounded,
            total_marks,
            has_started,
            has_finished,
            race_status
        FROM {TABLE_NAME}
        ORDER BY timestamp DESC
        LIMIT {limit}
        """

        # Step 3: Execute query with timeout
        logger.info(f"Step 3: Querying {TABLE_NAME} for {limit} rows")
        step3_start = time.time()

        if debug_mode:
            st.write("🔍 Step 4: Executing query with 60 second timeout...")

        cursor = conn.cursor()

        try:
            # Execute query with timeout
            execute_query_with_timeout(cursor, query, timeout_seconds=60)
            execution_time = time.time() - step3_start

            logger.info(f"Step 3 query execution complete in {execution_time:.2f}s")

            if debug_mode:
                st.write(f"  ✅ Query executed in {execution_time:.2f} seconds")

        except TimeoutError as te:
            logger.error(f"Query timeout: {str(te)}")
            st.error(f"❌ Query execution timeout: {str(te)}")
            st.warning("💡 Troubleshooting suggestions:")
            st.info("• The SQL warehouse may be slow or overloaded")
            st.info("• Try reducing the data limit or adding filters")
            st.info("• Check SQL warehouse status in Databricks UI")
            st.info(f"• Table: {TABLE_NAME}")
            cursor.close()
            conn.close()
            return None

        # Step 4-5: Fetch results
        logger.info("Step 4-5: Fetching results as DataFrame")
        fetch_start = time.time()

        if debug_mode:
            st.write("🔍 Step 5: Fetching results...")

        df = cursor.fetchall_arrow().to_pandas()
        cursor.close()

        fetch_time = time.time() - fetch_start
        logger.info(f"Fetch complete: {len(df)} rows in {fetch_time:.2f}s")

        total_time = time.time() - query_start
        logger.info(f"Query completed successfully in {total_time:.2f}s total")

        if debug_mode:
            st.write(f"🔍 Step 6: Retrieved {len(df)} rows")

        return df

    except TimeoutError as te:
        logger.error(f"Query timeout after {time.time() - query_start:.2f}s: {str(te)}")
        # Error already displayed above
        return None
    except Exception as e:
        logger.error(f"Query failed after {time.time() - query_start:.2f}s: {str(e)}", exc_info=True)
        st.error(f"❌ Failed to query data: {str(e)}")
        st.info("💡 Check:")
        st.info("• Table exists and has data")
        st.info("• Service principal has SELECT permission")
        st.info(f"• Table name: {TABLE_NAME}")
        if debug_mode:
            st.exception(e)
        return None

def query_weather_station():
    """Query latest weather station data"""
    logger.info("Querying weather station data")

    # Check if weather station table is configured
    weather_table = config.get("weather_table_name")
    if not weather_table:
        logger.warning("Weather station table not configured")
        return None

    try:
        conn = get_connection()
        if not conn:
            logger.error("Failed to establish connection for weather station query")
            return None

        # Query latest weather station reading
        query = f"""
            SELECT
                station_id,
                station_name,
                station_location,
                timestamp,
                wind_speed_knots,
                wind_direction_degrees,
                event_type,
                in_transition,
                time_in_state_seconds,
                next_change_in_seconds
            FROM {weather_table}
            ORDER BY timestamp DESC
            LIMIT 1
        """

        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall_arrow().to_pandas()
        cursor.close()

        if result.empty:
            logger.warning("No weather station data found")
            return None

        logger.info(f"Weather station data retrieved: {len(result)} record")
        return result.iloc[0]  # Return the latest record as a Series

    except Exception as e:
        logger.error(f"Failed to query weather station: {str(e)}", exc_info=True)
        return None

def get_race_course_config():
    """Get race course configuration (marks, start/finish) from config"""
    return {
        "start_lat": config["race_course_start_lat"],
        "start_lon": config["race_course_start_lon"],
        "marks": config["race_course_marks"]
    }

def calculate_total_remaining_distance(lat, lon, current_mark_index, marks):
    """
    Calculate total remaining distance along the race course in nautical miles.

    Args:
        lat: Current latitude
        lon: Current longitude
        current_mark_index: Index of the next mark to round (0-based)
        marks: List of all marks [[lat1, lon1], [lat2, lon2], ...] where last mark is finish

    Returns:
        Total distance remaining along the course in nautical miles
    """
    if not marks or len(marks) == 0:
        return 0.0

    total_distance = 0.0

    # Marks to round are all except the last one (which is the finish line)
    marks_to_round = marks[:-1] if len(marks) > 1 else []
    finish_lat, finish_lon = marks[-1][0], marks[-1][1]

    # If all marks are rounded, just return distance to finish
    if current_mark_index >= len(marks_to_round):
        return calculate_distance(lat, lon, finish_lat, finish_lon)

    # Add distance to next mark
    next_mark = marks_to_round[current_mark_index]
    total_distance += calculate_distance(lat, lon, next_mark[0], next_mark[1])

    # Add distances between subsequent marks
    for i in range(current_mark_index, len(marks_to_round) - 1):
        mark1 = marks_to_round[i]
        mark2 = marks_to_round[i + 1]
        total_distance += calculate_distance(mark1[0], mark1[1], mark2[0], mark2[1])

    # Add distance from last mark to finish
    if len(marks_to_round) > 0:
        last_mark = marks_to_round[-1]
        total_distance += calculate_distance(last_mark[0], last_mark[1], finish_lat, finish_lon)

    return total_distance

def create_race_map(df):
    """Create interactive race map with boat tracks using Folium"""
    if df is None or len(df) == 0:
        st.warning("No telemetry data available")
        return None

    # Convert timestamp from epoch microseconds to datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='us')

    # Get race course configuration
    course_config = get_race_course_config()

    # Get race course marks
    mark_lats = [m[0] for m in course_config['marks']]
    mark_lons = [m[1] for m in course_config['marks']]

    # Calculate center of the map
    center_lat = sum(mark_lats) / len(mark_lats)
    center_lon = sum(mark_lons) / len(mark_lons)

    # Create Folium map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=9,
        tiles='OpenStreetMap',
        prefer_canvas=True
    )

    # Add race course line connecting marks
    course_coords = [[lat, lon] for lat, lon in zip(mark_lats, mark_lons)]
    folium.PolyLine(
        course_coords,
        color='gray',
        weight=2,
        opacity=0.7,
        dash_array='10, 5',
        popup='Race Course'
    ).add_to(m)

    # Add markers for each race course mark
    for idx, (lat, lon) in enumerate(zip(mark_lats, mark_lons)):
        folium.Marker(
            location=[lat, lon],
            popup=f'Mark {idx + 1}',
            tooltip=f'Mark {idx + 1}',
            icon=folium.Icon(color='orange', icon='flag', prefix='fa')
        ).add_to(m)

    # Add start line marker
    folium.Marker(
        location=[course_config['start_lat'], course_config['start_lon']],
        popup='START',
        tooltip='Start Line',
        icon=folium.Icon(color='green', icon='play', prefix='fa')
    ).add_to(m)

    # Add finish line marker (last mark)
    finish_lat, finish_lon = course_config['marks'][-1]
    folium.Marker(
        location=[finish_lat, finish_lon],
        popup='FINISH',
        tooltip='Finish Line',
        icon=folium.Icon(color='red', icon='stop', prefix='fa')
    ).add_to(m)

    # Group by boat and plot tracks
    boats = df.groupby('boat_id')

    for idx, (boat_id, boat_df) in enumerate(boats):
        boat_df = boat_df.sort_values('timestamp')

        boat_name = boat_df['boat_name'].iloc[0]
        boat_type = boat_df['boat_type'].iloc[0]
        race_status = boat_df['race_status'].iloc[0]

        color = BOAT_COLORS[idx % len(BOAT_COLORS)]

        # Create boat track coordinates
        track_coords = [[row['latitude'], row['longitude']] for _, row in boat_df.iterrows()]

        # Add boat track as polyline
        folium.PolyLine(
            track_coords,
            color=color,
            weight=3,
            opacity=0.8,
            popup=boat_name,
            tooltip=boat_name
        ).add_to(m)

        # Add current position marker (most recent point - first in DESC order)
        latest = boat_df.iloc[-1]  # Last point after sorting by timestamp ASC

        # Create popup content with boat information
        popup_html = f"""
        <div style="font-family: Arial; font-size: 12px;">
            <b>{boat_name}</b><br>
            Status: {race_status}<br>
            Speed: {latest['speed_over_ground_knots']:.1f} knots<br>
            Heading: {latest['heading_degrees']:.0f}°<br>
            Distance: {latest['distance_traveled_nm']:.1f} nm<br>
            VMG: {latest['vmg_knots']:.1f} knots<br>
            Marks: {latest['marks_rounded']}/{latest['total_marks']}<br>
            Time: {pd.to_datetime(latest['timestamp'], unit='us').strftime('%Y-%m-%d %H:%M:%S')}
        </div>
        """

        # Get heading for boat marker
        heading = latest['heading_degrees']

        # Use BoatMarker for racing boats, regular markers for finished/DNF
        if race_status == 'finished':
            folium.Marker(
                location=[latest['latitude'], latest['longitude']],
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=boat_name.split("'s")[0],
                icon=folium.Icon(color='lightgreen', icon='star', prefix='fa')
            ).add_to(m)
        elif race_status == 'dnf':
            folium.Marker(
                location=[latest['latitude'], latest['longitude']],
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=boat_name.split("'s")[0],
                icon=folium.Icon(color='red', icon='times', prefix='fa')
            ).add_to(m)
        else:
            # Use BoatMarker for racing boats with rounded SVG shape
            plugins.BoatMarker(
                location=[latest['latitude'], latest['longitude']],
                heading=heading,
                wind_heading=latest.get('wind_direction_degrees', heading + 45),
                wind_speed=latest.get('wind_speed_knots', 10),
                color=color,
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=boat_name.split("'s")[0]
            ).add_to(m)

    # Add wind indicator in top left corner
    # Get average wind conditions from latest positions
    latest_positions_for_wind = df.sort_values('timestamp').groupby('boat_id').last()
    avg_wind_speed = latest_positions_for_wind['wind_speed_knots'].mean()
    avg_wind_direction = latest_positions_for_wind['wind_direction_degrees'].mean()

    # Create wind direction name
    def get_wind_direction_name(degrees):
        directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
        index = int((degrees + 11.25) / 22.5) % 16
        return directions[index]

    wind_dir_name = get_wind_direction_name(avg_wind_direction)

    # Create custom HTML for wind indicator
    wind_html = f"""
    <div style="
        position: fixed;
        top: 80px;
        left: 10px;
        z-index: 1000;
        background-color: rgba(255, 255, 255, 0.95);
        padding: 12px 16px;
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
        font-family: Arial, sans-serif;
        border: 2px solid #1e40af;
    ">
        <div style="text-align: center;">
            <div style="font-size: 11px; font-weight: bold; color: #1e40af; margin-bottom: 4px;">
                🌬️ WIND
            </div>
            <div style="
                font-size: 32px;
                transform: rotate({avg_wind_direction}deg);
                color: #1e40af;
                line-height: 1;
                margin: 4px 0;
            ">
                ↓
            </div>
            <div style="font-size: 14px; font-weight: bold; color: #1e3a8a; margin-top: 4px;">
                {avg_wind_speed:.1f} kts
            </div>
            <div style="font-size: 11px; color: #64748b; margin-top: 2px;">
                {wind_dir_name} ({avg_wind_direction:.0f}°)
            </div>
        </div>
    </div>
    """

    # Add the wind indicator to the map
    m.get_root().html.add_child(folium.Element(wind_html))

    return m

def display_leaderboard(df):
    """Display leaderboard with all boats"""
    if df is None or len(df) == 0:
        st.info("No race data available")
        return

    # Get latest position for each boat
    latest_positions = df.sort_values('timestamp').groupby('boat_id').last().reset_index()

    # Get race course marks
    course_config = get_race_course_config()
    marks = course_config['marks']

    # Calculate total remaining distance along the race course for each boat
    latest_positions['total_remaining_distance'] = latest_positions.apply(
        lambda row: calculate_total_remaining_distance(
            row['latitude'], row['longitude'], row['current_mark_index'], marks
        ),
        axis=1
    )

    # Sort by total remaining distance (less distance = better rank)
    # DNF boats go to the end
    latest_positions['sort_key'] = latest_positions.apply(
        lambda row: (
            float('inf') if row['race_status'] == 'dnf' else row['total_remaining_distance']
        ),
        axis=1
    )
    latest_positions = latest_positions.sort_values('sort_key')

    # Display leaderboard header
    st.markdown("##### Race Leaderboard")

    # Create container for scrollable leaderboard
    leaderboard_container = st.container(height=600)

    with leaderboard_container:
        # Display each boat with rank, name, and distance
        for rank, (idx, row) in enumerate(latest_positions.iterrows(), start=1):
            boat_id = row['boat_id']
            boat_name = row['boat_name']
            distance = row['distance_traveled_nm']

            # Create columns for rank and boat info
            col1, col2 = st.columns([0.5, 4.5])

            with col1:
                # Rank with medal emoji for top 3
                if rank == 1:
                    st.markdown(f"### 🥇")
                elif rank == 2:
                    st.markdown(f"### 🥈")
                elif rank == 3:
                    st.markdown(f"### 🥉")
                else:
                    st.markdown(f"### **{rank}**")

            with col2:
                # Boat name and distance
                st.markdown(f"**{boat_name}**")
                st.caption(f"{distance:.1f} nm")

            # Add divider between boats
            if rank < len(latest_positions):
                st.divider()

def display_boat_stats(df):
    """Display boat statistics table"""
    if df is None or len(df) == 0:
        return

    # Get latest position for each boat
    latest_positions = df.sort_values('timestamp').groupby('boat_id').last().reset_index()

    # Get race course marks
    course_config = get_race_course_config()
    marks = course_config['marks']

    # Calculate total remaining distance along the race course for each boat
    latest_positions['total_remaining_distance'] = latest_positions.apply(
        lambda row: calculate_total_remaining_distance(
            row['latitude'], row['longitude'], row['current_mark_index'], marks
        ),
        axis=1
    )

    # Sort by total remaining distance (ascending) - boat with least distance is in 1st place
    # DNF boats go to the end
    latest_positions['sort_key'] = latest_positions.apply(
        lambda row: (
            float('inf') if row['race_status'] == 'dnf' else row['total_remaining_distance']
        ),
        axis=1
    )
    latest_positions = latest_positions.sort_values('sort_key')

    # Create display dataframe with the new total_remaining_distance column
    display_df = latest_positions[[
        'boat_name', 'boat_type', 'race_status', 'speed_over_ground_knots',
        'distance_traveled_nm', 'total_remaining_distance', 'vmg_knots',
        'marks_rounded', 'total_marks'
    ]].copy()

    display_df.columns = [
        'Boat', 'Type', 'Status', 'Speed (kt)', 'Distance (nm)',
        'To Dest (nm)', 'VMG (kt)', 'Marks', 'Total Marks'
    ]

    # Add position column
    display_df.insert(0, 'Pos', range(1, len(display_df) + 1))

    # Format marks column
    display_df['Marks'] = display_df.apply(lambda row: f"{row['Marks']}/{row['Total Marks']}", axis=1)
    display_df = display_df.drop('Total Marks', axis=1)

    # Round numeric columns
    display_df['Speed (kt)'] = display_df['Speed (kt)'].round(1)
    display_df['Distance (nm)'] = display_df['Distance (nm)'].round(1)
    display_df['To Dest (nm)'] = display_df['To Dest (nm)'].round(1)
    display_df['VMG (kt)'] = display_df['VMG (kt)'].round(1)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Status": st.column_config.TextColumn(
                "Status",
                help="Current race status",
            )
        }
    )

# Main app
def main():
    try:
        # Debug: Check environment variables (shown in sidebar for debugging)
        debug_mode = os.getenv("DEBUG", "false").lower() == "true"

        # Sidebar
        with st.sidebar:
            st.header("⚙️ Settings")

            # Manual refresh only
            auto_refresh = False
            st.info("🔄 Manual refresh mode - click 'Refresh Now' to update data")

            # Manual refresh button
            if st.button("🔄 Refresh Now"):
                st.rerun()

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

            # Time acceleration
            st.markdown(f"**Speed:** {TIME_ACCELERATION:.0f}x acceleration")

            st.markdown(f"**Course Marks:** {len(config['race_course_marks'])}")
            st.markdown(f"**Fleet Size:** {config['num_boats']} boats")

            st.divider()

            # Data source
            st.header("📊 Data Source")
            st.markdown(f"**Table:** `{TABLE_NAME}`")

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

        # Query data
        with st.spinner("Loading race data..."):
            df = query_telemetry()

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
            weather_data = query_weather_station()

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
                folium_map = create_race_map(df)
                if folium_map:
                    # Disable map interaction returns to prevent reruns on zoom/pan
                    st_folium(folium_map, width=None, height=700, returned_objects=[])

            with leaderboard_col:
                st.subheader("🏆 Leaderboard")
                display_leaderboard(df)

            st.divider()

            # Full boat statistics table
            st.subheader("📋 Boat Positions & Statistics")
            display_boat_stats(df)

            # Race timeline info
            st.caption(f"Race time: {latest_time.strftime('%Y-%m-%d %H:%M:%S UTC')} | Total telemetry records: {len(df):,}")

        else:
            st.warning("⚠️ No race data available. Make sure the telemetry generator is running and sending data to the table.")
            st.info("Run `python main.py` to start the race simulation.")

        # Auto-refresh - always refresh regardless of data availability
        if auto_refresh:
            time.sleep(REFRESH_INTERVAL)
            st.rerun()

    except Exception as e:
        st.error(f"❌ Application Error: {str(e)}")
        st.exception(e)
        st.info("🔄 Click 'Refresh Now' in the sidebar to retry...")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"Fatal error: {str(e)}")
        st.exception(e)
