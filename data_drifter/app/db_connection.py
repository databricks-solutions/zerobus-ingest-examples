"""
Database connection and query logic for the Data Drifter Regatta app.

Usage:
    import db_connection
    db_connection.init(config, table_name)
    df = db_connection.query_telemetry()
"""

import os
import time
import logging
import threading

import streamlit as st
from databricks.sdk.core import Config, oauth_service_principal

# Import databricks-sql-connector with error handling
try:
    from databricks import sql
    from databricks.sdk import WorkspaceClient
except ImportError as e:
    st.error(f"Failed to import databricks modules: {str(e)}")
    st.info("Please ensure databricks-sql-connector and databricks-sdk are installed in requirements.txt")
    st.stop()

logger = logging.getLogger(__name__)

# Module-level state set by init()
_config = None
_table_name = None


def init(config, table_name):
    """Initialize the db_connection module with config and table name."""
    global _config, _table_name
    _config = config
    _table_name = table_name


def get_connection():
    """Create Databricks SQL connection using workspace authentication"""
    logger.info("Starting SQL warehouse connection attempt")
    debug_mode = os.getenv("DEBUG", "false").lower() == "true"
    start_time = time.time()

    try:
        if debug_mode:
            st.write("Debug - Step 1: Checking environment variables")
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
        warehouse_id = _config["warehouse_id"]

        # Extract server hostname from workspace URL (remove https:// prefix)
        workspace_url = _config.get("workspace_url", os.getenv("DATABRICKS_HOST", ""))
        if workspace_url.startswith("https://"):
            server_hostname = workspace_url.replace("https://", "")
        elif workspace_url.startswith("http://"):
            server_hostname = workspace_url.replace("http://", "")
        else:
            server_hostname = workspace_url

        if debug_mode:
            st.write(f"Debug - Step 2: Connection parameters")
            st.write(f"  Server hostname: {server_hostname}")
            st.write(f"  Warehouse ID: {warehouse_id}")
            st.write(f"  HTTP Path: /sql/1.0/warehouses/{warehouse_id}")

        # Method 1: Try using OAuth M2M with client credentials (recommended for Databricks Apps)
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

        if client_id and client_secret:
            logger.info("Attempting OAuth M2M authentication (Method 1)")
            if debug_mode:
                st.write("Debug - Step 3: Attempting connection with OAuth M2M (client credentials)...")
                st.write(f"  DATABRICKS_CLIENT_ID found: {client_id[:8]}...")
                st.write("  DATABRICKS_CLIENT_SECRET found")

            try:
                credential_provider = lambda: oauth_service_principal(Config(
                    host          = f"https://{server_hostname}",
                    client_id     = client_id,
                    client_secret = client_secret))

                connection = sql.connect(
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
                    st.success(f"OAuth M2M authentication successful! Result: {result}")

                logger.info(f"OAuth M2M authentication successful in {time.time() - start_time:.2f}s")
                return connection

            except Exception as oauth_error:
                logger.warning(f"OAuth M2M auth failed: {str(oauth_error)}")
                if debug_mode:
                    st.error(f"OAuth M2M auth failed: {str(oauth_error)}")
                    st.write("  Falling back to alternative auth methods...")
        else:
            if debug_mode:
                st.write("Debug - Step 3: Client credentials not found in environment")
                if not client_id:
                    st.write("  DATABRICKS_CLIENT_ID not set")
                if not client_secret:
                    st.write("  DATABRICKS_CLIENT_SECRET not set")
                st.write("  Attempting connection with SDK default auth...")

        # Method 2: Try SDK automatic authentication
        logger.info("Attempting SDK OAuth authentication (Method 2)")
        try:
            if debug_mode:
                st.write("  Initializing WorkspaceClient for auth...")

            from databricks.sdk import WorkspaceClient
            from databricks.sdk.core import ApiClient

            cfg = Config()

            if debug_mode:
                st.write(f"  Config host: {cfg.host if cfg.host else 'auto-detect'}")
                st.write("  Creating API client...")

            api_client = ApiClient(cfg)

            if debug_mode:
                st.write("  Getting auth headers...")

            def get_token():
                headers = api_client.do("GET", "/api/2.0/preview/scim/v2/Me",
                                       headers={}, data={}, raw=True).headers
                auth_header = headers.get('Authorization', '')
                if auth_header.startswith('Bearer '):
                    return auth_header[7:]
                return None

            if debug_mode:
                st.write("  Connecting to SQL Warehouse...")

            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                auth_type="databricks-oauth",
                _socket_timeout=30
            )

            if debug_mode:
                st.write("  Connection object created, testing query...")

            cursor = connection.cursor()
            cursor.execute("SELECT 'test' as status, current_database() as db")
            result = cursor.fetchone()
            cursor.close()

            if debug_mode:
                st.success(f"Connection successful! Result: {result}")

            logger.info(f"SDK OAuth authentication successful in {time.time() - start_time:.2f}s")
            return connection

        except Exception as sdk_error:
            logger.warning(f"SDK OAuth auth failed: {str(sdk_error)}")
            if debug_mode:
                st.error(f"SDK OAuth auth method failed: {str(sdk_error)}")
                st.write(f"  Error type: {type(sdk_error).__name__}")
                st.exception(sdk_error)

        # Method 3: Try WorkspaceClient with better error handling
        logger.info("Attempting WorkspaceClient authentication (Method 3)")
        if debug_mode:
            st.write("Method 3: Trying WorkspaceClient authentication...")

        try:
            w = WorkspaceClient(host=f"https://{server_hostname}")

            if debug_mode:
                st.write(f"  WorkspaceClient created")
                st.write(f"  Host: {w.config.host}")
                try:
                    auth_details = str(w.config)
                    st.write(f"  Config: {auth_details[:200]}")
                except:
                    pass

            if debug_mode:
                st.write("  Attempting authentication...")

            try:
                credentials = w.config.authenticate()
                if debug_mode:
                    st.write(f"  Authentication successful, got credentials")

                connection = sql.connect(
                    server_hostname=server_hostname,
                    http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                    credentials_provider=lambda: credentials,
                    _socket_timeout=30
                )

                if debug_mode:
                    st.write("  SQL Connection created, testing...")

                cursor = connection.cursor()
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                cursor.close()

                if debug_mode:
                    st.success(f"Method 3 successful! Test query returned: {result}")

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
                st.error(f"Method 3 failed: {str(wc_error)}")
                st.write(f"  Error type: {type(wc_error).__name__}")
                st.exception(wc_error)

        # Method 4: Try OAuth U2M flow (for apps with attached resources)
        logger.info("Attempting OAuth U2M flow (Method 4)")
        if debug_mode:
            st.write("Method 4: Trying OAuth U2M flow...")

        try:
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                auth_type="databricks-oauth",
                _socket_timeout=30
            )

            if debug_mode:
                st.write("  OAuth connection created, testing...")

            cursor = connection.cursor()
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            cursor.close()

            if debug_mode:
                st.success(f"Method 4 successful! Test query returned: {result}")

            logger.info(f"OAuth U2M authentication successful in {time.time() - start_time:.2f}s")
            return connection

        except Exception as oauth_error:
            logger.warning(f"OAuth U2M auth failed: {str(oauth_error)}")
            if debug_mode:
                st.error(f"Method 4 failed: {str(oauth_error)}")
                st.write(f"  Error type: {type(oauth_error).__name__}")
                st.exception(oauth_error)

        # All methods failed
        logger.error(f"All connection methods failed after {time.time() - start_time:.2f}s")
        if debug_mode:
            st.error("All connection methods failed!")
            st.write("Troubleshooting suggestions:")
            st.write("1. Check that the SQL Warehouse resource is properly attached")
            st.write("2. Verify the warehouse is running and accessible")
            st.write("3. Ensure the app has CAN_USE permission on the warehouse")
            st.write("4. Check that environment variables are being set correctly")

        raise Exception("Unable to connect to Databricks SQL Warehouse. All authentication methods failed.")

    except Exception as e:
        st.error(f"Failed to connect to Databricks SQL: {str(e)}")
        st.info("Troubleshooting tips:")
        st.info("- Ensure the app has a SQL Warehouse resource with CAN_USE permission")
        st.info("- Check that the warehouse is running and accessible")
        st.info(f"- Warehouse ID: {warehouse_id}")
        if debug_mode:
            st.write("Full error details:")
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
            st.write("Step 1: Getting connection...")

        conn = get_connection()
        if not conn:
            logger.error("Failed to establish connection")
            st.warning("Could not establish database connection")
            return None

        logger.info(f"Step 1 complete in {time.time() - query_start:.2f}s")

        # Step 2: Count rows in table to verify we can query it
        logger.info(f"Step 2: Counting rows in {_table_name}")
        step2_start = time.time()

        try:
            count_query = f"SELECT COUNT(*) as row_count FROM {_table_name}"
            cursor = conn.cursor()
            cursor.execute(count_query)
            count_result = cursor.fetchone()
            row_count = count_result[0] if count_result else 0
            cursor.close()

            logger.info(f"Step 2 complete: {row_count:,} rows found in {time.time() - step2_start:.2f}s")

            if row_count == 0:
                logger.warning("Table is empty, no data available")
                st.warning("Table is empty. No telemetry data available yet.")
                st.info("Run `python main.py` to start generating telemetry data.")
                return None

        except Exception as count_error:
            logger.error(f"Failed to count rows: {str(count_error)}")
            st.error(f"Failed to count rows in table: {str(count_error)}")
            st.info("Check that:")
            st.info("  - The table exists")
            st.info("  - The service principal has SELECT permission on the table")
            st.info(f"  - Table name is correct: {_table_name}")
            if debug_mode:
                st.exception(count_error)
            return None

        if debug_mode:
            st.write(f"Step 3: Querying table: {_table_name}")

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
        FROM {_table_name}
        ORDER BY timestamp DESC
        LIMIT {limit}
        """

        # Step 3: Execute query with timeout
        logger.info(f"Step 3: Querying {_table_name} for {limit} rows")
        step3_start = time.time()

        if debug_mode:
            st.write("Step 4: Executing query with 60 second timeout...")

        cursor = conn.cursor()

        try:
            execute_query_with_timeout(cursor, query, timeout_seconds=60)
            execution_time = time.time() - step3_start

            logger.info(f"Step 3 query execution complete in {execution_time:.2f}s")

            if debug_mode:
                st.write(f"  Query executed in {execution_time:.2f} seconds")

        except TimeoutError as te:
            logger.error(f"Query timeout: {str(te)}")
            st.error(f"Query execution timeout: {str(te)}")
            st.warning("Troubleshooting suggestions:")
            st.info("- The SQL warehouse may be slow or overloaded")
            st.info("- Try reducing the data limit or adding filters")
            st.info("- Check SQL warehouse status in Databricks UI")
            st.info(f"- Table: {_table_name}")
            cursor.close()
            conn.close()
            return None

        # Step 4-5: Fetch results
        logger.info("Step 4-5: Fetching results as DataFrame")
        fetch_start = time.time()

        if debug_mode:
            st.write("Step 5: Fetching results...")

        df = cursor.fetchall_arrow().to_pandas()
        cursor.close()

        fetch_time = time.time() - fetch_start
        logger.info(f"Fetch complete: {len(df)} rows in {fetch_time:.2f}s")

        total_time = time.time() - query_start
        logger.info(f"Query completed successfully in {total_time:.2f}s total")

        if debug_mode:
            st.write(f"Step 6: Retrieved {len(df)} rows")

        return df

    except TimeoutError as te:
        logger.error(f"Query timeout after {time.time() - query_start:.2f}s: {str(te)}")
        return None
    except Exception as e:
        logger.error(f"Query failed after {time.time() - query_start:.2f}s: {str(e)}", exc_info=True)
        st.error(f"Failed to query data: {str(e)}")
        st.info("Check:")
        st.info(f"- Table exists and has data")
        st.info("- Service principal has SELECT permission")
        st.info(f"- Table name: {_table_name}")
        if debug_mode:
            st.exception(e)
        return None


def query_weather_station():
    """Query latest weather station data"""
    logger.info("Querying weather station data")

    weather_table = _config.get("weather_table_name")
    if not weather_table:
        logger.warning("Weather station table not configured")
        return None

    try:
        conn = get_connection()
        if not conn:
            logger.error("Failed to establish connection for weather station query")
            return None

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
        return result.iloc[0]

    except Exception as e:
        logger.error(f"Failed to query weather station: {str(e)}", exc_info=True)
        return None
