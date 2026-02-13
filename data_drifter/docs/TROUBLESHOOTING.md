# Troubleshooting Guide

Common issues and solutions for the Data Drifter Regatta project.

---

## Quick Diagnosis

**Problem category:**
- [Installation & Setup](#installation--setup)
- [Configuration Errors](#configuration-errors)
- [Deployment Issues](#deployment-issues)
- [No Data Being Generated](#no-data-being-generated)
- [Permission Denied](#permission-denied)
- [App Issues](#app-issues)
- [Performance Problems](#performance-problems)

---

## Installation & Setup

### Python Version Issues

**Problem:** ImportError or syntax errors when running scripts
```
SyntaxError: invalid syntax
```

**Solution:**
- Ensure Python 3.8 or newer is installed
- Check version: `python3 --version`
- If using older Python, upgrade or use pyenv

### Missing Dependencies

**Problem:** ModuleNotFoundError when running scripts
```
ModuleNotFoundError: No module named 'databricks'
```

**Solution:**
```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### Databricks CLI Not Installed

**Problem:** Command not found when running deployment
```
bash: databricks: command not found
```

**Solution:**
- Install Databricks CLI: [Installation Guide](https://docs.databricks.com/en/dev-tools/cli/install.html)
- Verify installation: `databricks --version` (should be 0.210.0 or newer)
- For Apps support, you need CLI version 0.210.0+

---

## Configuration Errors

### Missing config.toml

**Problem:** Config file not found
```
FileNotFoundError: config.toml not found
```

**Solution:**
- Ensure `config.toml` exists in project root
- Copy from template if missing: `cp config.toml.template config.toml`
- Edit with your actual values

### Invalid TOML Syntax

**Problem:** TOML parsing error
```
TOMLDecodeError: Invalid TOML
```

**Solution:**
- Validate TOML syntax online: [TOML Validator](https://www.toml-lint.com/)
- Common issues:
  - Missing quotes around strings
  - Mismatched brackets in arrays
  - Invalid escape sequences

### Missing Required Fields

**Problem:** KeyError when loading config
```
KeyError: 'table_name'
```

**Solution:**
Check config.toml has these required fields:
```toml
[zerobus]
server_endpoint = "your-endpoint.cloud.databricks.com"
workspace_url = "https://your-workspace.cloud.databricks.com"
table_name = "catalog.schema.table_name"
weather_station_table_name = "catalog.schema.weather_station"

[warehouse]
sql_warehouse_id = "your-warehouse-id"
```

### Invalid Table Names

**Problem:** Table name format error
```
Invalid table name format
```

**Solution:**
- Use fully qualified format: `catalog.schema.table`
- Example: `main.sailboat.telemetry`
- Do not use special characters except underscores

---

## Deployment Issues

### Bundle Deployment Fails

**Problem:** Warning about unknown field "app"
```
Warning: unknown field: app
```

**Solution:**
- Your Databricks CLI is outdated
- Update to version 0.210.0 or newer:
  - **Homebrew:** `brew upgrade databricks`
  - **Other:** See [installation guide](https://docs.databricks.com/en/dev-tools/cli/install.html)
- Verify: `databricks --version`

### Authentication Errors During Deployment

**Problem:** Databricks CLI not authenticated
```
Error: Authentication required
```

**Solution:**
```bash
# Reconfigure authentication
databricks configure --token

# Enter when prompted:
# - Databricks Host: https://your-workspace.cloud.databricks.com
# - Token: (generate from User Settings → Developer → Access Tokens)

# Test connection
databricks workspace list /
```

### Table Creation Fails

**Problem:** Permission error when creating tables
```
Error: CREATE TABLE permission denied
```

**Solution:**
- Ensure your user has CREATE TABLE permission in Unity Catalog
- Check permissions on catalog and schema:
  ```sql
  SHOW GRANTS ON CATALOG <catalog_name>;
  SHOW GRANTS ON SCHEMA <catalog_name>.<schema_name>;
  ```
- Request permissions from admin if needed

### Permission Grant Job Fails

**Problem:** App service principal not found
```
Error: Service principal ID not found
```

**Solution:**
- App may still be initializing after deployment
- Wait 30-60 seconds after app deployment
- Get service principal ID manually:
  ```bash
  APP_SP_ID=$(databricks apps get data-drifter-regatta --output json | jq -r '.service_principal_client_id')
  echo $APP_SP_ID
  ```
- Re-run permission grant:
  ```bash
  databricks bundle run grant_permissions --var app_service_principal_id=$APP_SP_ID
  ```

---

## No Data Being Generated

### Telemetry Generator Not Running

**Problem:** No data appears in table or app

**Check if generator is running:**
```bash
# Should see output like:
# ✓ Sent 60 records via SDK (gRPC)
python3 main.py --client-id <id> --client-secret <secret>
```

**Solution:**
- Ensure OAuth credentials are correct
- Check Zerobus endpoint is reachable
- Verify table exists: `SELECT COUNT(*) FROM <table_name>`

### OAuth Authentication Fails

**Problem:** Authentication error in telemetry generator
```
Error: OAuth authentication failed
```

**Solution:**
- Verify `client_id` and `client_secret` are correct
- Check service principal has required permissions:
  - SELECT on telemetry table
  - MODIFY on telemetry table (for inserts)
- Recreate credentials if expired

### Connection Timeout

**Problem:** Timeout connecting to Zerobus endpoint
```
Error: Connection timeout to Zerobus endpoint
```

**Solution:**
- Check `server_endpoint` in config.toml is correct
- Verify network connectivity:
  ```bash
  # Test if endpoint is reachable
  ping your-endpoint.cloud.databricks.com
  ```
- Check firewall/proxy settings if behind corporate network
- Verify Zerobus endpoint is active in your workspace

### Table Doesn't Exist

**Problem:** Table not found error
```
Error: Table <catalog>.<schema>.<table> not found
```

**Solution:**
```bash
# Run table creation job
databricks bundle run create_tables

# Verify table exists
databricks sql execute \
  --warehouse-id <warehouse-id> \
  "SHOW TABLES IN <catalog>.<schema>"
```

---

## Permission Denied

### SELECT Permission Missing

**Problem:** Cannot read from table
```
Error: User does not have SELECT permission
```

**Solution:**
- Grant SELECT permission to app service principal:
  ```sql
  GRANT SELECT ON TABLE <catalog>.<schema>.<table> TO `<service_principal_id>`;
  ```
- Or re-run permission grant job:
  ```bash
  databricks bundle run grant_permissions --var app_service_principal_id=<SP_ID>
  ```

### MODIFY Permission Missing

**Problem:** Cannot write to table
```
Error: User does not have MODIFY permission
```

**Solution:**
- Grant MODIFY permission:
  ```sql
  GRANT MODIFY ON TABLE <catalog>.<schema>.<table> TO `<service_principal_id>`;
  ```

### Warehouse Permission Missing

**Problem:** Cannot use SQL warehouse
```
Error: User does not have CAN_USE permission on warehouse
```

**Solution:**
- Go to Databricks UI → SQL Warehouses
- Select your warehouse → Permissions tab
- Grant CAN_USE to app service principal

---

## App Issues

### App Shows "No Data"

**Problem:** App loads but shows empty or "No data" message

**Diagnosis steps:**
1. **Check if telemetry generator is running**
   ```bash
   # Should see continuous output
   python3 main.py --client-id <id> --client-secret <secret>
   ```

2. **Verify table has data**
   ```bash
   databricks sql execute \
     --warehouse-id <warehouse-id> \
     "SELECT COUNT(*) FROM <table_name>"
   ```

3. **Check SQL warehouse is running**
   - Go to Databricks UI → SQL Warehouses
   - Ensure warehouse state is "Running"
   - Start if stopped

4. **Wait for initial data**
   - First telemetry records may take 10-30 seconds to appear
   - Click "Refresh Now" button in app

**Solution:**
- If no data in table: Start telemetry generator
- If warehouse stopped: Start warehouse
- If data exists but not showing: Check app permissions (SELECT on table)

### Map Not Loading

**Problem:** Map area is blank or shows error

**Possible causes:**
- No GPS coordinates in data
- Coordinates out of valid range (-90 to 90 lat, -180 to 180 lon)
- JavaScript errors

**Solution:**
- Check browser console for JavaScript errors (F12)
- Verify data has valid lat/lon:
  ```sql
  SELECT latitude, longitude FROM <table_name> LIMIT 10
  ```
- Clear browser cache and reload

### App Connection Errors

**Problem:** "Connection failed" or authentication errors in app

**Solution:**
- Verify SQL warehouse is running
- Check app service principal has:
  - CAN_USE on warehouse
  - SELECT on telemetry table
- Re-grant permissions if needed:
  ```bash
  databricks bundle run grant_permissions
  ```

---

## Performance Problems

### Slow Query Performance

**Problem:** App takes long time to load data

**Solutions:**

1. **Reduce query limit**
   - Edit `app/app.py` and change:
     ```python
     df = query_telemetry(limit=10000)  # Reduce to 5000 or 1000
     ```

2. **Enable result caching**
   - Go to SQL Warehouse settings
   - Enable "Result caching"

3. **Use serverless SQL warehouse**
   - Better auto-scaling
   - Faster startup

4. **Add time-based filtering**
   ```sql
   SELECT * FROM table
   WHERE timestamp > UNIX_MICROS(CURRENT_TIMESTAMP() - INTERVAL 1 HOUR)
   ```

### High Memory Usage

**Problem:** App or generator consumes too much memory

**Solutions:**

1. **For App:**
   - Reduce query limit
   - Filter to fewer boats
   - Query shorter time windows

2. **For Generator:**
   - Reduce fleet size in config.toml
   - Increase send_interval_seconds
   - Reduce batch_size

### Warehouse Cost Concerns

**Problem:** SQL warehouse costs are high

**Solutions:**
- Use serverless warehouse (pay per query)
- Set auto-stop timeout (e.g., 10 minutes)
- Use smaller warehouse size for development
- Query only recent data instead of full history

---

## Common Error Messages

### "databricks: command not found"
→ [Install Databricks CLI](#databricks-cli-not-installed)

### "unknown field: app"
→ [Update Databricks CLI](#bundle-deployment-fails)

### "OAuth authentication failed"
→ [Fix OAuth credentials](#oauth-authentication-fails)

### "Table not found"
→ [Create tables](#table-doesnt-exist)

### "Permission denied"
→ [Grant permissions](#permission-denied)

### "Connection timeout"
→ [Check network connectivity](#connection-timeout)

### "No data"
→ [Start telemetry generator](#app-shows-no-data)

---

## Getting Additional Help

If you're still stuck after trying these solutions:

1. **Check Databricks documentation:**
   - [Apps Documentation](https://docs.databricks.com/apps/index.html)
   - [Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
   - [Unity Catalog](https://docs.databricks.com/unity-catalog/index.html)

2. **Enable verbose logging:**
   ```bash
   # Deployment
   databricks bundle deploy --verbose

   # App debugging
   export DEBUG=true
   streamlit run app/app.py
   ```

3. **Check logs:**
   - App logs: Databricks UI → Apps → Data Drifter Regatta → Logs
   - Job logs: Databricks UI → Workflows → Job runs

4. **Review configuration:**
   - Validate config.toml has all required fields
   - Check table names match between config and deployment
   - Verify endpoint URLs are correct

---

## Prevention Tips

### Before Deployment:
- ✅ Validate config.toml syntax
- ✅ Check Unity Catalog permissions
- ✅ Verify SQL warehouse is available
- ✅ Test Databricks CLI authentication
- ✅ Ensure CLI version is 0.210.0+

### After Deployment:
- ✅ Wait 30 seconds for app to initialize
- ✅ Verify tables were created
- ✅ Check permissions were granted
- ✅ Start telemetry generator with correct OAuth creds
- ✅ Confirm data appears in table before checking app

### Regular Maintenance:
- ✅ Monitor warehouse costs
- ✅ Archive old telemetry data periodically
- ✅ Update Databricks CLI regularly
- ✅ Rotate OAuth credentials periodically
- ✅ Review and optimize queries
