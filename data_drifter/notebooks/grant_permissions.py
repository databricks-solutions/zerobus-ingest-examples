# Databricks notebook source
# MAGIC %md
# MAGIC # Grant Permissions to App Service Principal
# MAGIC
# MAGIC This notebook grants Unity Catalog permissions for the Streamlit app to access tables.
# MAGIC The app service principal needs SELECT on telemetry table and SELECT/MODIFY on weather table.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `catalog_name`: Unity Catalog name
# MAGIC - `schema_name`: Schema name within catalog
# MAGIC - `telemetry_table`: Full table name for telemetry data
# MAGIC - `weather_table`: Full table name for weather station data
# MAGIC - `app_service_principal_id`: Service principal ID of the deployed app

# COMMAND ----------

# Get parameters from DAB
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
telemetry_table = dbutils.widgets.get("telemetry_table")
weather_table = dbutils.widgets.get("weather_table")
app_service_principal_id = dbutils.widgets.get("app_service_principal_id")

print(f"Granting permissions to service principal: {app_service_principal_id}")
print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Telemetry table: {telemetry_table}")
print(f"Weather table: {weather_table}")

# COMMAND ----------

# Grant USE CATALOG permission
spark.sql(f"""
GRANT USE CATALOG ON CATALOG {catalog_name} TO `{app_service_principal_id}`
""")
print(f"✅ Granted USE CATALOG on {catalog_name}")

# COMMAND ----------

# Grant USE SCHEMA permission
spark.sql(f"""
GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema_name} TO `{app_service_principal_id}`
""")
print(f"✅ Granted USE SCHEMA on {catalog_name}.{schema_name}")

# COMMAND ----------

# Grant SELECT on telemetry table (read-only for app)
spark.sql(f"""
GRANT SELECT ON TABLE {telemetry_table} TO `{app_service_principal_id}`
""")
print(f"✅ Granted SELECT on {telemetry_table}")

# COMMAND ----------

# Grant SELECT and MODIFY on weather station table (app may write weather data)
spark.sql(f"""
GRANT SELECT, MODIFY ON TABLE {weather_table} TO `{app_service_principal_id}`
""")
print(f"✅ Granted SELECT, MODIFY on {weather_table}")

# COMMAND ----------

# Verify permissions were granted
print(f"\n✅ All permissions granted successfully to service principal: {app_service_principal_id}")
