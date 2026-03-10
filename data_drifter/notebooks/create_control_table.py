# Databricks notebook source
# MAGIC %md
# MAGIC # Create Race Control Table
# MAGIC
# MAGIC Simple control table that allows the app to send speed commands to the telemetry generator.
# MAGIC The generator polls this table periodically and adjusts its playback speed.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `telemetry_table`: Full telemetry table name (used to derive schema prefix)

# COMMAND ----------

telemetry_table = dbutils.widgets.get("telemetry_table")
schema_prefix = ".".join(telemetry_table.split(".")[:2])
control_table = f"{schema_prefix}.race_control"

print(f"Creating control table: {control_table}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_table} (
  speed_multiplier DOUBLE COMMENT 'Playback speed multiplier (0.5 = half speed, 2.0 = double speed)',
  updated_at TIMESTAMP COMMENT 'When the speed was last changed',
  updated_by STRING COMMENT 'Who changed the speed (app or manual)'
)
USING DELTA
COMMENT 'Race control table - allows app to control telemetry generator speed'
""")

# Insert default row if table is empty
count = spark.sql(f"SELECT COUNT(*) as cnt FROM {control_table}").collect()[0].cnt
if count == 0:
    spark.sql(f"""
    INSERT INTO {control_table} VALUES (1.0, current_timestamp(), 'system')
    """)
    print("Inserted default speed_multiplier = 1.0")

print(f"Control table created: {control_table}")
