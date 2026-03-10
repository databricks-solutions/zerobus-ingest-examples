# Databricks notebook source
# MAGIC %md
# MAGIC # Boat Performance Analysis
# MAGIC
# MAGIC ## Data Drifter Regatta - Sailboat Telemetry Analysis
# MAGIC
# MAGIC **Purpose**: Analyze individual boat performance and identify top performers.
# MAGIC
# MAGIC **Key Questions**:
# MAGIC - Which boats performed best overall?
# MAGIC - What are the completion times and distances for each boat?
# MAGIC - Which boats had the highest success rate?
# MAGIC - How do boats compare in speed and VMG?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Telemetry Data

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from race_utils import load_race_course_config, add_remaining_distance_column, load_race_data

# Load race course configuration from config.toml
TABLE_NAME, marks, config = load_race_course_config()

# Load telemetry data using utility function
df = load_race_data(TABLE_NAME)

print(f"Date range: {df.agg(F.min('timestamp'), F.max('timestamp')).collect()[0]}")
print(f"Number of boats: {df.select('boat_id').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Remaining Distance to Finish

# COMMAND ----------

# Add remaining distance to dataframe using utility function
df = add_remaining_distance_column(df, marks)
print("✓ Calculated remaining distance to finish for all telemetry records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boat Performance Overview

# COMMAND ----------

# Calculate overall boat performance metrics
boat_performance = df.groupBy("boat_id", "boat_name").agg(
    F.count("*").alias("total_observations"),
    F.avg("distance_traveled_nm").alias("avg_distance_sailed"),
    F.max("distance_traveled_nm").alias("final_distance"),
    F.avg("speed_over_ground_knots").alias("avg_speed"),
    F.avg("vmg_knots").alias("avg_vmg"),
    F.max("has_finished").alias("finished")
).orderBy("final_distance")

display(boat_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finish Analysis

# COMMAND ----------

# Get finished boats with rankings using utility function
from race_utils import get_finished_boats

finished_boats = get_finished_boats(df)

print("=" * 60)
print("FINISHED BOATS (by finish time)")
print("=" * 60)
for row in finished_boats.collect():
    medal = (
        "🥇" if row['rank'] == 1
        else "🥈" if row['rank'] == 2
        else "🥉" if row['rank'] == 3
        else f"  {row['rank']}."
    )
    duration = row['race_duration_hours']
    if hasattr(duration, "total_seconds"):
        duration_hours = duration.total_seconds() / 3600
    else:
        duration_hours = float(duration)
    print(f"\n{medal} {row['boat_name']} ({row['boat_id']})")
    print(f"   Distance Sailed: {row['total_distance']:.2f} nm")
    print(f"   Race Duration: {duration_hours:.2f} hours")
    print(f"   Final Remaining Distance: {row['final_remaining_distance']:.4f} nm")

display(finished_boats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Speed and VMG Analysis

# COMMAND ----------

# Analyze speed and VMG by boat
speed_vmg_analysis = df.groupBy("boat_id", "boat_name").agg(
    F.avg("speed_over_ground_knots").alias("avg_speed"),
    F.max("speed_over_ground_knots").alias("max_speed"),
    F.avg("vmg_knots").alias("avg_vmg"),
    F.stddev("speed_over_ground_knots").alias("speed_consistency")
).orderBy(F.desc("avg_vmg"))

display(speed_vmg_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations

# COMMAND ----------

import matplotlib.pyplot as plt

# Bar chart: Average VMG by boat
vmg_plot = speed_vmg_analysis.orderBy(F.desc("avg_vmg")).toPandas()

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.barh(vmg_plot['boat_name'], vmg_plot['avg_vmg'])

# Color top 3 differently
for i, bar in enumerate(bars):
    if i < 3:
        bar.set_color('green')
    else:
        bar.set_color('lightgreen')

ax.set_xlabel('Average VMG (knots)')
ax.set_ylabel('Boat')
ax.set_title('Boat Performance: Average Velocity Made Good')
ax.grid(axis='x', alpha=0.3)

plt.tight_layout()
display(fig)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results for Downstream Notebooks

# COMMAND ----------

# Derive schema from TABLE_NAME (catalog.schema.table -> catalog.schema)
SCHEMA_PREFIX = ".".join(TABLE_NAME.split(".")[:2])

# Save boat performance summary as tables
boat_performance.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.boat_performance_summary")
finished_boats.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.finished_boats_summary")

print("Boat performance results saved to tables:")
print(f"  - {SCHEMA_PREFIX}.boat_performance_summary")
print(f"  - {SCHEMA_PREFIX}.finished_boats_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Findings
# MAGIC
# MAGIC **Summary**:
# MAGIC - Analyzed performance of all boats in the fleet
# MAGIC - Evaluated based on distance sailed, race completion time, speed, and VMG
# MAGIC - Identified top performing boats
# MAGIC
# MAGIC **Insights**:
# MAGIC - VMG is a key differentiator between top performers
# MAGIC - Consistent speed and efficiency determine race outcomes
# MAGIC
# MAGIC **Next Steps**:
# MAGIC - Analyze performance by wind conditions (see 02_wind_conditions notebook)
# MAGIC - Examine race phase performance differences (see 03_race_progress notebook)
# MAGIC - View comprehensive summary (see 04_race_summary notebook)
