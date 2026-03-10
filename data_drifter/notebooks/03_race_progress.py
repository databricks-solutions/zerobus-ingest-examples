# Databricks notebook source
# MAGIC %md
# MAGIC # Race Progress Analysis
# MAGIC
# MAGIC ## Data Drifter Regatta - Sailboat Telemetry Analysis
# MAGIC
# MAGIC **Purpose**: Analyze how boats progressed through the race and performed on different legs.
# MAGIC
# MAGIC **Key Questions**:
# MAGIC - How did boat positions change throughout the race?
# MAGIC - Which boats started strong vs. finished strong?
# MAGIC - How did performance vary by race leg?
# MAGIC - Which boats were most consistent?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Telemetry Data

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from race_utils import load_race_course_config, add_remaining_distance_column, load_race_data, get_schema_prefix

# Load race course configuration from config.toml
TABLE_NAME, marks, config = load_race_course_config()
total_marks = len(marks) - 1
print(f"Number of legs: {len(marks)}")

# Load telemetry data using utility function
df = load_race_data(TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Remaining Distance to Finish

# COMMAND ----------

# Add remaining distance to dataframe using utility function
df = add_remaining_distance_column(df, marks)
print("✓ Calculated remaining distance to finish for all telemetry records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Position Tracking Over Time

# COMMAND ----------

# Calculate position rankings at each timestamp based on remaining distance to finish
# Lower remaining distance = better position (ahead in the race)
positions = df.withColumn(
    "race_position",
    F.row_number().over(
        Window.partitionBy("timestamp").orderBy(F.col("remaining_distance_nm"))
    )
)

# Calculate position changes over time
position_changes = positions.withColumn(
    "prev_position",
    F.lag("race_position").over(Window.partitionBy("boat_id").orderBy("timestamp"))
).withColumn(
    "position_change",
    F.col("prev_position") - F.col("race_position")  # Positive means gained positions (moved up)
).filter(F.col("prev_position").isNotNull())

# Aggregate position changes by boat
position_summary = position_changes.groupBy("boat_id", "boat_name").agg(
    F.sum(F.when(F.col("position_change") > 0, 1).otherwise(0)).alias("positions_gained"),
    F.sum(F.when(F.col("position_change") < 0, 1).otherwise(0)).alias("positions_lost"),
    F.sum(F.when(F.col("position_change") == 0, 1).otherwise(0)).alias("positions_held"),
    F.avg("position_change").alias("avg_position_change")
).orderBy(F.desc("positions_gained"))

display(position_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance by Race Leg

# COMMAND ----------

# Analyze VMG performance by boat and leg
vmg_by_leg = df.groupBy("boat_id", "boat_name", "current_mark_index").agg(
    F.avg("vmg_knots").alias("avg_vmg"),
    F.avg("speed_over_ground_knots").alias("avg_speed"),
    F.count("*").alias("observations")
).withColumnRenamed("current_mark_index", "leg").orderBy("boat_id", "leg")

display(vmg_by_leg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Early vs. Late Race Performance

# COMMAND ----------

# Categorize legs into Early/Middle/Late
df_with_phase = df.withColumn("race_phase",
    F.when(F.col("current_mark_index") <= 3, "Early Legs")
     .when(F.col("current_mark_index") <= 6, "Middle Legs")
     .otherwise("Late Legs")
)

# Analyze VMG by phase
vmg_by_phase = df_with_phase.groupBy("boat_id", "boat_name", "race_phase").agg(
    F.avg("vmg_knots").alias("avg_vmg"),
    F.avg("speed_over_ground_knots").alias("avg_speed")
).orderBy("boat_id", "race_phase")

display(vmg_by_phase)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Strong vs. Finish Strong

# COMMAND ----------

# Compare first leg vs. last leg VMG
first_leg = vmg_by_leg.filter(F.col("leg") == 0).withColumn(
    "first_rank",
    F.row_number().over(Window.orderBy(F.desc("avg_vmg")))
).select("boat_id", "boat_name", "first_rank", F.col("avg_vmg").alias("first_vmg"))

last_leg = vmg_by_leg.filter(F.col("leg") == total_marks).withColumn(
    "last_rank",
    F.row_number().over(Window.orderBy(F.desc("avg_vmg")))
).select("boat_id", "boat_name", "last_rank", F.col("avg_vmg").alias("last_vmg"))

# Join and analyze trend
leg_trend = first_leg.join(last_leg, ["boat_id", "boat_name"], "outer") \
    .fillna({"first_rank": 999, "last_rank": 999, "first_vmg": 0.0, "last_vmg": 0.0}) \
    .withColumn(
        "rank_change",
        F.col("first_rank") - F.col("last_rank")  # Positive means improved
    ).withColumn(
        "performance_trend",
        F.when(F.col("rank_change") > 1, "Strong Finish")
         .when(F.col("rank_change") < -1, "Strong Start")
         .otherwise("Consistent")
    ).orderBy(F.desc("rank_change"))

print("=" * 60)
print("PERFORMANCE TRENDS (First Leg vs. Last Leg)")
print("=" * 60)
for row in leg_trend.collect():
    print(f"\n{row['boat_name']} - {row['performance_trend']}")
    print(f"  First leg rank: #{row['first_rank']} (VMG: {row['first_vmg']:.2f})")
    print(f"  Last leg rank: #{row['last_rank']} (VMG: {row['last_vmg']:.2f})")
    print(f"  Rank change: {row['rank_change']:+d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consistency Analysis

# COMMAND ----------

# Read consistency thresholds from config
_very_consistent = config.get("analysis", {}).get("consistency_very_consistent", 0.1)
_consistent = config.get("analysis", {}).get("consistency_consistent", 0.2)
_variable = config.get("analysis", {}).get("consistency_variable", 0.3)

# Calculate VMG consistency across legs
consistency = vmg_by_leg.groupBy("boat_id", "boat_name").agg(
    F.avg("avg_vmg").alias("mean_vmg"),
    F.stddev("avg_vmg").alias("stddev_vmg"),
    F.count("*").alias("legs_completed")
).withColumn(
    "coefficient_of_variation",
    F.col("stddev_vmg") / F.abs(F.col("mean_vmg"))
).withColumn(
    "consistency_rating",
    F.when(F.col("coefficient_of_variation") < _very_consistent, "Very Consistent")
     .when(F.col("coefficient_of_variation") < _consistent, "Consistent")
     .when(F.col("coefficient_of_variation") < _variable, "Moderate")
     .otherwise("Variable")
).orderBy("coefficient_of_variation")

display(consistency)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations

# COMMAND ----------

# Line chart: VMG across legs by boat
import matplotlib.pyplot as plt
import seaborn as sns

plot_data = vmg_by_leg.orderBy("boat_id", "leg").toPandas()

fig, ax = plt.subplots(figsize=(14, 8))

for boat in plot_data['boat_name'].unique():
    boat_data = plot_data[plot_data['boat_name'] == boat]
    ax.plot(boat_data['leg'], boat_data['avg_vmg'], marker='o', label=boat)

ax.set_xlabel('Leg Number')
ax.set_ylabel('Average VMG (knots)')
ax.set_title('VMG Performance Across Race Legs by Boat')
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
ax.grid(alpha=0.3)

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

# Derive schema from TABLE_NAME (catalog.schema.table -> catalog.schema)
SCHEMA_PREFIX = get_schema_prefix(TABLE_NAME)

# Save race progress analysis results as tables
positions.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.race_positions_over_time")
vmg_by_leg.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.race_leg_vmg")
leg_trend.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.race_leg_trends")
consistency.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.race_leg_consistency")

print("Race progress analysis results saved to tables:")
print(f"  - {SCHEMA_PREFIX}.race_positions_over_time")
print(f"  - {SCHEMA_PREFIX}.race_leg_vmg")
print(f"  - {SCHEMA_PREFIX}.race_leg_trends")
print(f"  - {SCHEMA_PREFIX}.race_leg_consistency")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Findings
# MAGIC
# MAGIC **Summary**:
# MAGIC - Tracked position changes throughout the race
# MAGIC - Analyzed performance across individual legs between marks
# MAGIC - Identified "start strong" vs. "finish strong" boats
# MAGIC - Evaluated consistency across different legs
# MAGIC
# MAGIC **Insights**:
# MAGIC - Some boats improve as race progresses, others fade
# MAGIC - Position changes reveal tactical effectiveness
# MAGIC - Consistency varies - some maintain performance, others fluctuate
