# Databricks notebook source
# MAGIC %md
# MAGIC # Wind Conditions Analysis
# MAGIC
# MAGIC ## Data Drifter Regatta - Sailboat Telemetry Analysis
# MAGIC
# MAGIC **Purpose**: Analyze how wind conditions affected boat performance throughout the race.
# MAGIC
# MAGIC **Key Questions**:
# MAGIC - How did boats perform in different wind conditions?
# MAGIC - Which boats handled heavy wind best?
# MAGIC - Which boats excelled in light wind?
# MAGIC - How does VMG vary with wind strength and direction?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Telemetry Data

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from race_utils import load_race_course_config, load_race_data

# Load race course configuration from config.toml
TABLE_NAME, marks, config = load_race_course_config()

# Load telemetry data using utility function
df = load_race_data(TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wind Condition Categorization

# COMMAND ----------

# Categorize wind conditions
df_with_wind = df.withColumn("wind_category",
    F.when(F.col("wind_speed_knots") < 8, "Light")
     .when(F.col("wind_speed_knots") < 15, "Moderate")
     .otherwise("Heavy")
)

# Check distribution
wind_distribution = df_with_wind.groupBy("wind_category").count().orderBy("wind_category")
display(wind_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance by Wind Condition

# COMMAND ----------

# Analyze performance by boat and wind condition
performance_by_wind = df_with_wind.groupBy("boat_id", "boat_name", "wind_category").agg(
    F.avg("vmg_knots").alias("avg_vmg"),
    F.avg("speed_over_ground_knots").alias("avg_speed"),
    F.count("*").alias("observations")
).orderBy("boat_id", "wind_category")

display(performance_by_wind)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Performers by Wind Condition

# COMMAND ----------

# Find top 3 boats for each wind condition
top_by_wind = performance_by_wind.withColumn(
    "rank",
    F.row_number().over(Window.partitionBy("wind_category").orderBy(F.desc("avg_vmg")))
).filter(F.col("rank") <= 3)

print("=" * 60)
print("TOP 3 BOATS BY WIND CONDITION")
print("=" * 60)
for wind_cat in ["Light", "Moderate", "Heavy"]:
    print(f"\n{wind_cat} Wind:")
    rows = top_by_wind.filter(F.col("wind_category") == wind_cat).orderBy("rank").collect()
    for row in rows:
        print(f"  {row['rank']}. {row['boat_name']} - VMG: {row['avg_vmg']:.2f} kt, Speed: {row['avg_speed']:.2f} kt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wind Speed Distribution

# COMMAND ----------

# Analyze overall wind conditions during race
wind_stats = df.agg(
    F.min("wind_speed_knots").alias("min_wind"),
    F.avg("wind_speed_knots").alias("avg_wind"),
    F.max("wind_speed_knots").alias("max_wind"),
    F.stddev("wind_speed_knots").alias("wind_variability")
).collect()[0]

print("=" * 60)
print("WIND CONDITIONS SUMMARY")
print("=" * 60)
print(f"Minimum wind speed: {wind_stats['min_wind']:.1f} knots")
print(f"Average wind speed: {wind_stats['avg_wind']:.1f} knots")
print(f"Maximum wind speed: {wind_stats['max_wind']:.1f} knots")
print(f"Wind variability (stddev): {wind_stats['wind_variability']:.1f} knots")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations

# COMMAND ----------

# Heatmap: VMG by wind condition and boat
import matplotlib.pyplot as plt
import seaborn as sns

# Pivot data for heatmap
pivot_data = performance_by_wind.toPandas().pivot(
    index='boat_name',
    columns='wind_category',
    values='avg_vmg'
)

# Reorder columns
pivot_data = pivot_data[['Light', 'Moderate', 'Heavy']]

fig, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(pivot_data, annot=True, fmt='.2f', cmap='RdYlGn', center=0, ax=ax)
ax.set_title('Average VMG by Wind Condition and Boat')
ax.set_xlabel('Wind Condition')
ax.set_ylabel('Boat')

plt.tight_layout()
display(fig)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

# Derive schema from TABLE_NAME (catalog.schema.table -> catalog.schema)
SCHEMA_PREFIX = ".".join(TABLE_NAME.split(".")[:2])

# Save wind condition analysis results as tables
performance_by_wind.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.performance_by_wind")
top_by_wind.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.top_performers_by_wind")

print("Wind condition analysis results saved to tables:")
print(f"  - {SCHEMA_PREFIX}.performance_by_wind")
print(f"  - {SCHEMA_PREFIX}.top_performers_by_wind")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point of Sail Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point of Sail Classification
# MAGIC
# MAGIC **Point of Sail**: The angle between boat heading and true wind direction
# MAGIC - **Close-hauled** (30-50°): Sailing as close to wind as possible
# MAGIC - **Close Reach** (50-70°): Sailing slightly off the wind
# MAGIC - **Beam Reach** (70-110°): Sailing perpendicular to wind
# MAGIC - **Broad Reach** (110-150°): Sailing downwind at an angle
# MAGIC - **Running** (150-180°): Sailing directly downwind

# COMMAND ----------

# Apply point of sail classification using utility function
from race_utils import add_point_of_sail_column

df_with_sail = add_point_of_sail_column(df)

# Check distribution
sail_distribution = df_with_sail.groupBy("point_of_sail").count().orderBy("point_of_sail")
print("=" * 60)
print("POINT OF SAIL DISTRIBUTION (Telemetry Records)")
print("=" * 60)
for row in sail_distribution.collect():
    print(f"{row['point_of_sail']:15s}: {row['count']:,} records")

display(sail_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distance Sailed at Each Point of Sail

# COMMAND ----------

# Calculate distance delta between consecutive records for each boat
df_with_distance_delta = df_with_sail.withColumn(
    "prev_distance",
    F.lag("distance_traveled_nm").over(Window.partitionBy("boat_id").orderBy("timestamp"))
).withColumn(
    "distance_delta_nm",
    F.when(F.col("prev_distance").isNotNull(), F.col("distance_traveled_nm") - F.col("prev_distance")).otherwise(0)
)

# Calculate total distance sailed at each point of sail (fleet-wide)
distance_by_sail = df_with_distance_delta.groupBy("point_of_sail").agg(
    F.sum("distance_delta_nm").alias("total_distance_nm"),
    F.avg("distance_delta_nm").alias("avg_segment_distance_nm"),
    F.count("*").alias("observations")
).orderBy(F.desc("total_distance_nm"))

print("=" * 60)
print("DISTANCE SAILED BY POINT OF SAIL (Fleet Total)")
print("=" * 60)
for row in distance_by_sail.collect():
    pct = (row['total_distance_nm'] / distance_by_sail.agg(F.sum("total_distance_nm")).collect()[0][0]) * 100
    print(f"{row['point_of_sail']:15s}: {row['total_distance_nm']:8.2f} nm ({pct:5.1f}%)")

display(distance_by_sail)

# COMMAND ----------

# Calculate distance sailed at each point of sail by boat
distance_by_sail_boat = df_with_distance_delta.groupBy("boat_id", "boat_name", "point_of_sail").agg(
    F.sum("distance_delta_nm").alias("distance_sailed_nm"),
    F.count("*").alias("observations")
).orderBy("boat_id", "point_of_sail")

print("=" * 60)
print("DISTANCE SAILED BY BOAT AND POINT OF SAIL")
print("=" * 60)
for boat in df_with_sail.select("boat_name").distinct().orderBy("boat_name").collect():
    boat_name = boat['boat_name']
    print(f"\n{boat_name}:")
    boat_data = distance_by_sail_boat.filter(F.col("boat_name") == boat_name).orderBy(F.desc("distance_sailed_nm")).collect()
    for row in boat_data:
        if row['distance_sailed_nm'] > 0:
            print(f"  {row['point_of_sail']:15s}: {row['distance_sailed_nm']:6.2f} nm")

display(distance_by_sail_boat)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance by Point of Sail

# COMMAND ----------

# Analyze performance by boat and point of sail
performance_by_sail = df_with_sail.groupBy("boat_id", "boat_name", "point_of_sail").agg(
    F.avg("vmg_knots").alias("avg_vmg"),
    F.avg("speed_over_ground_knots").alias("avg_speed"),
    F.stddev("vmg_knots").alias("vmg_consistency"),
    F.count("*").alias("observations")
).orderBy("boat_id", "point_of_sail")

display(performance_by_sail)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Performers by Point of Sail

# COMMAND ----------

# Find top 3 boats for each point of sail
top_by_sail = performance_by_sail.withColumn(
    "rank",
    F.row_number().over(Window.partitionBy("point_of_sail").orderBy(F.desc("avg_vmg")))
).filter(F.col("rank") <= 3)

print("=" * 60)
print("TOP 3 BOATS BY POINT OF SAIL")
print("=" * 60)
for sail_type in ["Close-hauled", "Close Reach", "Beam Reach", "Broad Reach", "Running"]:
    rows = top_by_sail.filter(F.col("point_of_sail") == sail_type).orderBy("rank").collect()
    if rows:
        print(f"\n{sail_type}:")
        for row in rows:
            print(f"  {row['rank']}. {row['boat_name']}")
            print(f"      VMG: {row['avg_vmg']:.2f} kt, Speed: {row['avg_speed']:.2f} kt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boat Specializations

# COMMAND ----------

# Identify what point of sail each boat performs best at
boat_specialization = performance_by_sail.withColumn(
    "rank_within_boat",
    F.row_number().over(Window.partitionBy("boat_id", "boat_name").orderBy(F.desc("avg_vmg")))
).filter(F.col("rank_within_boat") == 1).select(
    "boat_id", "boat_name",
    F.col("point_of_sail").alias("best_point_of_sail"),
    F.col("avg_vmg").alias("best_vmg"),
    F.col("avg_speed").alias("best_speed")
).orderBy("boat_name")

print("=" * 60)
print("BOAT SPECIALIZATIONS (Best Point of Sail)")
print("=" * 60)
for row in boat_specialization.collect():
    print(f"\n{row['boat_name']}")
    print(f"  Best at: {row['best_point_of_sail']}")
    print(f"  VMG: {row['best_vmg']:.2f} kt, Speed: {row['best_speed']:.2f} kt")

display(boat_specialization)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combined Analysis: Wind Condition + Point of Sail

# COMMAND ----------

# Analyze by both wind condition and point of sail
combined_analysis = df_with_sail.withColumn("wind_category",
    F.when(F.col("wind_speed_knots") < 8, "Light")
     .when(F.col("wind_speed_knots") < 15, "Moderate")
     .otherwise("Heavy")
).groupBy("boat_id", "boat_name", "wind_category", "point_of_sail").agg(
    F.avg("vmg_knots").alias("avg_vmg"),
    F.avg("speed_over_ground_knots").alias("avg_speed"),
    F.count("*").alias("observations")
).orderBy("boat_id", "wind_category", "point_of_sail")

display(combined_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations - Point of Sail

# COMMAND ----------

# Heatmap: VMG by point of sail and boat
pivot_sail = performance_by_sail.toPandas().pivot(
    index='boat_name',
    columns='point_of_sail',
    values='avg_vmg'
)

# Reorder columns in sailing sequence
sail_order = ['Close-hauled', 'Close Reach', 'Beam Reach', 'Broad Reach', 'Running']
pivot_sail = pivot_sail[[col for col in sail_order if col in pivot_sail.columns]]

fig, ax = plt.subplots(figsize=(14, 8))
sns.heatmap(pivot_sail, annot=True, fmt='.2f', cmap='RdYlGn', center=0, ax=ax)
ax.set_title('Average VMG by Point of Sail and Boat')
ax.set_xlabel('Point of Sail')
ax.set_ylabel('Boat')

plt.tight_layout()
display(fig)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

# Save point of sail analysis results as tables
performance_by_sail.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.performance_by_point_of_sail")
top_by_sail.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.top_performers_by_sail")
boat_specialization.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.boat_specializations")
combined_analysis.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.performance_wind_and_sail")
distance_by_sail.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.distance_by_point_of_sail")
distance_by_sail_boat.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.distance_by_point_of_sail_boat")

print("Point of sail analysis results saved to tables:")
print(f"  - {SCHEMA_PREFIX}.performance_by_point_of_sail")
print(f"  - {SCHEMA_PREFIX}.top_performers_by_sail")
print(f"  - {SCHEMA_PREFIX}.boat_specializations")
print("  - performance_wind_and_sail")
print("  - distance_by_point_of_sail")
print("  - distance_by_point_of_sail_boat")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather Event Analysis
# MAGIC
# MAGIC Analyze how boats performed during different weather events (stable, frontal passage, gradual shift, gust)

# COMMAND ----------

# Load weather station data
from race_utils import load_weather_station_data, join_telemetry_with_weather, summarize_weather_events

weather_df = load_weather_station_data(config)

if weather_df is not None:
    # Display weather events
    print("=" * 60)
    print("WEATHER EVENTS SUMMARY")
    print("=" * 60)

    summary = summarize_weather_events(weather_df)
    if summary:
        print(f"\nTotal weather events recorded: {summary['total_events']}")
        print(f"\nWind conditions:")
        print(f"  Speed range: {summary['wind_speed_min']:.1f} - {summary['wind_speed_max']:.1f} knots")
        print(f"  Average speed: {summary['wind_speed_avg']:.1f} knots")
        print(f"  Direction range: {summary['wind_direction_min']:.0f}° - {summary['wind_direction_max']:.0f}°")
        print(f"  Average direction: {summary['wind_direction_avg']:.0f}°")

        print(f"\nEvent types:")
        for event_type, count in sorted(summary['event_counts'].items()):
            pct = (count / summary['total_events']) * 100
            print(f"  {event_type:20s}: {count:3d} events ({pct:5.1f}%)")

    # Display weather events table
    display(weather_df.orderBy("timestamp"))
else:
    print("⚠️  Weather station data not available - skipping weather event analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather Event Timeline

# COMMAND ----------

if weather_df is not None:
    # Visualize weather events over time
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from datetime import datetime

    weather_pd = weather_df.orderBy("timestamp").toPandas()

    # Convert microseconds timestamp to datetime
    weather_pd['datetime'] = weather_pd['timestamp'].apply(lambda x: datetime.fromtimestamp(x / 1_000_000))

    # Create figure with subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 12), sharex=True)

    # Plot 1: Wind speed over time
    ax1.plot(weather_pd['datetime'], weather_pd['wind_speed_knots'], 'b-', linewidth=2)
    ax1.set_ylabel('Wind Speed (knots)', fontsize=12)
    ax1.set_title('Weather Conditions During Race', fontsize=14, fontweight='bold')
    ax1.grid(alpha=0.3)
    ax1.fill_between(weather_pd['datetime'], weather_pd['wind_speed_knots'], alpha=0.3)

    # Plot 2: Wind direction over time
    ax2.plot(weather_pd['datetime'], weather_pd['wind_direction_degrees'], 'g-', linewidth=2)
    ax2.set_ylabel('Wind Direction (degrees)', fontsize=12)
    ax2.set_ylim(0, 360)
    ax2.grid(alpha=0.3)

    # Plot 3: Weather events
    event_colors = {
        'stable': 'green',
        'frontal_passage': 'red',
        'gradual_shift': 'orange',
        'gust': 'purple'
    }

    for event_type in weather_pd['event_type'].unique():
        event_data = weather_pd[weather_pd['event_type'] == event_type]
        ax3.scatter(event_data['datetime'], [event_type] * len(event_data),
                   color=event_colors.get(event_type, 'blue'), s=100, alpha=0.7,
                   label=event_type.replace('_', ' ').title())

    ax3.set_ylabel('Event Type', fontsize=12)
    ax3.set_xlabel('Race Time', fontsize=12)
    ax3.legend(loc='upper right')
    ax3.grid(alpha=0.3, axis='x')

    # Format x-axis
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    plt.xticks(rotation=45, ha='right')

    plt.tight_layout()
    display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boat Performance by Weather Event Type

# COMMAND ----------

if weather_df is not None:
    # Join telemetry with weather events
    df_with_weather = join_telemetry_with_weather(df, weather_df)

    # Analyze VMG by boat and weather event type
    performance_by_weather_event = df_with_weather.groupBy("boat_id", "boat_name", "weather_event_type").agg(
        F.avg("vmg_knots").alias("avg_vmg"),
        F.avg("speed_over_ground_knots").alias("avg_speed"),
        F.stddev("vmg_knots").alias("vmg_consistency"),
        F.count("*").alias("observations")
    ).filter(F.col("weather_event_type").isNotNull()).orderBy("boat_id", "weather_event_type")

    print("=" * 60)
    print("BOAT PERFORMANCE BY WEATHER EVENT TYPE")
    print("=" * 60)

    # Show sample data
    display(performance_by_weather_event)

    # Calculate fleet averages for comparison
    fleet_avg_by_event = df_with_weather.groupBy("weather_event_type").agg(
        F.avg("vmg_knots").alias("fleet_avg_vmg"),
        F.count("*").alias("total_observations")
    ).filter(F.col("weather_event_type").isNotNull())

    print("\nFleet Average VMG by Weather Event:")
    for row in fleet_avg_by_event.orderBy(F.desc("fleet_avg_vmg")).collect():
        print(f"  {row['weather_event_type']:20s}: {row['fleet_avg_vmg']:6.2f} kt (from {row['total_observations']:,} observations)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Performers During Each Weather Event

# COMMAND ----------

if weather_df is not None:
    from pyspark.sql.window import Window

    # Find top 5 boats for each weather event type
    top_by_event = performance_by_weather_event.withColumn(
        "rank",
        F.row_number().over(Window.partitionBy("weather_event_type").orderBy(F.desc("avg_vmg")))
    ).filter(F.col("rank") <= 5)

    print("=" * 60)
    print("TOP 5 BOATS BY WEATHER EVENT TYPE (Based on VMG)")
    print("=" * 60)

    for event_type in ["stable", "frontal_passage", "gradual_shift", "gust"]:
        rows = top_by_event.filter(F.col("weather_event_type") == event_type).orderBy("rank").collect()
        if rows:
            event_name = event_type.replace('_', ' ').title()
            print(f"\n{event_name}:")
            for row in rows:
                medal = "🥇" if row['rank'] == 1 else "🥈" if row['rank'] == 2 else "🥉" if row['rank'] == 3 else f"  {row['rank']}."
                print(f"  {medal} {row['boat_name']}")
                print(f"      VMG: {row['avg_vmg']:.2f} kt | Speed: {row['avg_speed']:.2f} kt | Observations: {row['observations']:,}")

    # Display full rankings
    display(top_by_event.orderBy("weather_event_type", "rank"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boat Adaptability Analysis

# COMMAND ----------

if weather_df is not None:
    # Calculate performance variance across different weather events
    # Lower variance = more consistent across conditions
    adaptability = performance_by_weather_event.groupBy("boat_id", "boat_name").agg(
        F.avg("avg_vmg").alias("overall_avg_vmg"),
        F.stddev("avg_vmg").alias("vmg_variance_across_events"),
        F.count("*").alias("events_participated")
    ).withColumn(
        "adaptability_score",
        # High VMG with low variance = high adaptability
        F.col("overall_avg_vmg") / (F.col("vmg_variance_across_events") + 0.01)  # Add small constant to avoid division by zero
    ).orderBy(F.desc("adaptability_score"))

    print("=" * 60)
    print("BOAT ADAPTABILITY TO CHANGING WEATHER")
    print("=" * 60)
    print("(Boats that maintain high VMG across all weather conditions)")
    print()

    for row in adaptability.collect():
        print(f"\n{row['boat_name']}")
        print(f"  Average VMG across all events: {row['overall_avg_vmg']:.2f} kt")
        print(f"  VMG variance across events: {row['vmg_variance_across_events']:.2f} kt")
        print(f"  Adaptability score: {row['adaptability_score']:.2f}")

    display(adaptability)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations - Weather Event Performance

# COMMAND ----------

if weather_df is not None:
    # Heatmap: VMG by weather event and boat
    pivot_weather = performance_by_weather_event.toPandas().pivot(
        index='boat_name',
        columns='weather_event_type',
        values='avg_vmg'
    )

    # Reorder columns
    event_order = ['stable', 'gradual_shift', 'frontal_passage', 'gust']
    pivot_weather = pivot_weather[[col for col in event_order if col in pivot_weather.columns]]

    # Rename columns for better display
    pivot_weather.columns = [col.replace('_', ' ').title() for col in pivot_weather.columns]

    fig, ax = plt.subplots(figsize=(12, 10))
    sns.heatmap(pivot_weather, annot=True, fmt='.2f', cmap='RdYlGn', center=pivot_weather.mean().mean(), ax=ax)
    ax.set_title('Average VMG by Weather Event Type and Boat', fontsize=14, fontweight='bold')
    ax.set_xlabel('Weather Event Type', fontsize=12)
    ax.set_ylabel('Boat', fontsize=12)

    plt.tight_layout()
    display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Gains/Losses During Weather Events

# COMMAND ----------

if weather_df is not None:
    # Calculate how much each boat's performance changed from stable conditions
    stable_vmg = performance_by_weather_event.filter(F.col("weather_event_type") == "stable") \
        .select("boat_id", "boat_name", F.col("avg_vmg").alias("stable_vmg"))

    # Join with all event types to calculate delta
    vmg_changes = performance_by_weather_event.join(stable_vmg, ["boat_id", "boat_name"], "left") \
        .filter(F.col("weather_event_type") != "stable") \
        .withColumn("vmg_delta", F.col("avg_vmg") - F.col("stable_vmg")) \
        .withColumn("vmg_delta_pct", (F.col("vmg_delta") / F.col("stable_vmg")) * 100) \
        .orderBy("boat_id", "weather_event_type")

    print("=" * 60)
    print("PERFORMANCE CHANGE FROM STABLE CONDITIONS")
    print("=" * 60)
    print("(Positive = better performance during event, Negative = worse)")
    print()

    for event_type in ["frontal_passage", "gradual_shift", "gust"]:
        event_data = vmg_changes.filter(F.col("weather_event_type") == event_type) \
            .orderBy(F.desc("vmg_delta"))

        print(f"\n{event_type.replace('_', ' ').title()}:")
        print("  Biggest Gainers:")
        for row in event_data.limit(3).collect():
            print(f"    • {row['boat_name']}: +{row['vmg_delta']:.2f} kt ({row['vmg_delta_pct']:+.1f}%)")

        print("  Biggest Losers:")
        for row in event_data.orderBy("vmg_delta").limit(3).collect():
            print(f"    • {row['boat_name']}: {row['vmg_delta']:.2f} kt ({row['vmg_delta_pct']:+.1f}%)")

    display(vmg_changes)

    # Save for use in other notebooks
    vmg_changes.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.performance_change_by_weather_event")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Weather Event Analysis Results

# COMMAND ----------

if weather_df is not None:
    # Save analysis results as tables
    weather_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.weather_events")
    performance_by_weather_event.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.performance_by_weather_event")
    top_by_event.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.top_performers_by_weather_event")
    adaptability.write.mode("overwrite").saveAsTable(f"{SCHEMA_PREFIX}.boat_adaptability_to_weather")

    print("Weather event analysis results saved to tables:")
    print(f"  - {SCHEMA_PREFIX}.weather_events")
    print(f"  - {SCHEMA_PREFIX}.performance_by_weather_event")
    print(f"  - {SCHEMA_PREFIX}.top_performers_by_weather_event")
    print("  - boat_adaptability_to_weather")
    print("  - performance_change_by_weather_event")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Findings
# MAGIC
# MAGIC **Summary**:
# MAGIC - Analyzed boat performance across different wind conditions (Light/Moderate/Heavy)
# MAGIC - Analyzed performance by point of sail (Close-hauled/Close Reach/Beam Reach/Broad Reach/Running)
# MAGIC - **NEW**: Analyzed performance during different weather events (stable, frontal passage, gradual shift, gust)
# MAGIC - Calculated total distance sailed at each point of sail for the fleet and each boat
# MAGIC - Identified which boats performed best in each condition, point of sail, and weather event
# MAGIC - Determined each boat's specialization (best point of sail)
# MAGIC - **NEW**: Evaluated boat adaptability to changing weather conditions
# MAGIC - Examined relationship between wind speed and boat speed
# MAGIC
# MAGIC **Insights**:
# MAGIC - Different boats excel in different wind conditions
# MAGIC - Each boat has a preferred point of sail where they perform best
# MAGIC - **NEW**: Some boats perform significantly better during specific weather events (frontal passages, gusts, etc.)
# MAGIC - **NEW**: Boat adaptability varies - some maintain consistent VMG across all conditions, others are specialists
# MAGIC - **NEW**: Frontal passages and gusts typically create performance differentiators between boats
# MAGIC - Distance distribution across points of sail reveals race course characteristics
# MAGIC - Close-hauled and beam reach segments typically cover more distance than running
# MAGIC - Boat speed increases as angle to wind increases (running fastest, close-hauled slowest)
# MAGIC - VMG varies significantly by wind strength, sailing angle, and weather event type
# MAGIC - Some boats are more versatile across conditions than others
# MAGIC - Combined wind condition + point of sail + weather event analysis reveals optimal scenarios for each boat
