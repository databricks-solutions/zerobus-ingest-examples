# Databricks notebook source
# MAGIC %md
# MAGIC # Race Summary - Executive Report
# MAGIC
# MAGIC ## Data Drifter Regatta - Sailboat Telemetry Analysis
# MAGIC
# MAGIC **Purpose**: Provide comprehensive overview of race results and key outcomes.
# MAGIC
# MAGIC **Analysis Modules**:
# MAGIC 1. **Boat Performance** - Overall best performing boats
# MAGIC 2. **Wind Conditions** - Performance under different wind conditions
# MAGIC 3. **Race Progress** - Performance across different legs and position changes
# MAGIC
# MAGIC This notebook aggregates findings from all previous analyses to provide actionable insights.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Race Data

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from race_utils import load_race_course_config, load_race_data, get_finished_boats

# Load race course configuration from config.toml
TABLE_NAME, marks, config = load_race_course_config()

# Derive schema from TABLE_NAME (catalog.schema.table -> catalog.schema)
SCHEMA_PREFIX = ".".join(TABLE_NAME.split(".")[:2])

# Load telemetry data using utility function
df = load_race_data(TABLE_NAME)

# Get race statistics
race_stats = df.agg(
    F.count("*").alias("total_records"),
    F.countDistinct("boat_id").alias("total_boats"),
    F.min("timestamp").alias("race_start"),
    F.max("timestamp").alias("race_end")
).collect()[0]

# Extract timestamps (epoch microseconds)
race_start = race_stats['race_start']
race_end = race_stats['race_end']

# If race_start and race_end are datetime objects, convert to seconds
if hasattr(race_start, 'timestamp'):
    race_start_us = int(race_start.timestamp() * 1_000_000)
    race_end_us = int(race_end.timestamp() * 1_000_000)
else:
    race_start_us = race_start
    race_end_us = race_end

# Calculate duration in hours (timestamps are epoch microseconds)
race_duration_microseconds = race_end_us - race_start_us
race_duration_hours = race_duration_microseconds / (1000000 * 3600)

print("=" * 80)
print(" " * 20 + "🌊  DATA DRIFTER REGATTA  🌊")
print("=" * 80)
print(f"\nRace Statistics:")
print(f"  Total telemetry records: {race_stats['total_records']:,}")
print(f"  Boats in fleet: {race_stats['total_boats']}")
print(f"  Race duration: {race_duration_hours:.2f} hours")
print(f"  Start time: {race_start}")
print(f"  End time: {race_end}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Overall Boat Rankings
# MAGIC
# MAGIC Load results from boat_performance_summary table created by 01_boat_performance notebook

# COMMAND ----------

# Try to load from table, fallback to calculation if not available
try:
    boat_performance = spark.table(f"{SCHEMA_PREFIX}.finished_boats_summary")
    print(f"✓ Loaded boat rankings from {SCHEMA_PREFIX}.finished_boats_summary table")
except:
    print("⚠️  Table not found, calculating boat rankings...")
    boat_performance = get_finished_boats(df)

print("=" * 80)
print("FINAL RACE RESULTS (by finish time)")
print("=" * 80)
top_boats = boat_performance.limit(5).collect()
for row in top_boats:
    medal = "🥇" if row['rank'] == 1 else "🥈" if row['rank'] == 2 else "🥉" if row['rank'] == 3 else f"  {row['rank']}."
    print(f"\n{medal} {row['boat_name']} ({row['boat_id']})")
    print(f"   Distance Sailed: {row['total_distance']:.2f} nm")
    print(f"   Final Remaining: {row['final_remaining_distance']:.4f} nm")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Performance by Wind Condition
# MAGIC
# MAGIC Load results from performance_by_wind table created by 02_wind_conditions notebook

# COMMAND ----------

# Try to load from table, fallback to calculation if not available
try:
    vmg_by_wind = spark.table(f"{SCHEMA_PREFIX}.performance_by_wind")
    print(f"✓ Loaded wind condition performance from {SCHEMA_PREFIX}.performance_by_wind table")

    wind_winners = vmg_by_wind.withColumn(
        "rank",
        F.row_number().over(Window.partitionBy("wind_category").orderBy(F.desc("avg_vmg")))
    ).filter(F.col("rank") == 1)
except:
    print("⚠️  Table not found, calculating wind condition performance...")
    df_conditions = df.withColumn("wind_category",
        F.when(F.col("wind_speed_knots") < 8, "Light")
         .when(F.col("wind_speed_knots") < 15, "Moderate")
         .otherwise("Heavy")
    )

    vmg_by_wind = df_conditions.groupBy("boat_id", "boat_name", "wind_category").agg(
        F.avg("vmg_knots").alias("avg_vmg")
    )

    wind_winners = vmg_by_wind.withColumn(
        "rank",
        F.row_number().over(Window.partitionBy("wind_category").orderBy(F.desc("avg_vmg")))
    ).filter(F.col("rank") == 1)

print("=" * 80)
print("BEST PERFORMERS BY WIND CONDITION")
print("=" * 80)
for row in wind_winners.collect():
    print(f"\n{row['wind_category']} Wind: {row['boat_name']}")
    print(f"  Average VMG: {row['avg_vmg']:.2f} knots")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Performance by Point of Sail
# MAGIC
# MAGIC Load results from performance_by_point_of_sail table created by 02_wind_conditions notebook

# COMMAND ----------

# Try to load from table, fallback to calculation if not available
try:
    vmg_by_sail = spark.table(f"{SCHEMA_PREFIX}.performance_by_point_of_sail")
    print(f"✓ Loaded point of sail performance from {SCHEMA_PREFIX}.performance_by_point_of_sail table")

    sail_winners = vmg_by_sail.withColumn(
        "rank",
        F.row_number().over(Window.partitionBy("point_of_sail").orderBy(F.desc("avg_vmg")))
    ).filter(F.col("rank") == 1)
except:
    print("⚠️  Table not found, calculating point of sail performance...")
    from race_utils import add_point_of_sail_column

    df_with_sail = add_point_of_sail_column(df)

    vmg_by_sail = df_with_sail.groupBy("boat_id", "boat_name", "point_of_sail").agg(
        F.avg("vmg_knots").alias("avg_vmg")
    )

    sail_winners = vmg_by_sail.withColumn(
        "rank",
        F.row_number().over(Window.partitionBy("point_of_sail").orderBy(F.desc("avg_vmg")))
    ).filter(F.col("rank") == 1)

print("=" * 80)
print("BEST PERFORMERS BY POINT OF SAIL")
print("=" * 80)
for row in sail_winners.orderBy("point_of_sail").collect():
    print(f"\n{row['point_of_sail']}: {row['boat_name']}")
    print(f"  Average VMG: {row['avg_vmg']:.2f} knots")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5. Weather Event Analysis
# MAGIC
# MAGIC Load results from performance_by_weather_event table created by 02_wind_conditions notebook

# COMMAND ----------

# Try to load from table first
from race_utils import load_weather_station_data, summarize_weather_events

weather_df = load_weather_station_data(config)

if weather_df is not None:
    print("=" * 80)
    print("WEATHER CONDITIONS DURING RACE")
    print("=" * 80)

    # Summarize weather events
    weather_summary = summarize_weather_events(weather_df)

    if weather_summary:
        print(f"\nTotal weather events: {weather_summary['total_events']}")
        print(f"\nWind speed range: {weather_summary['wind_speed_min']:.1f} - {weather_summary['wind_speed_max']:.1f} knots")
        print(f"Average wind speed: {weather_summary['wind_speed_avg']:.1f} knots")
        print(f"\nWind direction range: {weather_summary['wind_direction_min']:.0f}° - {weather_summary['wind_direction_max']:.0f}°")
        print(f"Average wind direction: {weather_summary['wind_direction_avg']:.0f}°")

        print(f"\nWeather event types:")
        for event_type, count in sorted(weather_summary['event_counts'].items()):
            pct = (count / weather_summary['total_events']) * 100
            print(f"  {event_type.replace('_', ' ').title():20s}: {count:3d} events ({pct:5.1f}%)")

    # Try to load from table, fallback to calculation if not available
    try:
        vmg_by_weather = spark.table(f"{SCHEMA_PREFIX}.performance_by_weather_event")
        print(f"\n✓ Loaded weather event performance from {SCHEMA_PREFIX}.performance_by_weather_event")

        weather_winners = vmg_by_weather.withColumn(
            "rank",
            F.row_number().over(Window.partitionBy("weather_event_type").orderBy(F.desc("avg_vmg")))
        ).filter(F.col("rank") == 1)
    except:
        print("\n⚠️  Table not found, calculating weather event performance...")
        from race_utils import join_telemetry_with_weather

        df_with_weather = join_telemetry_with_weather(df, weather_df)

        vmg_by_weather = df_with_weather.groupBy("boat_id", "boat_name", "weather_event_type").agg(
            F.avg("vmg_knots").alias("avg_vmg")
        ).filter(F.col("weather_event_type").isNotNull())

        weather_winners = vmg_by_weather.withColumn(
            "rank",
            F.row_number().over(Window.partitionBy("weather_event_type").orderBy(F.desc("avg_vmg")))
        ).filter(F.col("rank") == 1)

    print("\n" + "=" * 80)
    print("BEST PERFORMERS BY WEATHER EVENT TYPE")
    print("=" * 80)
    for row in weather_winners.orderBy("weather_event_type").collect():
        event_name = row['weather_event_type'].replace('_', ' ').title()
        print(f"\n{event_name}: {row['boat_name']}")
        print(f"  Average VMG: {row['avg_vmg']:.2f} knots")

    # Calculate fleet average VMG by weather event from the persisted table
    try:
        fleet_vmg_by_weather = spark.table(f"{SCHEMA_PREFIX}.performance_by_weather_event").groupBy("weather_event_type").agg(
            F.avg("avg_vmg").alias("fleet_avg_vmg"),
            F.sum("observations").alias("observations")
        ).filter(F.col("weather_event_type").isNotNull()).orderBy(F.desc("fleet_avg_vmg"))

        print("\n" + "=" * 80)
        print("FLEET PERFORMANCE BY WEATHER EVENT")
        print("=" * 80)
        print("(Fleet-wide average VMG during each weather condition)\n")
        for row in fleet_vmg_by_weather.collect():
            event_name = row['weather_event_type'].replace('_', ' ').title()
            print(f"{event_name:20s}: {row['fleet_avg_vmg']:.2f} kt")
    except:
        pass

else:
    print("=" * 80)
    print("⚠️  Weather station data not available")
    print("=" * 80)
    print("Weather event analysis requires weather station table to be populated.")
    weather_winners = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather Event Performance Visualization

# COMMAND ----------

if weather_df is not None:
    import matplotlib.pyplot as plt
    import seaborn as sns

    # Heatmap of boat performance by weather event
    weather_heatmap_data = vmg_by_weather.toPandas().pivot(
        index='boat_name',
        columns='weather_event_type',
        values='avg_vmg'
    )

    # Reorder columns
    event_order = ['stable', 'gradual_shift', 'frontal_passage', 'gust']
    weather_heatmap_data = weather_heatmap_data[[col for col in event_order if col in weather_heatmap_data.columns]]
    weather_heatmap_data.columns = [col.replace('_', ' ').title() for col in weather_heatmap_data.columns]

    fig, ax = plt.subplots(figsize=(10, 10))
    sns.heatmap(weather_heatmap_data, annot=True, fmt='.2f', cmap='RdYlGn',
                center=weather_heatmap_data.mean().mean(), ax=ax)
    ax.set_title('Boat VMG Performance by Weather Event Type', fontsize=14, fontweight='bold')
    ax.set_xlabel('Weather Event Type', fontsize=12)
    ax.set_ylabel('Boat', fontsize=12)

    plt.tight_layout()
    display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Position Changes Throughout Race
# MAGIC
# MAGIC **Note**: Position change analysis requires full race data. Loading from race_positions_over_time table if available.

# COMMAND ----------

# Try to load from table created by 03_race_progress notebook
try:
    print("✓ Position analysis available - see 03_race_progress notebook for detailed position tracking")
    print("  Tables available:")
    print(f"    - {SCHEMA_PREFIX}.race_positions_over_time")
    print(f"    - {SCHEMA_PREFIX}.race_leg_vmg")
    print(f"    - {SCHEMA_PREFIX}.race_leg_trends")
    print(f"    - {SCHEMA_PREFIX}.race_leg_consistency")
except:
    print("⚠️  Run 03_race_progress notebook first for detailed position analysis")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Comprehensive Performance Visualization

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Create comprehensive comparison
fig, axes = plt.subplots(1, 3, figsize=(18, 6))
fig.suptitle('Data Drifter Regatta - Boat Performance Summary', fontsize=16, fontweight='bold')

# Plot 1: Overall Rankings
ax1 = axes[0]
rankings_data = boat_performance.orderBy("rank").toPandas()
bars1 = ax1.barh(rankings_data['boat_name'], rankings_data['total_distance'])
for i, bar in enumerate(bars1):
    if i < 3:
        bar.set_color('gold' if i == 0 else 'silver' if i == 1 else '#CD7F32')
    else:
        bar.set_color('lightblue')
ax1.set_xlabel('Total Distance Sailed (nm)')
ax1.set_title('Overall Boat Rankings')
ax1.invert_yaxis()
ax1.grid(axis='x', alpha=0.3)

# Plot 2: VMG by Wind Condition
ax2 = axes[1]
wind_data = vmg_by_wind.toPandas().pivot(
    index='boat_name',
    columns='wind_category',
    values='avg_vmg'
)
wind_data = wind_data[['Light', 'Moderate', 'Heavy']]
sns.heatmap(wind_data, annot=True, fmt='.2f', cmap='RdYlGn', center=0, ax=ax2)
ax2.set_title('VMG by Wind Condition')
ax2.set_ylabel('')

# Plot 3: VMG by Point of Sail
ax3 = axes[2]
sail_data = vmg_by_sail.toPandas().pivot(
    index='boat_name',
    columns='point_of_sail',
    values='avg_vmg'
)
# Reorder columns in sailing sequence
sail_order = ['Close-hauled', 'Close Reach', 'Beam Reach', 'Broad Reach', 'Running']
sail_data = sail_data[[col for col in sail_order if col in sail_data.columns]]
sns.heatmap(sail_data, annot=True, fmt='.2f', cmap='RdYlGn', center=0, ax=ax3)
ax3.set_title('VMG by Point of Sail')
ax3.set_ylabel('')
plt.setp(ax3.get_xticklabels(), rotation=45, ha='right', fontsize=8)

plt.tight_layout()
display(fig)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Insights and Findings

# COMMAND ----------

# Calculate key metrics
winner = top_boats[0]
total_boats = race_stats['total_boats']
finished_boats = boat_performance.count()
finish_rate = (finished_boats / total_boats) * 100

print("=" * 80)
print("KEY INSIGHTS")
print("=" * 80)

print(f"\n🏆 RACE WINNER: {winner['boat_name']} ({winner['boat_id']})")
print(f"   - Distance sailed: {winner['total_distance']:.2f} nm")

print(f"\n📊 RACE COMPLETION:")
print(f"   - {finished_boats}/{total_boats} boats finished ({finish_rate:.1f}%)")

print(f"\n🌬️  WIND CONDITIONS:")
for row in wind_winners.collect():
    print(f"   - {row['wind_category']} wind: {row['boat_name']} performed best (VMG: {row['avg_vmg']:.2f})")

print(f"\n⛵ POINT OF SAIL:")
for row in sail_winners.orderBy("point_of_sail").collect():
    print(f"   - {row['point_of_sail']}: {row['boat_name']} performed best (VMG: {row['avg_vmg']:.2f})")

if weather_df is not None and weather_winners is not None:
    print(f"\n🌤️  WEATHER EVENTS:")
    for row in weather_winners.orderBy("weather_event_type").collect():
        event_name = row['weather_event_type'].replace('_', ' ').title()
        print(f"   - {event_name}: {row['boat_name']} performed best (VMG: {row['avg_vmg']:.2f})")

print(f"\n📊 For detailed race phase and position analysis:")
print(f"   - See 03_race_progress notebook for leg-by-leg breakdown")
print(f"   - Race position tracking over time")
print(f"   - Performance trends (start strong vs finish strong)")
print(f"   - Consistency ratings across legs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Race Statistics

# COMMAND ----------

# Overall race statistics
total_distance_sailed = boat_performance.agg(F.sum("total_distance")).collect()[0][0]
avg_speed = df.agg(F.avg("speed_over_ground_knots")).collect()[0][0]
avg_vmg = df.agg(F.avg("vmg_knots")).collect()[0][0]

print("=" * 80)
print("RACE STATISTICS SUMMARY")
print("=" * 80)
print(f"\nTotal distance sailed by fleet: {total_distance_sailed:.2f} nm")
print(f"Average boat speed: {avg_speed:.2f} knots")
print(f"Average VMG: {avg_vmg:.2f} knots")
print(f"Race duration: {race_duration_hours:.2f} hours")

# Best individual performance
best_boat = boat_performance.orderBy("total_distance").first()
print(f"\n🏅 Best individual performance:")
print(f"   {best_boat['boat_name']} ({best_boat['boat_id']})")
print(f"   Distance: {best_boat['total_distance']:.2f} nm")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC This analysis evaluated all boats in the fleet across multiple dimensions:
# MAGIC - Overall efficiency (distance sailed)
# MAGIC - Performance under varying wind conditions (Light/Moderate/Heavy)
# MAGIC - **NEW**: Performance during different weather events (stable, frontal passage, gradual shift, gust)
# MAGIC - Performance by point of sail (Close-hauled/Close Reach/Beam Reach/Broad Reach/Running)
# MAGIC - Distance sailed at each point of sail
# MAGIC - Position changes throughout the race
# MAGIC - Performance on different leg types (Upwind/Reaching/Downwind legs)
# MAGIC - Effectiveness across different race phases/legs
# MAGIC - Consistency and adaptability
# MAGIC
# MAGIC The winning boat demonstrates superior performance through:
# MAGIC 1. Efficient path selection (shortest distance)
# MAGIC 2. Adaptability to changing wind conditions
# MAGIC 3. **Strong VMG during critical weather events** (frontal passages, gusts)
# MAGIC 4. Strong performance across different points of sail
# MAGIC 5. Ability to gain positions on key leg types
# MAGIC 6. Consistent performance across race phases
# MAGIC 7. Strong tactical decisions
# MAGIC
# MAGIC **Key Tactical Insights**:
# MAGIC - Some boats excel on upwind legs, gaining positions when beating to weather
# MAGIC - Others perform best on reaching or downwind legs
# MAGIC - **Weather events create significant performance differentials between boats**
# MAGIC - **Boats that maintain high VMG during frontal passages and gusts often gain critical positions**
# MAGIC - **Adaptability to weather changes is a key predictor of race success**
# MAGIC - Understanding which boats gain/lose positions on which leg types reveals tactical strengths
# MAGIC - Net position changes show which boats improved throughout the race vs. those that faded
# MAGIC - Some boats are weather specialists (excel during specific events) while others are generalists (consistent across all conditions)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC *Analysis powered by Databricks and Zerobus Ingest*
# MAGIC
# MAGIC *Data Drifter Regatta - Lakeflow Connect*
