import dlt
from pyspark.sql.functions import col, to_date, avg, lag, datediff
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# 1. Ingest raw_sensor_data from volume
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import to_timestamp

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("aircraft_id", StringType()),
    StructField("model", StringType()),
    StructField("engine_temp", DoubleType()),
    StructField("fuel_efficiency", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("altitude", DoubleType()),
    StructField("airspeed", DoubleType()),
    StructField("anomaly_score", DoubleType()),
    StructField("oil_pressure", DoubleType()),
    StructField("engine_rpm", IntegerType()),
    StructField("battery_voltage", DoubleType())
])

@dlt.table(
    comment="Ingested raw sensor data from aircraft"
)
def raw_sensor_data():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(schema)  # <-- Added schema here
        .load("/Volumes/arao/aerodemo/tmp/raw")
        .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
    )
# 2. Ingest maintenance_events from volume
from pyspark.sql.types import StructType, StructField, StringType

maintenance_schema = StructType([
    StructField("aircraft_id", StringType()),
    StructField("event_date", StringType()),
    StructField("event_type", StringType())
])
@dlt.table(
    comment="Maintenance events for aircraft"
)
def maintenance_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(maintenance_schema)  # 👈 Explicit schema here
        .load("/Volumes/arao/aerodemo/tmp/maintenance")
    )



# 3. Clean raw sensor data
@dlt.table(
    comment="Cleaned sensor data after applying expected quality checks"
)
@dlt.expect("valid_engine_temp", "engine_temp BETWEEN 0 AND 1000")
@dlt.expect("valid_fuel_eff", "fuel_efficiency > 0")
@dlt.expect("valid_vibration", "vibration >= 0")
def cleaned_sensor_data():
    return (
        dlt.read_stream("raw_sensor_data")
        .filter((col("engine_temp") <= 700) &
                (col("fuel_efficiency") >= 50) &
                (col("vibration") <= 25))
    )

# 3A. Aircraft model reference table
@dlt.table(
    comment="Reference metadata for aircraft models"
)
def aircraft_model_reference_dlt():
    return spark.read.table("arao.aerodemo.aircraft_model_reference")

# 4. Enrich with maintenance history and aircraft metadata
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, to_date
import dlt

@dlt.table(
    comment="Sensor data enriched with most recent maintenance event info and aircraft metadata"
)
def enriched_sensor_data():
    cleaned_df = dlt.read("cleaned_sensor_data").withColumn("reading_date", to_date("timestamp"))
    events_df = dlt.read("maintenance_events").withColumnRenamed("event_date", "maint_date")
    model_df = dlt.read("aircraft_model_reference_dlt")

    # Drop _rescued_data if present
    if "_rescued_data" in cleaned_df.columns:
        cleaned_df = cleaned_df.drop("_rescued_data")
    if "_rescued_data" in events_df.columns:
        events_df = events_df.drop("_rescued_data")

    # Join with maintenance events and filter to records before the reading
    joined = cleaned_df.join(events_df, on="aircraft_id", how="left") \
                       .filter(col("maint_date") <= col("reading_date"))

    # Keep the most recent maintenance event per (aircraft_id, timestamp)
    window = Window.partitionBy("aircraft_id", "timestamp").orderBy(col("maint_date").desc())
    enriched = joined.withColumn("rank", row_number().over(window)) \
                     .filter(col("rank") == 1) \
                     .drop("rank", "reading_date")

    # Join with aircraft metadata
    final_df = enriched.join(model_df, on="model", how="left")

    # Deduplicate by aircraft_id and timestamp to enforce PK constraints downstream
    pk_window = Window.partitionBy("aircraft_id", "timestamp").orderBy(col("timestamp").desc())
    deduped = final_df.withColumn("row_num", row_number().over(pk_window)) \
                      .filter(col("row_num") == 1) \
                      .drop("row_num")

    return deduped


# 5. Sensor Features Table
# This DLT table computes engineered features for ML model input, including rolling averages and lagged anomaly scores.
# It enforces uniqueness on (aircraft_id, timestamp) to support Feature Store compatibility and downstream predictions.

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, to_date, lag, avg, datediff
import dlt
from pyspark.sql.functions import to_timestamp


@dlt.table(
    comment="Engineered sensor features for ML model input",
    table_properties={
        "pipelines.materialize": "true",
        # "delta.constraints.primaryKey": "aircraft_id timestamp",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["aircraft_id"]
)
def sensor_features():

    df = dlt.read("enriched_sensor_data") \
            .withColumn("timestamp", to_timestamp("timestamp")) \
            .withColumn("date", to_date("timestamp"))

    df = (
        df.withColumn("anomaly_score", col("anomaly_score").cast("int"))
          .withColumn("engine_rpm", col("engine_rpm").cast("int"))
          .withColumn("battery_voltage", col("battery_voltage").cast("double"))
          .withColumn("engine_temp", col("engine_temp").cast("double"))
          .withColumn("fuel_efficiency", col("fuel_efficiency").cast("double"))
          .withColumn("vibration", col("vibration").cast("double"))
          .withColumn("altitude", col("altitude").cast("double"))
          .withColumn("airspeed", col("airspeed").cast("double"))
          .withColumn("oil_pressure", col("oil_pressure").cast("double"))
          .withColumn("capacity", col("capacity").cast("int"))
          .withColumn("range_km", col("range_km").cast("int"))
    )

    # Rolling windows
    rolling = Window.partitionBy("aircraft_id").orderBy("date").rowsBetween(-6, 0)
    lag_window = Window.partitionBy("aircraft_id").orderBy("date")

    df = (
        df.withColumn("prev_anomaly", lag("anomaly_score", 1).over(lag_window).cast("double"))
          .withColumn("avg_engine_temp_7d", avg("engine_temp").over(rolling).cast("double"))
          .withColumn("avg_vibration_7d", avg("vibration").over(rolling).cast("double"))
          .withColumn("avg_rpm_7d", avg("engine_rpm").over(rolling).cast("double"))
          .withColumn("days_since_maint", datediff("date", F.coalesce(col("maint_date"), F.lit("1970-01-01"))).cast("int"))
    )

    # ✅ Strong deduplication on PK
    pk_window = Window.partitionBy("aircraft_id", "timestamp").orderBy(col("timestamp").desc())
    df = df.withColumn("row_num", row_number().over(pk_window)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    return df.select(
        "timestamp", "aircraft_id", "model", 
        "engine_temp", "fuel_efficiency", "vibration", "altitude", "airspeed",
        "oil_pressure", "engine_rpm", "battery_voltage", "anomaly_score", "event_type",
        "avg_engine_temp_7d", "avg_vibration_7d", "avg_rpm_7d", "prev_anomaly", "days_since_maint",
        "manufacturer", "engine_type", "capacity", "range_km"
    )


# 6. Risk prediction logic (simulated model)
@dlt.table(
    comment="Predicted AOG risk scores from model"
)
def prediction_results():
    df = dlt.read("sensor_features")

    return df.withColumn(
        "predicted_anomaly",
        (
            ((col("avg_engine_temp_7d") / 700) + (col("avg_vibration_7d") / 25)) * 0.5 +
            (col("prev_anomaly") * 0.3) +
            ((col("days_since_maint") / 100.0) * 0.2)
        ).cast("double") > 0.5
    ).withColumn("predicted_anomaly", col("predicted_anomaly").cast("int"))



# ✅ Step 7: digital_twin_engine_view (Materialized Table)
@dlt.table(
    comment="Latest engine-level state with predictions for each aircraft. Materialized to ensure downstream steps auto-trigger."
)
def digital_twin_engine_view():
    from pyspark.sql.window import Window
    import pyspark.sql.functions as F

    # Read prediction results from previous step
    df = dlt.read("prediction_results")

    # Assign a health status label based on predicted anomaly
    df = df.withColumn(
        "engine_health_status",
        F.when(F.col("predicted_anomaly") == 1, "HIGH_RISK").otherwise("NORMAL")
    )

    # Use a window to get the latest reading per aircraft
    window = Window.partitionBy("aircraft_id").orderBy(F.col("timestamp").desc())

    return (
        df.withColumn("row_num", F.row_number().over(window))  # Assign row number
          .filter(F.col("row_num") == 1)                       # Keep only latest row
          .drop("row_num")                                     # Drop helper column
    )


# ✅ Step 8: digital_twin_aircraft_view (Materialized Table)
@dlt.table(
    comment="Aircraft-level snapshot view for dashboarding. Shows latest engine status and anomaly prediction."
)
def digital_twin_aircraft_view():
    # Read latest per-aircraft engine state
    df = dlt.read("digital_twin_engine_view")

    # Select key columns for summary dashboards
    return (
        df.select(
            "aircraft_id",
            "timestamp",
            "engine_health_status",
            "predicted_anomaly"
        )
    )


# 9. airport_location_reference DLT View
@dlt.view(
    comment="Reference table for airport latitude and longitude"
)
def airport_location_reference():
    return spark.createDataFrame([
        ("SFO", "San Francisco Intl", "San Francisco", 37.6213, -122.3790),
        ("LAX", "Los Angeles Intl", "Los Angeles", 33.9416, -118.4085),
        ("JFK", "John F. Kennedy Intl", "New York", 40.6413, -73.7781),
        ("ORD", "O'Hare Intl", "Chicago", 41.9742, -87.9073)
    ], ["airport_code", "airport_name", "city", "latitude", "longitude"])


# 10. aircraft_location_reference DLT View
@dlt.view(
    comment="Reference table mapping aircraft to their base airport"
)
def aircraft_location_reference():
    return spark.createDataFrame([
        ("A320_101", "SFO"),
        ("A320_102", "SFO"),
        ("A320_103", "SFO"),
        ("A320_104", "SFO"),
        ("A330_301", "LAX"),
        ("A330_302", "LAX"),
        ("A330_303", "LAX"),
        ("B737_201", "JFK"),
        ("B737_202", "ORD"),
        ("B737_203", "ORD")
    ], ["aircraft_id", "base_airport_code"])

# 11. Aircraft Location Enriched Table
# This DLT table joins aircraft base airport data with actual airport coordinates for use in geospatial dashboards.
@dlt.table(
    comment="Aircraft with latitude and longitude from airport reference"
)
def aircraft_location_enriched():
    aircraft_df = dlt.read("aircraft_location_reference")
    airport_df = dlt.read("airport_location_reference")

    return (
        aircraft_df
        .join(airport_df, aircraft_df["base_airport_code"] == airport_df["airport_code"])
    )


# ✅ Step 12: Aircraft Status Map View with Alert Count
# Combines aircraft geolocation, current engine health, and alert count for geospatial dashboards


@dlt.table(
    comment="Aircraft geolocation with engine health status and number of anomaly alerts for geospatial visualization"
)
def aircraft_status_map_view():
    loc = dlt.read("aircraft_location_enriched")
    twin = dlt.read("digital_twin_engine_view")
    alerts = spark.read.table("arao.aerodemo.anomaly_alerts")  # this is a non-DLT table

    return (
        loc
        .join(twin, on="aircraft_id", how="left")
        .join(alerts, on="aircraft_id", how="left")
        .groupBy("aircraft_id", "latitude", "longitude", "engine_health_status")
        .agg(F.count(alerts["aircraft_id"]).alias("alert_count"))
    )

# ✅ Step 13: Component Twins Master Table
# This table maps each aircraft to its components with metadata for Digital Twin management.

@dlt.table(
    comment="Master registry of all component twins associated with aircraft"
)
def component_twins_master():
    return spark.createDataFrame([
        ("A320_101", "ENG_L1", "Engine", "Left Engine 1", "2022-05-10", "GE", "CFM56"),
        ("A320_101", "GEAR_M", "LandingGear", "Main Gear", "2021-11-15", "Honeywell", "LG-HYD-X"),
        ("A320_101", "AVN_SYS", "Avionics", "Avionics Suite", "2023-01-01", "Rockwell", "Fusion"),
        ("A320_101", "CABIN_PRESS", "CabinPressurization", "Cabin Press System", "2022-08-25", "Collins", "CPC-9000"),
        ("A320_101", "FRAME", "Airframe", "Main Fuselage", "2020-07-01", "Airbus", "AF-Shell")
    ], [
        "aircraft_id",
        "component_id",
        "component_type",
        "component_name",
        "install_date",
        "manufacturer",
        "model"
    ])


# Sanity Check 
# ✅ Clean Steps Included:
# 	1.	Null checks on key columns
# 	2.	Out-of-range checks
# 	3.	Duplicate detection on (aircraft_id, timestamp)
# 	4.	Anomaly score distribution
# 	5.	Optional record count logging

import dlt
from pyspark.sql.functions import col, count, when, isnan, current_timestamp, expr, approx_count_distinct

@dlt.table(
    comment="Post-DLT sanity check validation results",
    table_properties={
        "pipelines.materialize": "true"
    }
)
def post_dlt_sanity_check():
    df = dlt.read("sensor_features")

    # Null value checks
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(f"{c}_nulls")
        for c in ["timestamp", "aircraft_id", "engine_temp", "fuel_efficiency", "vibration"]
    ])

    # Range violation checks
    out_of_range = df.filter(
        (col("engine_temp") > 1000) |
        (col("engine_temp") < 0) |
        (col("vibration") < 0)
    ).agg(count("*").alias("out_of_range_violations"))

    # Duplicate key detection
    duplicate_check = df.groupBy("aircraft_id", "timestamp") \
        .count() \
        .filter("count > 1") \
        .agg(count("*").alias("duplicate_pk_rows"))

    # Anomaly score distribution
    anomaly_dist = df.groupBy("anomaly_score").count().alias("anomaly_distribution")

    # Record count
    record_count = df.agg(count("*").alias("total_sensor_feature_rows"))

    # Join all checks into one row
    result = (
        null_counts.crossJoin(out_of_range)
                   .crossJoin(duplicate_check)
                   .crossJoin(record_count)
                   .withColumn("check_time", current_timestamp())
    )

    return result