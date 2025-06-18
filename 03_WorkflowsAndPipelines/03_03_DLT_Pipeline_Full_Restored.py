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

# # 12 Aircraft Location with Engine Health Status
# @dlt.view(
#     comment="Joins the latest aircraft engine health predictions with airport geolocation data to visualize aircraft status on a map"
# )
# def aircraft_status_map_view():
#     # Latest aircraft-level engine health status
#     twin_df = dlt.read("digital_twin_engine_view")

#     # Airport base location for each aircraft
#     aircraft_location_df = dlt.read("aircraft_location_reference")

#     # Airport latitude/longitude and metadata
#     airport_location_df = dlt.read("airport_location_reference")

#     return (
#         twin_df
#         .join(aircraft_location_df, on="aircraft_id", how="left")
#         .join(airport_location_df, aircraft_location_df["base_airport_code"] == airport_location_df["airport_code"], "left")
#         .select(
#             "aircraft_id", "timestamp", "engine_health_status", "predicted_anomaly",
#             "base_airport_code", "city", "airport_name", "latitude", "longitude"
#         )
#     )


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
        ("A320_101", "FRAME", "Airframe", "Main Fuselage", "2020-07-01", "Airbus", "AF-Shell"),
        ("A320_101", "EL_SYS", "ElectricalSystems", "Electrical Power System", "2022-03-15", "Honeywell", "EP-2000"),
        ("A320_101", "FUEL_SYS", "FuelSystems", "Fuel Management System", "2021-09-20", "Parker", "FMS-3000"),
        ("A320_101", "HYD_SYS", "HydraulicSystems", "Hydraulic Control System", "2022-01-10", "Eaton", "HCS-500"),
        ("A320_101", "ENV_SYS", "EnvironmentalSystems", "Environmental Control", "2022-06-05", "Collins", "ECS-400")
    ], [
        "aircraft_id",
        "component_id",
        "component_type",
        "component_name",
        "install_date",
        "manufacturer",
        "model"
    ])


# ✅ Step 14: Twin - Engine Component
import dlt
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

@dlt.table(
    comment="Digital twin sensor data for engine components"
)
def twin_engine():
    schema = StructType([
        StructField("engine_id", StringType()),
        StructField("aircraft_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("thrust_level", DoubleType()),
        StructField("fuel_consumption_rate", DoubleType()),
        StructField("temperature_reading", DoubleType()),
        StructField("vibration_level", DoubleType()),
        StructField("oil_pressure", DoubleType())
    ])
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(schema)
            .load("/Volumes/arao/aerodemo/tmp/engine")
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))
    )

# ✅ Step 14A: Component Health Status for Engine
import dlt
from pyspark.sql.functions import col, when

@dlt.table(
    comment="Health status classification for engine components based on sensor metrics"
)
def component_health_engine():
    df = dlt.read("twin_engine")

    return (
        df.withColumn(
            "health_status",
            when(
                (col("temperature_reading") > 750) |
                (col("vibration_level") > 1.8) |
                (col("oil_pressure") < 40),
                "CRITICAL"
            ).when(
                (col("temperature_reading") > 700) |
                (col("vibration_level") > 1.2) |
                (col("oil_pressure") < 50),
                "WARNING"
            ).otherwise("NOMINAL")
        )
        .select(
            col("aircraft_id"),
            col("engine_id").alias("component_id"),
            col("event_timestamp"),
            col("thrust_level"),
            col("temperature_reading"),
            col("vibration_level"),
            col("oil_pressure"),
            col("fuel_consumption_rate"),
            col("health_status")
        )
    )

# ✅ Step 15: Twin - Landing Gear Component
@dlt.table(
    comment="Digital twin sensor data for landing gear components"
)
def twin_landing_gear():
    schema = StructType([
        StructField("gear_id", StringType()),
        StructField("aircraft_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("hydraulic_pressure", DoubleType()),
        StructField("strut_compression", DoubleType()),
        StructField("brake_wear", DoubleType()),
        StructField("brake_temperature", DoubleType()),
        StructField("shock_absorber_status", DoubleType())
    ])
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(schema)
            .load("/Volumes/arao/aerodemo/tmp/landing_gear")
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))
    )

# ✅ Step 15A: Component Health Status for Landing Gear
import dlt
from pyspark.sql.functions import col, when

@dlt.table(
    comment="Health status classification for landing gear components based on sensor metrics"
)
def component_health_landing_gear():
    df = dlt.read("twin_landing_gear")

    return (
        df.withColumn(
            "health_status",
            when(
                (col("brake_temperature") > 450) |
                (col("brake_wear") > 80) |
                (col("shock_absorber_status") < 60),
                "CRITICAL"
            ).when(
                (col("brake_temperature") > 350) |
                (col("brake_wear") > 60) |
                (col("shock_absorber_status") < 70),
                "WARNING"
            ).otherwise("NOMINAL")
        )
        .select(
            col("aircraft_id"),
            col("gear_id").alias("component_id"),
            col("event_timestamp"),
            col("hydraulic_pressure"),
            col("strut_compression"),
            col("brake_wear"),
            col("brake_temperature"),
            col("shock_absorber_status"),
            col("health_status")
        )
    )

# ✅ Step 16: Twin - Avionics Systems Component
@dlt.table(
    comment="Digital twin telemetry for avionics systems"
)
def twin_avionics():
    schema = StructType([
        StructField("avionics_id", StringType()),
        StructField("aircraft_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("power_status", DoubleType()),
        StructField("signal_integrity", DoubleType()),
        StructField("data_transmission_rate", DoubleType()),
        StructField("system_temperature", DoubleType()),
        StructField("error_logs", IntegerType())
    ])
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(schema)
            .load("/Volumes/arao/aerodemo/tmp/avionics")
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))
    )

# ✅ Step 16A: Component Health Status for Avionics Systems
import dlt
from pyspark.sql.functions import col, when

@dlt.table(
    comment="Health status classification for avionics systems based on telemetry metrics"
)
def component_health_avionics():
    df = dlt.read("twin_avionics")

    return (
        df.withColumn(
            "health_status",
            when(
                (col("signal_integrity") < 20) |
                (col("error_logs") > 5) |
                (col("system_temperature") > 50),
                "CRITICAL"
            ).when(
                (col("signal_integrity") < 30) |
                (col("error_logs") > 3) |
                (col("system_temperature") > 40),
                "WARNING"
            ).otherwise("NOMINAL")
        )
        .select(
            col("aircraft_id"),
            col("avionics_id").alias("component_id"),
            col("event_timestamp"),
            col("power_status"),
            col("signal_integrity"),
            col("data_transmission_rate"),
            col("system_temperature"),
            col("error_logs"),
            col("health_status")
        )
    )


# ✅ Step 17: Twin - Cabin Pressurization System
@dlt.table(
    comment="Digital twin data for cabin pressurization components"
)
def twin_cabin_pressurization():
    schema = StructType([
        StructField("cabin_id", StringType()),
        StructField("aircraft_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("cabin_pressure", DoubleType()),
        StructField("seal_integrity", DoubleType()),
        StructField("airflow_rate", DoubleType()),
        StructField("temperature_control", DoubleType()),
        StructField("humidity_level", DoubleType())
    ])
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(schema)
            .load("/Volumes/arao/aerodemo/tmp/cabin")
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))
    )

# ✅ Step 17A: Component Health Status for Cabin Pressurization
import dlt
from pyspark.sql.functions import col, when

@dlt.table(
    comment="Health status classification for cabin pressurization systems based on environmental metrics"
)
def component_health_cabin_pressurization():
    df = dlt.read("twin_cabin_pressurization")

    return (
        df.withColumn(
            "health_status",
            when(
                (col("cabin_pressure") < 10) |
                (col("seal_integrity") < 85) |
                (col("airflow_rate") < 300),
                "CRITICAL"
            ).when(
                (col("cabin_pressure") < 11) |
                (col("seal_integrity") < 90) |
                (col("airflow_rate") < 350),
                "WARNING"
            ).otherwise("NOMINAL")
        )
        .select(
            col("aircraft_id"),
            col("cabin_id").alias("component_id"),
            col("event_timestamp"),
            col("cabin_pressure"),
            col("seal_integrity"),
            col("airflow_rate"),
            col("temperature_control"),
            col("humidity_level"),
            col("health_status")
        )
    )



# ✅ Step 18: Twin - Airframe Component
@dlt.table(
    comment="Digital twin structural data for aircraft airframes"
)
def twin_airframe():
    schema = StructType([
        StructField("airframe_id", StringType()),
        StructField("aircraft_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("stress_points", DoubleType()),
        StructField("fatigue_crack_growth", DoubleType()),
        StructField("temperature_fluctuations", DoubleType()),
        StructField("structural_integrity", DoubleType())
    ])
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(schema)
            .load("/Volumes/arao/aerodemo/tmp/airframe")
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))
    )

# ✅ Step 18A: Component Health Status for Airframe
import dlt
from pyspark.sql.functions import col, when

@dlt.table(
    comment="Health status classification for airframe based on structural integrity and stress metrics"
)
def component_health_airframe():
    df = dlt.read("twin_airframe")

    return (
        df.withColumn(
            "health_status",
            when(
                (col("stress_points") > 280) |
                (col("fatigue_crack_growth") > 8) |
                (col("structural_integrity") < 60),
                "CRITICAL"
            ).when(
                (col("stress_points") > 200) |
                (col("fatigue_crack_growth") > 5) |
                (col("structural_integrity") < 75),
                "WARNING"
            ).otherwise("NOMINAL")
        )
        .select(
            col("aircraft_id"),
            col("airframe_id").alias("component_id"),
            col("event_timestamp"),
            col("stress_points"),
            col("fatigue_crack_growth"),
            col("temperature_fluctuations"),
            col("structural_integrity"),
            col("health_status")
        )
    )

# ✅ Step 19: Twin - Electrical Systems Component
@dlt.table(
    comment="Digital twin sensor data for electrical systems components"
)
def twin_electrical_systems():
    schema = StructType([
        StructField("electrical_id", StringType()),
        StructField("aircraft_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("battery_voltage", DoubleType()),
        StructField("charge_level", DoubleType()),
        StructField("power_distribution_efficiency", DoubleType()),
        StructField("electrical_load", DoubleType()),
        StructField("transformer_health", DoubleType()),
        StructField("fault_code", StringType())
    ])
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(schema)
            .load("/Volumes/arao/aerodemo/tmp/electrical")
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))
    )

# ✅ Step 19A: Component Health Status for Electrical Systems
@dlt.table(
    comment="Health status classification for electrical systems based on power and battery metrics"
)
def component_health_electrical_systems():
    df = dlt.read("twin_electrical_systems")

    return (
        df.withColumn(
            "health_status",
            when(
                (col("battery_voltage") < 24) |
                (col("charge_level") < 30) |
                (col("power_distribution_efficiency") < 85) |
                (col("fault_code") == "FAIL"),
                "CRITICAL"
            ).when(
                (col("battery_voltage") < 25) |
                (col("charge_level") < 50) |
                (col("power_distribution_efficiency") < 90) |
                (col("fault_code") == "WARN"),
                "WARNING"
            ).otherwise("NOMINAL")
        )
        .select(
            col("aircraft_id"),
            col("electrical_id").alias("component_id"),
            col("event_timestamp"),
            col("battery_voltage"),
            col("charge_level"),
            col("power_distribution_efficiency"),
            col("electrical_load"),
            col("transformer_health"),
            col("fault_code"),
            col("health_status")
        )
    )

# ✅ Step 20: Twin - Fuel Systems Component
@dlt.table(
    comment="Digital twin sensor data for fuel systems components"
)
def twin_fuel_systems():
    schema = StructType([
        StructField("fuel_system_id", StringType()),
        StructField("aircraft_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("fuel_level", DoubleType()),
        StructField("fuel_flow_rate", DoubleType()),
        StructField("fuel_pressure", DoubleType()),
        StructField("fuel_temperature", DoubleType()),
        StructField("tank_balance", DoubleType()),
        StructField("pump_health", DoubleType()),
        StructField("anomaly_flag", StringType())
    ])
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(schema)
            .load("/Volumes/arao/aerodemo/tmp/fuel")
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))
    )

# ✅ Step 20A: Component Health Status for Fuel Systems
@dlt.table(
    comment="Health status classification for fuel systems based on fuel delivery and balance metrics"
)
def component_health_fuel_systems():
    df = dlt.read("twin_fuel_systems")

    return (
        df.withColumn(
            "health_status",
            when(
                (col("fuel_level") < 15) |
                (col("fuel_pressure") < 25) |
                (col("pump_health") < 0.85) |
                (col("anomaly_flag") == "FAIL"),
                "CRITICAL"
            ).when(
                (col("fuel_level") < 25) |
                (col("fuel_pressure") < 30) |
                (col("pump_health") < 0.90) |
                (col("anomaly_flag") == "WARN"),
                "WARNING"
            ).otherwise("NOMINAL")
        )
        .select(
            col("aircraft_id"),
            col("fuel_system_id").alias("component_id"),
            col("event_timestamp"),
            col("fuel_level"),
            col("fuel_flow_rate"),
            col("fuel_pressure"),
            col("fuel_temperature"),
            col("tank_balance"),
            col("pump_health"),
            col("anomaly_flag"),
            col("health_status")
        )
    )

# ✅ Step 21: Twin - Hydraulic Systems Component
@dlt.table(
    comment="Digital twin sensor data for hydraulic systems components"
)
def twin_hydraulic_systems():
    schema = StructType([
        StructField("hydraulic_id", StringType()),
        StructField("aircraft_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("hydraulic_pressure", DoubleType()),
        StructField("fluid_temperature", DoubleType()),
        StructField("fluid_level", DoubleType()),
        StructField("pump_performance", DoubleType()),
        StructField("line_integrity", DoubleType()),
        StructField("anomaly_flag", StringType())
    ])
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(schema)
            .load("/Volumes/arao/aerodemo/tmp/hydraulic")
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))
    )

# ✅ Step 21A: Component Health Status for Hydraulic Systems
@dlt.table(
    comment="Health status classification for hydraulic systems based on pressure and fluid metrics"
)
def component_health_hydraulic_systems():
    df = dlt.read("twin_hydraulic_systems")

    return (
        df.withColumn(
            "health_status",
            when(
                (col("hydraulic_pressure") < 2400) |
                (col("fluid_level") < 60) |
                (col("pump_performance") < 0.85) |
                (col("line_integrity") < 0.95) |
                (col("anomaly_flag").isin("LEAK", "PRESSURE_DROP")),
                "CRITICAL"
            ).when(
                (col("hydraulic_pressure") < 2600) |
                (col("fluid_level") < 75) |
                (col("pump_performance") < 0.90) |
                (col("line_integrity") < 0.98) |
                (col("anomaly_flag") == "WARN"),
                "WARNING"
            ).otherwise("NOMINAL")
        )
        .select(
            col("aircraft_id"),
            col("hydraulic_id").alias("component_id"),
            col("event_timestamp"),
            col("hydraulic_pressure"),
            col("fluid_temperature"),
            col("fluid_level"),
            col("pump_performance"),
            col("line_integrity"),
            col("anomaly_flag"),
            col("health_status")
        )
    )

# ✅ Step 22: Twin - Environmental Systems Component
@dlt.table(
    comment="Digital twin data for environmental and weather systems"
)
def twin_environmental_systems():
    schema = StructType([
        StructField("environmental_id", StringType()),
        StructField("aircraft_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("external_temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("wind_speed", DoubleType()),
        StructField("crosswind_impact", DoubleType()),
        StructField("precipitation", StringType()),
        StructField("atmospheric_pressure", DoubleType()),
        StructField("extreme_event", StringType())
    ])
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(schema)
            .load("/Volumes/arao/aerodemo/tmp/environmental")
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))
    )

# ✅ Step 22A: Component Health Status for Environmental Systems
@dlt.table(
    comment="Health status classification for environmental systems based on weather and atmospheric conditions"
)
def component_health_environmental_systems():
    df = dlt.read("twin_environmental_systems")

    return (
        df.withColumn(
            "health_status",
            when(
                (col("wind_speed") > 120) |
                (col("crosswind_impact") > 25) |
                (col("atmospheric_pressure") < 950) |
                (col("extreme_event").isin("SEVERE_WIND", "THUNDERSTORM")),
                "CRITICAL"
            ).when(
                (col("wind_speed") > 80) |
                (col("crosswind_impact") > 20) |
                (col("atmospheric_pressure") < 980) |
                (col("extreme_event") == "TURBULENCE"),
                "WARNING"
            ).otherwise("NOMINAL")
        )
        .select(
            col("aircraft_id"),
            col("environmental_id").alias("component_id"),
            col("event_timestamp"),
            col("external_temperature"),
            col("humidity"),
            col("wind_speed"),
            col("crosswind_impact"),
            col("precipitation"),
            col("atmospheric_pressure"),
            col("extreme_event"),
            col("health_status")
        )
    )

# ✅ Step 23: Unified Component Health View (Updated with all 10 components)
import dlt
from pyspark.sql.functions import lit

@dlt.table(
    comment="Combined digital twin component-level health status across all 10 systems"
)
def digital_twin_component_view():
    engine = dlt.read("component_health_engine").withColumn("component_type", lit("Engine"))
    gear = dlt.read("component_health_landing_gear").withColumn("component_type", lit("LandingGear"))
    avionics = dlt.read("component_health_avionics").withColumn("component_type", lit("Avionics"))
    cabin = dlt.read("component_health_cabin_pressurization").withColumn("component_type", lit("CabinPressurization"))    
    airframe = dlt.read("component_health_airframe").withColumn("component_type", lit("Airframe"))
    electrical = dlt.read("component_health_electrical_systems").withColumn("component_type", lit("ElectricalSystems"))
    fuel = dlt.read("component_health_fuel_systems").withColumn("component_type", lit("FuelSystems"))
    hydraulic = dlt.read("component_health_hydraulic_systems").withColumn("component_type", lit("HydraulicSystems"))
    environmental = dlt.read("component_health_environmental_systems").withColumn("component_type", lit("EnvironmentalSystems"))

    return engine.unionByName(gear, allowMissingColumns=True) \
                 .unionByName(avionics, allowMissingColumns=True) \
                 .unionByName(cabin, allowMissingColumns=True) \
                 .unionByName(airframe, allowMissingColumns=True) \
                 .unionByName(electrical, allowMissingColumns=True) \
                 .unionByName(fuel, allowMissingColumns=True) \
                 .unionByName(hydraulic, allowMissingColumns=True) \
                 .unionByName(environmental, allowMissingColumns=True)

from pyspark.sql.functions import current_timestamp

# ✅ Step 19A: Component-Level Anomaly Alerts Table
# This table identifies components currently in CRITICAL or WARNING health states.
# Since the unified digital_twin_component_view does not carry forward the original event_timestamp 
# from each component source, we assign the alert timestamp as the current system timestamp
# to record when the alert was generated in this pipeline.
@dlt.table(
    comment="Alert table for components in CRITICAL or WARNING health states"
)
def anomaly_alerts_component():
    df = dlt.read("digital_twin_component_view")

    return (
        df.filter(col("health_status").isin("CRITICAL", "WARNING"))
          .withColumn("alert_timestamp", col("event_timestamp"))
          .select(
              "aircraft_id",
              "component_id",
              "component_type",
              "health_status",
              "alert_timestamp"
          )
    )

    



# ✅ Step 21: DLT Pipeline Summary Log Table
# This table captures a snapshot summary after each DLT run, including:
# - Total component records per aircraft and component type
# - Total alert counts per aircraft and health status
# - A current run timestamp to track when the summary was generated
# 
# This acts as a lightweight DLT run log, useful for:
# ✅ Monitoring pipeline execution health
# ✅ Auditing record volumes over time
# ✅ Powering dashboards or reports without running separate queries
# 
# Note: This summary is refreshed on every DLT pipeline run.
import dlt
from pyspark.sql.functions import col, count, when, current_timestamp

@dlt.table(
    comment="Extended sanity check validation across sensor features, component-level twins, alerts, and location mappings",
    table_properties={
        "pipelines.materialize": "true"
    }
)
def post_dlt_sanity_check():
    # Sensor Features Table
    sensor_df = dlt.read("sensor_features")
    sensor_nulls = sensor_df.select([
        count(when(col(c).isNull(), c)).alias(f"sensor_{c}_nulls")
        for c in ["timestamp", "aircraft_id", "engine_temp", "fuel_efficiency", "vibration"]
    ])
    sensor_violations = sensor_df.filter(
        (col("engine_temp") > 1000) | (col("engine_temp") < 0) | (col("vibration") < 0)
    ).agg(count("*").alias("sensor_out_of_range"))
    sensor_dupes = sensor_df.groupBy("aircraft_id", "timestamp").count() \
        .filter("count > 1").agg(count("*").alias("sensor_duplicate_pk"))
    sensor_count = sensor_df.agg(count("*").alias("sensor_row_count"))

    # Landing Gear
    gear_df = dlt.read("twin_landing_gear")
    gear_nulls = gear_df.select([
        count(when(col(c).isNull(), c)).alias(f"gear_{c}_nulls")
        for c in gear_df.columns
    ])
    gear_count = gear_df.agg(count("*").alias("gear_row_count"))

    # Avionics
    avionics_df = dlt.read("twin_avionics")
    avionics_nulls = avionics_df.select([
        count(when(col(c).isNull(), c)).alias(f"avionics_{c}_nulls")
        for c in avionics_df.columns
    ])
    avionics_count = avionics_df.agg(count("*").alias("avionics_row_count"))

    # Cabin Pressurization
    cabin_df = dlt.read("twin_cabin_pressurization")
    cabin_nulls = cabin_df.select([
        count(when(col(c).isNull(), c)).alias(f"cabin_{c}_nulls")
        for c in cabin_df.columns
    ])
    cabin_count = cabin_df.agg(count("*").alias("cabin_row_count"))

    # Airframe
    airframe_df = dlt.read("twin_airframe")
    airframe_nulls = airframe_df.select([
        count(when(col(c).isNull(), c)).alias(f"airframe_{c}_nulls")
        for c in airframe_df.columns
    ])
    airframe_count = airframe_df.agg(count("*").alias("airframe_row_count"))

    # Electrical Systems
    electrical_df = dlt.read("twin_electrical_systems")
    electrical_nulls = electrical_df.select([
        count(when(col(c).isNull(), c)).alias(f"electrical_{c}_nulls")
        for c in electrical_df.columns
    ])
    electrical_count = electrical_df.agg(count("*").alias("electrical_row_count"))

    # Fuel Systems
    fuel_df = dlt.read("twin_fuel_systems")
    fuel_nulls = fuel_df.select([
        count(when(col(c).isNull(), c)).alias(f"fuel_{c}_nulls")
        for c in fuel_df.columns
    ])
    fuel_count = fuel_df.agg(count("*").alias("fuel_row_count"))

    # Hydraulic Systems
    hydraulic_df = dlt.read("twin_hydraulic_systems")
    hydraulic_nulls = hydraulic_df.select([
        count(when(col(c).isNull(), c)).alias(f"hydraulic_{c}_nulls")
        for c in hydraulic_df.columns
    ])
    hydraulic_count = hydraulic_df.agg(count("*").alias("hydraulic_row_count"))

    # Environmental Systems
    environmental_df = dlt.read("twin_environmental_systems")
    environmental_nulls = environmental_df.select([
        count(when(col(c).isNull(), c)).alias(f"environmental_{c}_nulls")
        for c in environmental_df.columns
    ])
    environmental_count = environmental_df.agg(count("*").alias("environmental_row_count"))

    # Component Alerts
    component_alerts = dlt.read("anomaly_alerts_component") \
        .groupBy("health_status") \
        .agg(count("*").alias("component_alert_count")) \
        .withColumnRenamed("health_status", "component_alert_health_status")

    # Combine all sanity check outputs into one row
    result = (
        sensor_nulls.crossJoin(sensor_violations)
                    .crossJoin(sensor_dupes)
                    .crossJoin(sensor_count)
                    .crossJoin(gear_nulls)
                    .crossJoin(gear_count)
                    .crossJoin(avionics_nulls)
                    .crossJoin(avionics_count)
                    .crossJoin(cabin_nulls)
                    .crossJoin(cabin_count)
                    .crossJoin(airframe_nulls)
                    .crossJoin(airframe_count)
                    .crossJoin(electrical_nulls)
                    .crossJoin(electrical_count)
                    .crossJoin(fuel_nulls)
                    .crossJoin(fuel_count)
                    .crossJoin(hydraulic_nulls)
                    .crossJoin(hydraulic_count)
                    .crossJoin(environmental_nulls)
                    .crossJoin(environmental_count)
                    .crossJoin(component_alerts)
                    .withColumn("check_time", current_timestamp())
    )

    return result



# ✅ Step 22: Airport Reference Table (cleaned + materialized)
@dlt.table(
    comment="Materialized reference table for airports, with lat/lon and metadata"
)
def airport_reference():
    return spark.createDataFrame([
        ("SFO", "San Francisco Intl", "San Francisco", 37.6213, -122.3790),
        ("LAX", "Los Angeles Intl", "Los Angeles", 33.9416, -118.4085),
        ("JFK", "John F. Kennedy Intl", "New York", 40.6413, -73.7781),
        ("ORD", "O'Hare Intl", "Chicago", 41.9742, -87.9073),
        ("DFW", "Dallas/Fort Worth Intl", "Dallas", 32.8998, -97.0403)
    ], ["airport_code", "airport_name", "city", "latitude", "longitude"])

# ✅ Step 23: Aircraft-to-Airport Mapping Table

@dlt.table(
    comment="Mapping table linking aircraft IDs to their base airport codes"
)
def aircraft_airport_map():
    return spark.createDataFrame([
        # A320 family
        ("A320_101", "SFO"),
        ("A320_102", "SFO"),
        ("A320_103", "SFO"),
        ("A320_104", "SFO"),
        ("A320_105", "SFO"),
        # B737 family
        ("B737_201", "JFK"),
        ("B737_202", "JFK"),
        ("B737_203", "JFK"),
        ("B737_204", "JFK"),
        ("B737_205", "JFK"),
        ("B737_301", "JFK"),    # ✅ ADD: B737_301
        # A330 family
        ("A330_201", "LAX"),    # ✅ ADD: A330_201
        ("A330_301", "LAX"),
        ("A330_302", "LAX"),
        ("A330_303", "LAX"),
        ("A330_304", "LAX"),
        ("A330_305", "LAX"),
        # B777 family
        ("B777_401", "ORD"),
        ("B777_402", "ORD"),
        ("B777_403", "ORD"),
        ("B777_404", "ORD"),
        ("B777_405", "ORD"),
        # E190 family
        ("E190_501", "SEA"),
        ("E190_502", "SEA"),
        ("E190_503", "SEA"),
        ("E190_504", "SEA"),
        ("E190_505", "SEA"),
    ], ["aircraft_id", "airport_code"])


# ✅ Step 24: Aircraft Location Enriched Table (Aircraft + Airport Metadata + Lat/Lon)
@dlt.table(
    comment="Aircraft mapped with airport metadata and coordinates for geospatial dashboards"
)
def aircraft_location_enriched_v2():
    aircraft_df = dlt.read("aircraft_airport_map")
    airport_df = dlt.read("airport_reference")

    return (
        aircraft_df
        .join(airport_df, on="airport_code", how="left")
        .select(
            "aircraft_id", "airport_code", "airport_name", "city", "latitude", "longitude"
        )
    )

# ✅ Step 25: Aircraft Status Map View (Engine Health + Alert Counts + Location)
@dlt.table(
    comment="Aircraft geolocation with engine health status and anomaly alert counts for map visualization"
)
def aircraft_status_map_view_v2():
    loc = dlt.read("aircraft_location_enriched")
    twin = dlt.read("digital_twin_engine_view")
    alerts = spark.read.table("arao.aerodemo.anomaly_alerts")  # non-DLT table

    return (
        loc
        .join(twin, on="aircraft_id", how="left")
        .join(alerts, on="aircraft_id", how="left")
        .groupBy("aircraft_id", "airport_code", "latitude", "longitude", "engine_health_status")
        .agg(F.count(alerts["aircraft_id"]).alias("alert_count"))
    )



# ✅ Step 26: Component-Level Alert Map View
@dlt.table(
    comment="Aggregated component health alerts by airport location for geospatial dashboards"
)
def component_status_map_view():
    loc = dlt.read("aircraft_location_enriched")
    alerts = dlt.read("anomaly_alerts_component")

    return (
        loc.join(alerts, on="aircraft_id", how="left")
           .groupBy("airport_code", "latitude", "longitude", "component_type", "health_status")
           .agg(F.count("*").alias("alert_count"))
    )