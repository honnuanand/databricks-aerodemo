import dlt
from pyspark.sql.functions import col, to_date, avg, lag, datediff
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# 1. Ingest raw_sensor_data from volume
@dlt.table(
    comment="Ingested raw sensor data from aircraft"
)
def raw_sensor_data():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/arao/aerodemo/tmp/raw/schema/raw_sensor_data")
        .load("/Volumes/arao/aerodemo/tmp/raw")
    )

# 2. Ingest maintenance_events from volume
@dlt.table(
    comment="Ingested maintenance event logs per aircraft"
)
@dlt.expect("valid_aircraft_id", "aircraft_id IS NOT NULL")
@dlt.expect("valid_event_type", "event_type IN ('Routine Check', 'Engine Repair')")
def maintenance_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/arao/aerodemo/tmp/maintenance/schema")
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
@dlt.table(
    comment="Sensor data enriched with most recent maintenance event info and aircraft metadata"
)
def enriched_sensor_data():
    cleaned_df = dlt.read("cleaned_sensor_data").withColumn("reading_date", to_date("timestamp"))
    events_df = dlt.read("maintenance_events").withColumnRenamed("event_date", "maint_date")
    model_df = dlt.read("aircraft_model_reference_dlt")

    # Drop _rescued_data if present
    if '_rescued_data' in cleaned_df.columns:
        cleaned_df = cleaned_df.drop('_rescued_data')
    if '_rescued_data' in events_df.columns:
        events_df = events_df.drop('_rescued_data')

    # Join with maintenance events
    joined = cleaned_df.join(events_df, "aircraft_id", "left") \
        .filter(col("maint_date") <= col("reading_date"))

    # Select most recent maintenance event
    window = Window.partitionBy("aircraft_id", "timestamp").orderBy(col("maint_date").desc())
    enriched = joined.withColumn("rank", F.row_number().over(window)) \
                     .filter(col("rank") == 1) \
                     .drop("rank", "reading_date")

    # Join with aircraft model reference
    final_df = enriched.join(model_df, on="model", how="left")

    return final_df

@dlt.table(
    comment="Engineered sensor features for ML model input",
    table_properties={
        "pipelines.materialize": "true",
        "delta.constraints.primaryKey": "aircraft_id timestamp"
    }
)
def sensor_features():
    df = dlt.read("enriched_sensor_data").withColumn("date", to_date("timestamp"))

    # Cast anomaly_score to int (safely)
    df = df.withColumn("anomaly_score", col("anomaly_score").cast("int"))

    rolling = Window.partitionBy("aircraft_id").orderBy("date").rowsBetween(-6, 0)
    lag_window = Window.partitionBy("aircraft_id").orderBy("date")

    df = (
        df.withColumn("engine_rpm", col("engine_rpm").cast("int"))
          .withColumn("prev_anomaly", lag("anomaly_score", 1).over(lag_window).cast("double"))
          .withColumn("avg_engine_temp_7d", avg("engine_temp").over(rolling).cast("double"))
          .withColumn("avg_vibration_7d", avg("vibration").over(rolling).cast("double"))
          .withColumn("avg_rpm_7d", avg("engine_rpm").over(rolling).cast("double"))
          .withColumn("days_since_maint", datediff("date", F.coalesce(col("maint_date"), F.lit("1970-01-01"))).cast("int"))
    )

    return df.select(
        "timestamp", "aircraft_id", "model", 
        col("engine_temp").cast("double"),
        col("fuel_efficiency").cast("double"),
        col("vibration").cast("double"),
        col("altitude").cast("double"),
        col("airspeed").cast("double"),
        col("oil_pressure").cast("double"),
        "engine_rpm", 
        col("battery_voltage").cast("double"),
        "anomaly_score", "event_type", 
        "avg_engine_temp_7d", "avg_vibration_7d", "avg_rpm_7d", 
        "prev_anomaly", "days_since_maint",
        "manufacturer", "engine_type", 
        col("capacity").cast("int"), 
        col("range_km").cast("int")
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
            ((col("avg_engine_temp_7d") / 700 + col("avg_vibration_7d") / 25) * 0.5) +
            (col("prev_anomaly") * 0.3) +
            ((col("days_since_maint") / 100.0) * 0.2)
        > 0.5).cast("int")
    )