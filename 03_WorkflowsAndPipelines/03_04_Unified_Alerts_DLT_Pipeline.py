import dlt
from pyspark.sql.functions import col, lit, to_date, current_timestamp, when, count, countDistinct, avg
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

# DLT Pipeline for Unified Alerts View with Inference Traceability
# This pipeline creates a unified view that combines all component-specific alert tables
# into a single view for easy visualization and dashboarding with full traceability back to source inference.

@dlt.table(
    comment="Unified view combining all component-specific alert tables for comprehensive dashboarding with inference traceability",
    table_properties={
        "pipelines.materialize": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["alert_date", "component_id"]
)
def unified_alerts_view():
    """
    Creates a unified view that combines all component-specific alert tables.
    Includes alert date, aircraft ID, component name, component ID, and inference traceability.
    """
    
    # Define component configurations with inference table mappings
    components = [
        {
            "name": "Aircraft",
            "alert_table": "arao.aerodemo.aircraft_alerts",
            "inference_table": "arao.aerodemo.prediction_results",
            "component_id": "AC",
            "description": "Full Aircraft Health"
        },
        {
            "name": "Engine",
            "alert_table": "arao.aerodemo.engine_alerts",
            "inference_table": "arao.aerodemo.prediction_results_engine",
            "component_id": "ENG",
            "description": "Engine System"
        },
        {
            "name": "Landing Gear",
            "alert_table": "arao.aerodemo.landinggear_alerts",
            "inference_table": "arao.aerodemo.prediction_results_landinggear",
            "component_id": "LG",
            "description": "Landing Gear System"
        },
        {
            "name": "Avionics",
            "alert_table": "arao.aerodemo.avionics_alerts",
            "inference_table": "arao.aerodemo.prediction_results_avionics",
            "component_id": "AV",
            "description": "Avionics System"
        },
        {
            "name": "Cabin Pressurization",
            "alert_table": "arao.aerodemo.cabinpressurization_alerts",
            "inference_table": "arao.aerodemo.prediction_results_cabinpressurization",
            "component_id": "CP",
            "description": "Cabin Pressurization System"
        },
        {
            "name": "Airframe",
            "alert_table": "arao.aerodemo.airframe_alerts",
            "inference_table": "arao.aerodemo.prediction_results_airframe",
            "component_id": "AF",
            "description": "Airframe Structure"
        },
        {
            "name": "Electrical Systems",
            "alert_table": "arao.aerodemo.electricalsystems_alerts",
            "inference_table": "arao.aerodemo.prediction_results_electricalsystems",
            "component_id": "ES",
            "description": "Electrical Systems"
        },
        {
            "name": "Fuel Systems",
            "alert_table": "arao.aerodemo.fuelsystems_alerts",
            "inference_table": "arao.aerodemo.prediction_results_fuelsystems",
            "component_id": "FS",
            "description": "Fuel Systems"
        },
        {
            "name": "Hydraulic Systems",
            "alert_table": "arao.aerodemo.hydraulicsystems_alerts",
            "inference_table": "arao.aerodemo.prediction_results_hydraulicsystems",
            "component_id": "HS",
            "description": "Hydraulic Systems"
        },
        {
            "name": "Environmental Systems",
            "alert_table": "arao.aerodemo.environmentalsystems_alerts",
            "inference_table": "arao.aerodemo.prediction_results_environmentalsystems",
            "component_id": "ENV",
            "description": "Environmental Systems"
        },
        {
            "name": "Auxiliary Systems",
            "alert_table": "arao.aerodemo.auxiliarysystems_alerts",
            "inference_table": "arao.aerodemo.prediction_results_auxiliarysystems",
            "component_id": "AUX",
            "description": "Auxiliary Systems"
        }
    ]
    
    # List to store all component DataFrames
    all_alerts_dfs = []
    
    # Process each component
    for component in components:
        try:
            # Read the component's alert table
            df = spark.read.table(component['alert_table'])
            
            # Add component metadata and inference traceability
            df_with_metadata = df.withColumn("component_name", lit(component['name'])) \
                                .withColumn("component_id", lit(component['component_id'])) \
                                .withColumn("component_description", lit(component['description'])) \
                                .withColumn("alert_source", lit(component['alert_table'])) \
                                .withColumn("inference_source", lit(component['inference_table'])) \
                                .withColumn("processed_at", current_timestamp()) \
                                .withColumn("inference_trace_id", 
                                           F.concat(
                                               col("aircraft_id"), 
                                               lit("_"), 
                                               col("component_id"), 
                                               lit("_"), 
                                               F.date_format(col("timestamp"), "yyyyMMdd_HHmmss")
                                           ))
            
            # Ensure we have the required columns
            required_columns = ["timestamp", "aircraft_id", "predicted_anomaly", "alert_day", "batch_id"]
            available_columns = df_with_metadata.columns
            
            # Select only the columns we need for the unified view
            select_columns = []
            for col_name in required_columns:
                if col_name in available_columns:
                    select_columns.append(col_name)
                else:
                    # If column doesn't exist, use null
                    select_columns.append(lit(None).alias(col_name))
            
            # Add the metadata columns
            select_columns.extend([
                "component_name",
                "component_id", 
                "component_description",
                "alert_source",
                "inference_source",
                "inference_trace_id",
                "processed_at"
            ])
            
            # Select and add to our list
            unified_df = df_with_metadata.select(*select_columns)
            all_alerts_dfs.append(unified_df)
            
        except Exception as e:
            # Continue with other components even if one fails
            print(f"Warning: Error processing {component['name']} alerts: {str(e)}")
            continue
    
    if not all_alerts_dfs:
        # Return empty DataFrame with correct schema if no alerts processed
        return spark.createDataFrame([], "timestamp timestamp, aircraft_id string, predicted_anomaly int, alert_day date, batch_id string, component_name string, component_id string, component_description string, alert_source string, inference_source string, inference_trace_id string, processed_at timestamp")
    
    # Union all DataFrames
    unified_alerts = all_alerts_dfs[0]
    for df in all_alerts_dfs[1:]:
        unified_alerts = unified_alerts.union(df)
    
    # Add additional computed columns for easier analysis
    final_unified_alerts = unified_alerts.withColumn("alert_date", to_date(col("timestamp"))) \
                                        .withColumn("alert_hour", col("timestamp").hour()) \
                                        .withColumn("alert_severity", 
                                                   when(col("predicted_anomaly") == 1, "HIGH")
                                                   .otherwise("LOW")) \
                                        .withColumn("alert_category", 
                                                   when(col("component_id").isin(["ENG", "LG", "AF"]), "CRITICAL")
                                                   .when(col("component_id").isin(["AV", "CP", "ES"]), "SAFETY")
                                                   .otherwise("OPERATIONAL"))
    
    return final_unified_alerts

@dlt.table(
    comment="Inference traceability table linking alerts back to their source inference records",
    table_properties={
        "pipelines.materialize": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["alert_date", "component_id"]
)
def inference_traceability():
    """
    Creates a traceability table that links alerts back to their source inference records.
    This enables drilling down from alerts to the original inference that generated them.
    """
    
    # Read from the unified alerts view
    unified_df = dlt.read("unified_alerts_view")
    
    # Define component configurations for inference tables
    components = [
        {"name": "Aircraft", "inference_table": "arao.aerodemo.prediction_results", "component_id": "AC"},
        {"name": "Engine", "inference_table": "arao.aerodemo.prediction_results_engine", "component_id": "ENG"},
        {"name": "Landing Gear", "inference_table": "arao.aerodemo.prediction_results_landinggear", "component_id": "LG"},
        {"name": "Avionics", "inference_table": "arao.aerodemo.prediction_results_avionics", "component_id": "AV"},
        {"name": "Cabin Pressurization", "inference_table": "arao.aerodemo.prediction_results_cabinpressurization", "component_id": "CP"},
        {"name": "Airframe", "inference_table": "arao.aerodemo.prediction_results_airframe", "component_id": "AF"},
        {"name": "Electrical Systems", "inference_table": "arao.aerodemo.prediction_results_electricalsystems", "component_id": "ES"},
        {"name": "Fuel Systems", "inference_table": "arao.aerodemo.prediction_results_fuelsystems", "component_id": "FS"},
        {"name": "Hydraulic Systems", "inference_table": "arao.aerodemo.prediction_results_hydraulicsystems", "component_id": "HS"},
        {"name": "Environmental Systems", "inference_table": "arao.aerodemo.prediction_results_environmentalsystems", "component_id": "ENV"},
        {"name": "Auxiliary Systems", "inference_table": "arao.aerodemo.prediction_results_auxiliarysystems", "component_id": "AUX"}
    ]
    
    traceability_dfs = []
    
    for component in components:
        try:
            # Get alerts for this component
            component_alerts = unified_df.filter(col("component_id") == component["component_id"])
            
            if component_alerts.count() > 0:
                # Read the corresponding inference table
                inference_df = spark.read.table(component["inference_table"])
                
                # Join alerts with inference data
                joined_df = component_alerts.join(
                    inference_df,
                    on=["aircraft_id", "timestamp"],
                    how="left"
                ).withColumn("component_name", lit(component["name"])) \
                 .withColumn("component_id", lit(component["component_id"])) \
                 .withColumn("inference_table", lit(component["inference_table"])) \
                 .withColumn("trace_created_at", current_timestamp())
                
                traceability_dfs.append(joined_df)
                
        except Exception as e:
            print(f"Warning: Error creating traceability for {component['name']}: {str(e)}")
            continue
    
    if not traceability_dfs:
        return spark.createDataFrame([], "aircraft_id string, timestamp timestamp, component_name string, component_id string, inference_table string, trace_created_at timestamp")
    
    # Union all traceability DataFrames
    traceability = traceability_dfs[0]
    for df in traceability_dfs[1:]:
        traceability = traceability.union(df)
    
    return traceability

@dlt.table(
    comment="Summary statistics table for alerts by component, category, and date for dashboarding",
    table_properties={
        "pipelines.materialize": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["alert_date"]
)
def alert_summary():
    """
    Creates a summary table with alert statistics for dashboarding.
    """
    
    # Read from the unified alerts view
    unified_df = dlt.read("unified_alerts_view")
    
    # Create summary statistics
    summary_df = unified_df.groupBy(
        "component_name",
        "component_id", 
        "alert_category",
        "alert_severity",
        "alert_date"
    ).agg(
        F.count("*").alias("alert_count"),
        F.countDistinct("aircraft_id").alias("affected_aircraft_count")
    ).orderBy("alert_date", "alert_count")
    
    return summary_df

@dlt.table(
    comment="Real-time alert dashboard view with latest alert status by aircraft and component",
    table_properties={
        "pipelines.materialize": "true"
    }
)
def alert_dashboard_view():
    """
    Creates a real-time dashboard view with the latest alert status for each aircraft and component.
    """
    
    # Read from the unified alerts view
    unified_df = dlt.read("unified_alerts_view")
    
    # Get the latest alert for each aircraft-component combination
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window_spec = Window.partitionBy("aircraft_id", "component_id").orderBy(col("timestamp").desc())
    
    latest_alerts = unified_df.withColumn("row_num", row_number().over(window_spec)) \
                              .filter(col("row_num") == 1) \
                              .drop("row_num")
    
    return latest_alerts

@dlt.table(
    comment="Alert trends analysis table showing alert patterns over time",
    table_properties={
        "pipelines.materialize": "true"
    },
    partition_cols=["alert_date"]
)
def alert_trends():
    """
    Creates an alert trends analysis table showing alert patterns over time.
    """
    
    # Read from the unified alerts view
    unified_df = dlt.read("unified_alerts_view")
    
    # Create trend analysis
    trends_df = unified_df.groupBy(
        "alert_date",
        "component_id",
        "alert_category",
        "alert_severity"
    ).agg(
        F.count("*").alias("daily_alert_count"),
        F.countDistinct("aircraft_id").alias("daily_affected_aircraft"),
        F.avg("predicted_anomaly").alias("avg_anomaly_score")
    ).orderBy("alert_date", "component_id")
    
    return trends_df

@dlt.view(
    comment="Unified alerts view for easy SQL queries and dashboard integration with inference traceability"
)
def unified_alerts_sql_view():
    """
    Creates a SQL view for easy querying of unified alerts with inference traceability.
    """
    
    unified_df = dlt.read("unified_alerts_view")
    
    return unified_df.select(
        "timestamp",
        "aircraft_id",
        "predicted_anomaly",
        "alert_day",
        "alert_date",
        "alert_hour",
        "alert_severity",
        "alert_category",
        "component_name",
        "component_id",
        "component_description",
        "alert_source",
        "inference_source",
        "inference_trace_id",
        "batch_id",
        "processed_at"
    )

@dlt.view(
    comment="Drill-down view for getting source inference data from alert traceability"
)
def alert_to_inference_drilldown():
    """
    Creates a drill-down view that allows going from alerts to source inference data.
    """
    
    traceability_df = dlt.read("inference_traceability")
    
    return traceability_df.select(
        "aircraft_id",
        "timestamp",
        "component_name",
        "component_id",
        "inference_table",
        "predicted_anomaly",
        "alert_severity",
        "alert_category",
        "inference_trace_id",
        "trace_created_at"
    ) 