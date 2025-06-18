from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def load_env_configs(env, catalog="arao", schema="aerodemo", table_name="aerodemo_config_store"):
    """
    Load all configs for a given environment as a Python dictionary.
    """
    spark = SparkSession.builder.getOrCreate()
    table_path = f"{catalog}.{schema}.{table_name}"
    
    df = spark.read.table(table_path)
    env_df = df.filter(df.env == env)
    
    configs = {
        row["config_key"]: row["config_value"]
        for row in env_df.collect()
    }
    
    return configs

def upsert_config(env, key, value, catalog="arao", schema="aerodemo", table_name="aerodemo_config_store"):
    """
    Upsert a single config key-value pair for an environment.
    """
    spark = SparkSession.builder.getOrCreate()
    table_path = f"{catalog}.{schema}.{table_name}"
    
    schema_def = StructType([
        StructField("env", StringType(), False),
        StructField("config_key", StringType(), False),
        StructField("config_value", StringType(), False)
    ])
    
    df = spark.createDataFrame([(env, key, value)], schema=schema_def)
    temp_view = "temp_config_upsert"
    df.createOrReplaceTempView(temp_view)
    
    spark.sql(f"""
        MERGE INTO {table_path} AS target
        USING {temp_view} AS source
        ON target.env = source.env AND target.config_key = source.config_key
        WHEN MATCHED THEN UPDATE SET target.config_value = source.config_value
        WHEN NOT MATCHED THEN INSERT (env, config_key, config_value) VALUES (source.env, source.config_key, source.config_value)
    """)
def upsert_multiple_configs(env, config_dict, catalog="arao", schema="aerodemo", table_name="aerodemo_config_store"):
    """
    Upsert multiple configs provided as a dictionary {key: value}.
    """
    spark = SparkSession.builder.getOrCreate()
    table_path = f"{catalog}.{schema}.{table_name}"

    schema_def = StructType([
        StructField("env", StringType(), False),
        StructField("config_key", StringType(), False),
        StructField("config_value", StringType(), False)
    ])

    # Convert dict to list of tuples [(env, key, value), ...]
    data_list = [(env, k, v) for k, v in config_dict.items()]

    df = spark.createDataFrame(data_list, schema=schema_def)
    temp_view = "temp_config_batch_upsert"
    df.createOrReplaceTempView(temp_view)

    spark.sql(f"""
        MERGE INTO {table_path} AS target
        USING {temp_view} AS source
        ON target.env = source.env AND target.config_key = source.config_key
        WHEN MATCHED THEN UPDATE SET target.config_value = source.config_value
        WHEN NOT MATCHED THEN INSERT (env, config_key, config_value) VALUES (source.env, source.config_key, source.config_value)
    """)

def delete_env_configs(env, catalog="arao", schema="aerodemo", table_name="aerodemo_config_store"):
    """
    Delete all configs for a given environment.
    """
    spark = SparkSession.builder.getOrCreate()
    table_path = f"{catalog}.{schema}.{table_name}"
    
    spark.sql(f"DELETE FROM {table_path} WHERE env = '{env}'")

# Optional: declare all exposed functions
__all__ = [
    "load_env_configs",
    "upsert_config",
    "upsert_multiple_configs",
    "delete_env_configs"
]