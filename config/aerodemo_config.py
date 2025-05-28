CONFIG = {
    "catalog": "arao",
    "schema": "aerodemo",
    "pipeline_ids": {
        "full_pipeline": "pipeline-id-here",
        "registration_pipeline": "pipeline-id-here"
    },
    "pat_token": "dapi-XXXXXX",
    "databricks_instance": "https://e2-demo-field-eng.cloud.databricks.com",
    "e2e_workflow_job_id": "864722071013094"  # Add this for E2E workflow
}CONFIG = {
    "dev": {
        "catalog": "arao_dev",
        "schema": "aerodemo_dev",
        "pipeline_ids": {
            "full_pipeline": "dev-pipeline-id-here",
            "registration_pipeline": "dev-registration-pipeline-id-here"
        },
        "pat_token": "dapi-DEV-XXXXXX",
        "databricks_instance": "https://dev-demo-field-eng.cloud.databricks.com",
        "e2e_workflow_job_id": "dev-864722071013094"
    },
    "staging": {
        "catalog": "arao_staging",
        "schema": "aerodemo_staging",
        "pipeline_ids": {
            "full_pipeline": "staging-pipeline-id-here",
            "registration_pipeline": "staging-registration-pipeline-id-here"
        },
        "pat_token": "dapi-STAGING-XXXXXX",
        "databricks_instance": "https://staging-demo-field-eng.cloud.databricks.com",
        "e2e_workflow_job_id": "staging-864722071013094"
    },
    "prod": {
        "catalog": "arao",
        "schema": "aerodemo",
        "pipeline_ids": {
            "full_pipeline": "prod-pipeline-id-here",
            "registration_pipeline": "prod-registration-pipeline-id-here"
        },
        "pat_token": "dapi-PROD-XXXXXX",
        "databricks_instance": "https://e2-demo-field-eng.cloud.databricks.com",
        "e2e_workflow_job_id": "864722071013094"
    }
}

def get_config(env="dev"):
    """
    Retrieve the configuration for the specified environment.
    
    Args:
        env (str): The environment to retrieve the configuration for. 
                   Options: 'dev', 'staging', 'prod'. Default is 'dev'.
    
    Returns:
        dict: The configuration dictionary for the specified environment.
    """
    if env not in CONFIG:
        raise ValueError(f"Invalid environment: {env}. Choose from 'dev', 'staging', or 'prod'.")
    return CONFIG[env]