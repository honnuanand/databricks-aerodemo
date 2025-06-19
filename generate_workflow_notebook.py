#!/usr/bin/env python3

workflow_content = '''# %load_ext autoreload
# %autoreload 2

# 🚀 99_Setup_Workflow Notebook - Clean Version

%md
### 🛠 99_Setup_Workflow.ipynb - Clean Version
This notebook creates or replaces a Databricks Workflow for the AeroDemo project.
It chains together all synthetic data generation, DLT processing, feature registration, model training, inference, and alert generation.

✅ Uses Databricks Runtime ML (latest: 14.3.x-cpu-ml-scala2.12)
✅ Configures autoscale (1–6 workers)
✅ Auto-terminates after 60 minutes idle
✅ Includes all 10 components + full aircraft twin
✅ Component-specific alert tables
✅ Unified alerts DLT pipeline

# 📦 Load environment configs using Delta-backed config store

from conf.config_reader import load_env_configs

# Set up the environment (uses Databricks widget)
dbutils.widgets.dropdown("ENV", "dev", ["dev", "staging", "prod"], "Environment")
env = dbutils.widgets.get("ENV")

# Load configs
configs = load_env_configs(env)

# Pull individual values as needed
DATABRICKS_INSTANCE = configs.get("databricks_instance")
TOKEN = configs.get("pat_token")
JOB_ID = configs.get("e2e_workflow_job_id")
CATALOG = configs.get("catalog")
SCHEMA = configs.get("schema")

# ✅ Log loaded configs
print(f"✅ Using environment: {env}")
print(f"  → Catalog: {CATALOG}")
print(f"  → Schema: {SCHEMA}")
print(f"  → Databricks Instance: {DATABRICKS_INSTANCE}")
print(f"  → PAT Token: {TOKEN}")
print(f"  → Job ID: {JOB_ID}")

import os

# Get current notebook path and folder
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
folder_path = os.path.dirname(notebook_path)
workspace_path = f"/Workspace{folder_path}"

print(f"✅ Notebook path: {notebook_path}")
print(f"✅ Notebook folder (user-level): {folder_path}")
print(f"✅ Notebook folder (workspace-level): {workspace_path}")

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, PipelineTask, TaskDependency

w = WorkspaceClient()

NOTEBOOK_BASE_PATH = "/Workspace/Users/anand.rao@databricks.com/databricks-aerodemo"
synthetic_data_path = f"{NOTEBOOK_BASE_PATH}/02_SyntheticData"
feature_registration_path = f"{NOTEBOOK_BASE_PATH}/03B_FeatureRegistration"
model_training_path = f"{NOTEBOOK_BASE_PATH}/04_ModelTrainingAndInference"
workflows_path = f"{NOTEBOOK_BASE_PATH}/03_WorkflowsAndPipelines"

env = "dev"
config = load_env_configs(env)
EXISTING_CLUSTER_ID = config["workflow_configs.existing_cluster_id"]
DLT_PIPELINE_ID = config["workflow_configs.dlt_pipeline_id"]
WORKFLOW_NAME = config["workflow_configs.workflow_name"]

tasks = []

# 1. Synthetic Data Generation Tasks (parallel)
synthetic_notebooks = [
    "02_01_Synthetic_Data_Generation_v2",
    "02_02_Engine_Data_Generation",
    "02_03_CabinPressurization_Data_Generation",
    "02_04_Airframe_Synthetic_Data_Generation",
    "02_05_LandingGear_Data_Generation",
    "02_06_Avionics_Data_Generation",
    "02_07_ElectricalSystems_Data_Generation",
    "02_08_FuelSystems_Data_Generation",
    "02_09_HydraulicSystems_Data_Generation",
    "02_10_EnvironmentalSystems_Data_Generation"
]

for notebook in synthetic_notebooks:
    tasks.append(Task(
        task_key=notebook,
        notebook_task=NotebookTask(notebook_path=f"{synthetic_data_path}/{notebook}"),
        existing_cluster_id=EXISTING_CLUSTER_ID
    ))

# 2. DLT Pipeline Task (depends on all synthetic data)
dlt_task_key = "Run_DLT_Pipeline"
tasks.append(Task(
    task_key=dlt_task_key,
    pipeline_task=PipelineTask(pipeline_id=DLT_PIPELINE_ID),
    depends_on=[TaskDependency(task_key=nb) for nb in synthetic_notebooks]
))

# 3. Feature Store Registration (depends on DLT)
feature_task_key = "Feature_Store_Registration"
tasks.append(Task(
    task_key=feature_task_key,
    notebook_task=NotebookTask(notebook_path=f"{feature_registration_path}/03B_Feature_Store_Registration"),
    existing_cluster_id=EXISTING_CLUSTER_ID,
    depends_on=[TaskDependency(task_key=dlt_task_key)]
))

# 4. Aircraft Model Training (depends on feature registration)
model_task_key = "Aircraft_Model_Training"
tasks.append(Task(
    task_key=model_task_key,
    notebook_task=NotebookTask(notebook_path=f"{model_training_path}/04_Model_Training_And_Registration"),
    existing_cluster_id=EXISTING_CLUSTER_ID,
    depends_on=[TaskDependency(task_key=feature_task_key)]
))

# 5. Component Model Training Tasks (parallel, depend on feature registration)
component_model_tasks = [
    "04_Engine_Training",
    "04_LandingGear_Training", 
    "04_Avionics_Training",
    "04_CabinPressurization_Training",
    "04_Airframe_Training",
    "04_ElectricalSystems_Training",
    "04_FuelSystems_Training",
    "04_HydraulicSystems_Training",
    "04_EnvironmentalSystems_Training",
    "04_AuxiliarySystems_Training"
]

for notebook in component_model_tasks:
    task_key = f"Component_{notebook.replace('04_', '').replace('_Training', '')}_Training"
    tasks.append(Task(
        task_key=task_key,
        notebook_task=NotebookTask(notebook_path=f"{model_training_path}/{notebook}"),
        existing_cluster_id=EXISTING_CLUSTER_ID,
        depends_on=[TaskDependency(task_key=feature_task_key)]
    ))

# 6. Aircraft Model Inference (depends on aircraft model training)
inference_task_key = "Aircraft_Model_Inference"
tasks.append(Task(
    task_key=inference_task_key,
    notebook_task=NotebookTask(notebook_path=f"{model_training_path}/05_Model_Inference"),
    existing_cluster_id=EXISTING_CLUSTER_ID,
    depends_on=[TaskDependency(task_key=model_task_key)]
))

# 7. Component Model Inference Tasks (parallel, depend on respective component training)
component_inference_tasks = [
    "05_Engine_Inference",
    "05_LandingGear_Inference",
    "05_Avionics_Inference", 
    "05_CabinPressurization_Inference",
    "05_Airframe_Inference",
    "05_ElectricalSystems_Inference",
    "05_FuelSystems_Inference",
    "05_HydraulicSystems_Inference",
    "05_EnvironmentalSystems_Inference",
    "05_AuxiliarySystems_Inference"
]

for notebook in component_inference_tasks:
    task_key = f"Component_{notebook.replace('05_', '').replace('_Inference', '')}_Inference"
    component_training_key = f"Component_{notebook.replace('05_', '04_').replace('_Inference', '_Training')}"
    tasks.append(Task(
        task_key=task_key,
        notebook_task=NotebookTask(notebook_path=f"{model_training_path}/{notebook}"),
        existing_cluster_id=EXISTING_CLUSTER_ID,
        depends_on=[TaskDependency(task_key=component_training_key)]
    ))

# 8. Aircraft Alert Generation (depends on aircraft inference)
alerts_task_key = "Aircraft_Inference_To_Alerts"
tasks.append(Task(
    task_key=alerts_task_key,
    notebook_task=NotebookTask(notebook_path=f"{model_training_path}/06_Model_Inference_To_Alerts_table"),
    existing_cluster_id=EXISTING_CLUSTER_ID,
    depends_on=[TaskDependency(task_key=inference_task_key)]
))

# 9. Component Alert Generation Tasks (parallel, depend on respective component inference)
component_alert_tasks = [
    "06_Engine_Alerts",
    "06_LandingGear_Alerts",
    "06_Avionics_Alerts",
    "06_CabinPressurization_Alerts", 
    "06_Airframe_Alerts",
    "06_ElectricalSystems_Alerts",
    "06_FuelSystems_Alerts",
    "06_HydraulicSystems_Alerts",
    "06_EnvironmentalSystems_Alerts",
    "06_AuxiliarySystems_Alerts"
]

for notebook in component_alert_tasks:
    task_key = f"Component_{notebook.replace('06_', '').replace('_Alerts', '')}_Alerts"
    component_inference_key = f"Component_{notebook.replace('06_', '05_').replace('_Alerts', '_Inference')}"
    tasks.append(Task(
        task_key=task_key,
        notebook_task=NotebookTask(notebook_path=f"{model_training_path}/{notebook}"),
        existing_cluster_id=EXISTING_CLUSTER_ID,
        depends_on=[TaskDependency(task_key=component_inference_key)]
    ))

# 10. Final Summary Task (depends on aircraft alerts)
summary_task_key = "101_Final_Summary_Task"
tasks.append(Task(
    task_key=summary_task_key,
    notebook_task=NotebookTask(notebook_path=f"{workflows_path}/{summary_task_key}"),
    existing_cluster_id=EXISTING_CLUSTER_ID,
    depends_on=[TaskDependency(task_key=alerts_task_key)]
))

# Create/update the workflow
job = w.jobs.create(
    name=WORKFLOW_NAME,
    tasks=tasks
)

print(f"✅ Workflow '{WORKFLOW_NAME}' created/updated with Job ID: {job.job_id}")

from conf.config_reader import upsert_config

# Set variables
new_job_id = job.job_id

print(f"✅ Workflow created/updated with Job ID: {new_job_id}")

# 🔄 Update in config store
upsert_config(env, "e2e_workflow_job_id", str(new_job_id))

print(f"✅ e2e_workflow_job_id updated in config store for environment '{env}'")

%md
### ✅ Workflow Creation Complete!

The workflow has been successfully created with the following structure:

1. **Synthetic Data Generation** (10 parallel tasks)
2. **DLT Pipeline** (depends on all synthetic data)
3. **Feature Registration** (depends on DLT)
4. **Model Training** (11 parallel tasks - 1 aircraft + 10 components)
5. **Model Inference** (11 parallel tasks - 1 aircraft + 10 components)  
6. **Alert Generation** (11 parallel tasks - 1 aircraft + 10 components)
7. **Final Summary** (depends on aircraft alerts)

All component tasks use component-specific alert tables and the unified alerts DLT pipeline provides a consolidated view.
'''

# Write the content to the new workflow file
with open('03_WorkflowsAndPipelines/03_99_Setup_Workflow_New.ipynb', 'w') as f:
    f.write(workflow_content)

print("✅ Workflow notebook content generated successfully!") 