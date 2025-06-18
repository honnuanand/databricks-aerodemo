#!/usr/bin/env python3
"""
Script to update the workflow setup notebook to include the unified alerts view task
"""

import json

# Read the current workflow notebook
with open('03_WorkflowsAndPipelines/03_99_Setup_Workflow.ipynb', 'r') as f:
    workflow_data = json.load(f)

# Find the cell with the task definitions
task_cell_index = None
for i, cell in enumerate(workflow_data['cells']):
    if 'source' in cell and isinstance(cell['source'], list):
        for line in cell['source']:
            if 'summary_task_key = "101_Final_Summary_Task"' in line:
                task_cell_index = i
                break
    if task_cell_index is not None:
        break

if task_cell_index is None:
    print("Could not find the task definition cell")
    exit(1)

# Get the current task cell content
task_cell = workflow_data['cells'][task_cell_index]
task_source = task_cell['source']

# Find where to insert the unified alerts task (before the summary task)
summary_task_start = None
for i, line in enumerate(task_source):
    if 'summary_task_key = "101_Final_Summary_Task"' in line:
        summary_task_start = i
        break

if summary_task_start is None:
    print("Could not find summary task section")
    exit(1)

# Create the new task cell content
new_task_source = []

# Add content up to the summary task
for i in range(summary_task_start):
    new_task_source.append(task_source[i])

# Add the unified alerts task
new_task_source.extend([
    '\n',
    '# Unified Alerts View Task\n',
    'unified_alerts_task_key = "Unified_Alerts_View"\n',
    'tasks.append(Task(\n',
    '    task_key=unified_alerts_task_key,\n',
    '    spark_python_task=SparkPythonTask(\n',
    '        python_file=f"{model_training_path}/07_Unified_Alerts_View.py"\n',
    '    ),\n',
    '    existing_cluster_id=EXISTING_CLUSTER_ID,\n',
    '    depends_on=[TaskDependency(task_key=alert_key) for alert_key in all_alert_tasks]\n',
    '))\n',
    '\n'
])

# Add the rest of the content (summary task and beyond)
for i in range(summary_task_start, len(task_source)):
    new_task_source.append(task_source[i])

# Update the task cell
workflow_data['cells'][task_cell_index]['source'] = new_task_source

# Write the updated notebook
with open('03_WorkflowsAndPipelines/03_99_Setup_Workflow.ipynb', 'w') as f:
    json.dump(workflow_data, f, indent=2)

print("✅ Workflow notebook updated with unified alerts view task!") 