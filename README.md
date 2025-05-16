# Databricks AeroDemo

This repository contains a demonstration of building an anomaly detection pipeline using Databricks and MLflow. The workflow includes data generation, model training, inference, and alerting mechanisms.

## Notebooks Overview

1. **00_Instructions_and_Workflow.ipynb**: Provides an overview and instructions for the demo.
2. **01_Table_Creation.ipynb**: Sets up the necessary Delta tables.
3. **02_Synthetic_Data_Generation.ipynb**: Generates synthetic sensor data for model training.
4. **03_DLT_Pipeline.ipynb**: Demonstrates how to set up a Delta Live Tables pipeline for continuous data processing.
5. **04_Model_Training_And_Registration.ipynb**: Trains a RandomForestClassifier, logs the model with MLflow, and registers it in Unity Catalog with a model signature.
6. **05_Model_Inference.ipynb**: Loads the registered model and performs inference on new data.
7. **06_Anomaly_Alert_Logger.ipynb**: Filters high-risk predictions and writes them to a Delta table for alerting purposes.

## Getting Started

To run the demo:

1. Clone this repository to your Databricks workspace.
2. Follow the sequence of notebooks as outlined above.
3. Ensure that you have the necessary permissions and configurations to create Delta tables and register models in Unity Catalog.

## Requirements

- Databricks workspace with Unity Catalog enabled.
- MLflow for model tracking and registration.
- Delta Live Tables for pipeline orchestration.

## License

This project is licensed under the MIT License.
