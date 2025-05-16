# ✈️ Databricks AeroDemo

![Aircraft Banner](https://cdn.pixabay.com/photo/2015/05/15/14/42/airplane-768526_960_720.jpg)  
*A simulation of sensor intelligence and predictive maintenance with Databricks + MLflow.*

---

## 🚀 What is This?

This project demonstrates a complete end-to-end **aircraft anomaly detection pipeline** using **Databricks**, including:

- 🔄 Auto-ingestion of synthetic sensor data with Delta Live Tables (DLT)
- 🧹 Data cleaning and enrichment with expectations
- 🧠 ML model training and registration using MLflow
- 🔮 Real-time inference and anomaly prediction
- ⚠️ Writing high-risk alerts to Delta for operational monitoring

---

## 📘 Notebook Guide

| Notebook | Purpose |
|----------|---------|
| `00_Instructions_and_Workflow.ipynb` | ✅ Overview and how to run the demo |
| `01_Table_Creation.ipynb` | 🗂️ Create Delta tables (`raw_sensor_data`, `maintenance_events`, etc.) |
| `02_Synthetic_Data_Generation.ipynb` | 🛠️ Generate sensor readings + maintenance logs |
| `03_DLT_Pipeline.ipynb` | 🔄 DLT pipeline for ingestion and cleaning |
| `04_Model_Training_And_Registration.ipynb` | 🤖 Train & register RandomForest model using MLflow |
| `05_Model_Inference.ipynb` | 🔍 Perform predictions using registered model |
| `06_Anomaly_Alert_Logger.ipynb` | 🚨 Save anomalies to a dedicated alerts table |

---

## 🛠️ Requirements

- 🔐 Unity Catalog-enabled Databricks Workspace
- 🧪 MLflow Tracking & Model Registry (Unity Catalog-backed)
- 💾 Delta Live Tables enabled
- 🐍 Python libraries: `scikit-learn`, `pandas`, `mlflow`

---

## 🧪 Tech Stack

- **Databricks + Unity Catalog**
- **Delta Live Tables (DLT)**
- **Structured Streaming**
- **MLflow Model Registry**
- **scikit-learn / pandas / PySpark**

---

## 🧰 Setup Instructions

1. Clone this repo into your Databricks Workspace.
2. Run the notebooks sequentially from `00_` through `06_`.
3. Use the **DLT pipeline** to orchestrate data ingestion and transformation.
4. Execute model training and inference notebooks.
5. Optionally build a Genie app or Power BI dashboard on top!