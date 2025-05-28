
# Databricks AeroDemo 🚀✈️

This project demonstrates an end-to-end digital twin system for aircraft, integrating streaming sensor data, Delta Live Tables (DLT), Feature Store, and machine learning models, all running on Databricks.

---

## 📚 Updated Notebook Workflow

| Notebook Name                                   | Purpose |
|------------------------------------------------|---------|
| `00_Overview_and_Instructions.md`              | 🧭 Describes the workflow and purpose of each notebook |
| `01_Table_Creation.ipynb`                      | 🏗️ Creates all required Delta tables in Unity Catalog |
| `02_Synthetic_Data_Generation.ipynb`           | 🧪 Generates synthetic CSVs for sensor and maintenance data |
| `02_01_Synthetic_Engine_Component_Data_Generation.ipynb` | 🔧 Generates synthetic engine-level component sensor data |
| `02_02_Synthetic_LandingGear_Component_Data_Generation.ipynb` | 🔧 Generates synthetic landing gear component data |
| `02_03_Synthetic_Avionics_Component_Data_Generation.ipynb` | 🔧 Generates synthetic avionics component data |
| `02_04_Synthetic_CabinPressurization_Component_Data_Generation.ipynb` | 🔧 Generates synthetic cabin pressurization component data |
| `02_05_Synthetic_Airframe_Component_Data_Generation.ipynb` | 🔧 Generates synthetic airframe component data |
| `03_DLT_Pipeline_Full.py`                      | 🔄 Delta Live Tables pipeline: ingest → clean → enrich → feature engineer → predict |
| `03A_Feature_Store_Registration.py`            | 🧠 Registers engineered aircraft-level features into Feature Store |
| `03B_Component_Feature_Store_Registration.py`  | 🧠 Registers component-level feature tables into Feature Store |
| `04_Model_Training_And_Registration.ipynb`     | 🎯 Trains and registers ML model using sensor features |
| `05_Model_Inference.ipynb`                     | 📈 Loads model by version or alias and runs inference |
| `06_High_Risk_Alert_Generation.ipynb`          | ⚠️ Writes high-risk predictions to `anomaly_alerts` table |

---

## 🔩 Component-Level Digital Twin Expansion

We extended the digital twin architecture to cover **component-level twins** for:  
✅ Engine  
✅ Landing Gear  
✅ Avionics  
✅ Cabin Pressurization  
✅ Airframe  

For each component, the pipeline:  
- Ingests streaming sensor data  
- Classifies health status (Nominal, Warning, Critical)  
- Builds rolling and lag features in materialized Delta tables  
- Prepares tables for model input and registration into the Feature Store  

---

## 🧠 Feature Engineering (Component-Level Highlights)

| Component         | Feature Tables                      | Key Engineered Features |
|------------------|-------------------------------------|--------------------------|
| Engine           | `component_features_engine`         | Prev temp, avg temp 7d, avg vibration 7d |
| Landing Gear     | `component_features_landing_gear`   | Prev brake wear, avg brake temp 7d |
| Avionics         | `component_features_avionics`       | Prev signal integrity, avg system temp 7d |
| Cabin Pressurization | `component_features_cabin`      | Prev airflow, avg seal integrity 7d |
| Airframe         | `component_features_airframe`       | Prev stress points, avg crack growth 7d |

These are registered into the **Databricks Feature Store** for governance, reproducibility, and reuse.

---

## 🌐 Updated Workflow Diagram

```mermaid
flowchart TD
    A[ Synthetic Data Generation (02 Series) ] --> B[ Delta Live Tables (DLT) Pipeline ]
    B --> C1[ Cleaned Sensor Data ]
    B --> C2[ Component Twin Streams (Engine, Gear, Avionics, Cabin, Airframe) ]
    C1 --> D1[ Sensor Feature Engineering ]
    C2 --> D2[ Component Health Status ]
    D2 --> D3[ Component Feature Engineering Tables ]
    D1 --> E1[ Aircraft-Level ML Model ]
    D3 --> E2[ Component-Level ML Models ]
    E1 --> F1[ Anomaly Predictions + Alerts ]
    E2 --> F2[ Component Risk Predictions + Alerts ]
    F1 --> G1[ Aircraft Dashboard ]
    F2 --> G2[ Component Dashboard ]
    G1 --> H[ Databricks Feature Store ]
    G2 --> H
```

---

## 📦 Coming Soon (Roadmap Update)

- ✅ What-if simulations using historical and component context  
- ✅ Component-level ML models with their own risk predictions  
- ✅ Extended dashboards for fleet-level and component-level views  
- ✅ Real-time alerts and integrations with maintenance systems  

---

For details, see the notebooks inside the `/notebooks` folder or open each from Databricks Workspace.

