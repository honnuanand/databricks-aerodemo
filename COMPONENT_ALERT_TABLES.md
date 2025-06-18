# Component-Specific Alert Tables

This document lists all the component-specific alert tables that have been created to avoid conflicts when multiple components run in parallel.

## Alert Table Naming Convention

All alert tables follow the pattern: `arao.aerodemo.{component}_alerts`

## Component Alert Tables

### 1. Aircraft-Level (Full Aircraft Twin)
- **Table**: `arao.aerodemo.aircraft_alerts`
- **Notebook**: `06_Model_Inference_To_Alerts_table.ipynb`
- **Description**: Overall aircraft health alerts based on full aircraft sensor data

### 2. Engine Component
- **Table**: `arao.aerodemo.engine_alerts`
- **Notebook**: `06_01_Engine_Model_Inference_To_Alerts.ipynb`
- **Description**: Engine-specific anomaly alerts

### 3. Landing Gear Component
- **Table**: `arao.aerodemo.landinggear_alerts`
- **Notebook**: `06_02_LandingGear_Model_Inference_To_Alerts.ipynb`
- **Description**: Landing gear-specific anomaly alerts

### 4. Avionics Component
- **Table**: `arao.aerodemo.avionics_alerts`
- **Notebook**: `06_03_Avionics_Model_Inference_To_Alerts.ipynb`
- **Description**: Avionics-specific anomaly alerts

### 5. Cabin Pressurization Component
- **Table**: `arao.aerodemo.cabinpressurization_alerts`
- **Notebook**: `06_04_CabinPressurization_Model_Inference_To_Alerts.ipynb`
- **Description**: Cabin pressurization-specific anomaly alerts

### 6. Airframe Component
- **Table**: `arao.aerodemo.airframe_alerts`
- **Notebook**: `06_05_Airframe_Model_Inference_To_Alerts.ipynb`
- **Description**: Airframe-specific anomaly alerts

### 7. Electrical Systems Component
- **Table**: `arao.aerodemo.electricalsystems_alerts`
- **Notebook**: `06_06_ElectricalSystems_Model_Inference_To_Alerts.ipynb`
- **Description**: Electrical systems-specific anomaly alerts

### 8. Fuel Systems Component
- **Table**: `arao.aerodemo.fuelsystems_alerts`
- **Notebook**: `06_07_FuelSystems_Model_Inference_To_Alerts.ipynb`
- **Description**: Fuel systems-specific anomaly alerts

### 9. Hydraulic Systems Component
- **Table**: `arao.aerodemo.hydraulicsystems_alerts`
- **Notebook**: `06_08_HydraulicSystems_Model_Inference_To_Alerts.ipynb`
- **Description**: Hydraulic systems-specific anomaly alerts

### 10. Environmental Systems Component
- **Table**: `arao.aerodemo.environmentalsystems_alerts`
- **Notebook**: `06_09_EnvironmentalSystems_Model_Inference_To_Alerts.ipynb`
- **Description**: Environmental systems-specific anomaly alerts

### 11. Auxiliary Systems Component
- **Table**: `arao.aerodemo.auxiliarysystems_alerts`
- **Notebook**: `06_10_AuxiliarySystems_Model_Inference_To_Alerts.ipynb`
- **Description**: Auxiliary systems-specific anomaly alerts

## Benefits of Component-Specific Tables

1. **Parallel Execution**: Multiple components can run simultaneously without table conflicts
2. **Component Isolation**: Each component's alerts are stored separately for better organization
3. **Independent Analysis**: Component-specific dashboards and monitoring can be created
4. **Data Governance**: Easier to manage access controls and data retention policies per component
5. **Troubleshooting**: Easier to debug issues specific to individual components

## Table Schema

All alert tables have the same schema:
- `timestamp`: When the alert was generated
- `alert_day`: Date of the alert (for partitioning)
- `aircraft_id`: Aircraft identifier
- `engine_rpm`, `capacity`, `range_km`: Aircraft metrics
- `predicted_anomaly`: Anomaly prediction (1 = anomaly detected)
- `batch_id`: Unique batch identifier for traceability

## Usage in Dashboards

Each component's alert table can be used to create:
- Component-specific health dashboards
- Real-time alert monitoring
- Historical trend analysis
- Predictive maintenance insights
- Compliance reporting 