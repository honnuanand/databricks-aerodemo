# Unified Alerts DLT Pipeline

## Overview

The Unified Alerts DLT Pipeline (`03_04_Unified_Alerts_DLT_Pipeline.py`) creates a comprehensive unified view that combines all component-specific alert tables into a single, queryable dataset for easy visualization and dashboarding.

## Purpose

- **Merge all 11 alert tables** (aircraft + 10 components) into one unified view
- **Include alert metadata** such as alert date, aircraft ID, component name, and component ID
- **Enable easy filtering and visualization** across all components
- **Provide comprehensive dashboard views** for real-time monitoring
- **Create summary statistics** for trend analysis

## Input Tables

| Component | Alert Table | Component ID | Description |
|-----------|-------------|--------------|-------------|
| **Aircraft (Full)** | `arao.aerodemo.aircraft_alerts` | `AC` | Full Aircraft Health |
| **Engine** | `arao.aerodemo.engine_alerts` | `ENG` | Engine System |
| **Landing Gear** | `arao.aerodemo.landinggear_alerts` | `LG` | Landing Gear System |
| **Avionics** | `arao.aerodemo.avionics_alerts` | `AV` | Avionics System |
| **Cabin Pressurization** | `arao.aerodemo.cabinpressurization_alerts` | `CP` | Cabin Pressurization System |
| **Airframe** | `arao.aerodemo.airframe_alerts` | `AF` | Airframe Structure |
| **Electrical Systems** | `arao.aerodemo.electricalsystems_alerts` | `ES` | Electrical Systems |
| **Fuel Systems** | `arao.aerodemo.fuelsystems_alerts` | `FS` | Fuel Systems |
| **Hydraulic Systems** | `arao.aerodemo.hydraulicsystems_alerts` | `HS` | Hydraulic Systems |
| **Environmental Systems** | `arao.aerodemo.environmentalsystems_alerts` | `ENV` | Environmental Systems |
| **Auxiliary Systems** | `arao.aerodemo.auxiliarysystems_alerts` | `AUX` | Auxiliary Systems |

## Output Tables & Views

### 1. `unified_alerts_view` (Table)
**Primary unified alerts table with all component alerts**

**Schema:**
- `timestamp` - When the alert was generated
- `aircraft_id` - Aircraft identifier
- `predicted_anomaly` - Anomaly prediction (1 = anomaly detected)
- `alert_day` - Date of the alert (for partitioning)
- `alert_date` - Computed date from timestamp
- `alert_hour` - Hour of the alert
- `alert_severity` - HIGH/LOW based on predicted_anomaly
- `alert_category` - CRITICAL/SAFETY/OPERATIONAL based on component
- `component_name` - Human-readable component name
- `component_id` - Short component identifier
- `component_description` - Detailed component description
- `alert_source` - Source table name
- `processed_at` - When this record was processed

**Partitioning:** `alert_date`, `component_id`

### 2. `alert_summary` (Table)
**Summary statistics for dashboarding**

**Schema:**
- `component_name` - Component name
- `component_id` - Component identifier
- `alert_category` - Alert category
- `alert_severity` - Alert severity
- `alert_date` - Date of alerts
- `alert_count` - Number of alerts
- `affected_aircraft_count` - Number of unique aircraft affected

**Partitioning:** `alert_date`

### 3. `alert_dashboard_view` (Table)
**Real-time dashboard view with latest alert status**

**Schema:** Same as unified_alerts_view but with latest alert per aircraft-component combination

### 4. `alert_trends` (Table)
**Alert trends analysis over time**

**Schema:**
- `alert_date` - Date of alerts
- `component_id` - Component identifier
- `alert_category` - Alert category
- `alert_severity` - Alert severity
- `daily_alert_count` - Daily alert count
- `daily_affected_aircraft` - Daily affected aircraft count
- `avg_anomaly_score` - Average anomaly score

**Partitioning:** `alert_date`

### 5. `unified_alerts_sql_view` (View)
**SQL view for easy querying and dashboard integration**

**Schema:** Same as unified_alerts_view but optimized for SQL queries

## Alert Categories

The pipeline automatically categorizes alerts based on component criticality:

- **CRITICAL**: Engine (ENG), Landing Gear (LG), Airframe (AF)
- **SAFETY**: Avionics (AV), Cabin Pressurization (CP), Electrical Systems (ES)
- **OPERATIONAL**: All other components

## Alert Severity

- **HIGH**: When `predicted_anomaly = 1`
- **LOW**: When `predicted_anomaly = 0`

## Usage Examples

### SQL Queries

```sql
-- Get all critical alerts from today
SELECT * FROM arao.aerodemo.unified_alerts_sql_view 
WHERE alert_category = 'CRITICAL' 
AND alert_date = CURRENT_DATE();

-- Get alert summary by component
SELECT component_name, COUNT(*) as alert_count 
FROM arao.aerodemo.unified_alerts_sql_view 
WHERE alert_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY component_name 
ORDER BY alert_count DESC;

-- Get latest alert status for each aircraft
SELECT * FROM arao.aerodemo.alert_dashboard_view 
WHERE alert_severity = 'HIGH';
```

### Dashboard Integration

The unified alerts view can be easily integrated with:
- **Databricks Lakeview Dashboards**
- **Power BI**
- **Tableau**
- **Custom web applications**

## Benefits

1. **Unified Data Source**: Single source of truth for all alerts
2. **Easy Filtering**: Filter by component, severity, category, date
3. **Real-time Updates**: Automatically updates as new alerts are generated
4. **Performance Optimized**: Partitioned and optimized for fast queries
5. **Dashboard Ready**: Pre-computed summaries and trends
6. **Scalable**: Handles large volumes of alert data efficiently

## Integration with Workflow

This DLT pipeline should be added to the workflow after all component alert generation tasks complete, ensuring all alert tables are populated before creating the unified view.

## Monitoring

The pipeline includes error handling to continue processing even if individual component alert tables are unavailable, ensuring robustness in production environments. 