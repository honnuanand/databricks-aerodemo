# Alert Inference Traceability Guide

## Overview

The enhanced Unified Alerts DLT Pipeline now includes full traceability from alerts back to their source inference data. This allows you to double-click on any alert and drill down to see the original inference that generated it.

## 🔍 **How It Works**

### **Enhanced Schema**
Each alert now includes additional traceability fields:

- `inference_source` - The source inference table name
- `inference_trace_id` - Unique identifier linking alert to inference
- `batch_id` - Batch identifier from the original inference
- `alert_source` - The source alert table name

### **New DLT Objects**

1. **`inference_traceability`** (Table) - Links alerts to source inference records
2. **`alert_to_inference_drilldown`** (View) - Drill-down view for easy navigation
3. **Enhanced `unified_alerts_sql_view`** - Now includes traceability fields

## 📊 **Usage Examples**

### **1. View All Alerts with Traceability**

```sql
-- Get all alerts with inference traceability information
SELECT 
    timestamp,
    aircraft_id,
    component_name,
    alert_severity,
    alert_category,
    inference_source,
    inference_trace_id,
    batch_id
FROM arao.aerodemo.unified_alerts_sql_view
WHERE alert_date = CURRENT_DATE()
ORDER BY timestamp DESC;
```

### **2. Drill Down from Alert to Source Inference**

```sql
-- Get the source inference data for a specific alert
SELECT 
    t.aircraft_id,
    t.timestamp,
    t.component_name,
    t.component_id,
    t.inference_table,
    t.predicted_anomaly,
    t.alert_severity,
    t.alert_category,
    t.inference_trace_id
FROM arao.aerodemo.alert_to_inference_drilldown t
WHERE t.inference_trace_id = 'AC123_ENG_20241218_143022'
```

### **3. Find All Alerts for a Specific Aircraft with Inference Details**

```sql
-- Get all alerts for aircraft AC123 with inference traceability
SELECT 
    u.timestamp,
    u.aircraft_id,
    u.component_name,
    u.alert_severity,
    u.alert_category,
    u.inference_source,
    u.inference_trace_id,
    u.batch_id
FROM arao.aerodemo.unified_alerts_sql_view u
WHERE u.aircraft_id = 'AC123'
  AND u.alert_date >= DATE_SUB(CURRENT_DATE(), 7)
ORDER BY u.timestamp DESC;
```

### **4. Get Source Inference Data for Critical Alerts**

```sql
-- Get source inference data for all critical alerts
SELECT 
    t.aircraft_id,
    t.timestamp,
    t.component_name,
    t.alert_severity,
    t.inference_table,
    t.inference_trace_id
FROM arao.aerodemo.alert_to_inference_drilldown t
WHERE t.alert_category = 'CRITICAL'
  AND t.alert_date = CURRENT_DATE()
ORDER BY t.timestamp DESC;
```

### **5. Trace Alert Back to Original Inference Table**

```sql
-- For a specific alert, get the full inference record
-- Replace 'AC123_ENG_20241218_143022' with your actual inference_trace_id

-- Step 1: Get the inference table name
SELECT inference_source 
FROM arao.aerodemo.unified_alerts_sql_view 
WHERE inference_trace_id = 'AC123_ENG_20241218_143022';

-- Step 2: Query the source inference table (example for engine)
SELECT * 
FROM arao.aerodemo.prediction_results_engine 
WHERE aircraft_id = 'AC123' 
  AND timestamp = '2024-12-18 14:30:22';
```

## 🎯 **Dashboard Integration**

### **Drill-Down Workflow**

1. **Alert Dashboard**: Show alerts in a table/grid
2. **Click Alert**: User clicks on any alert row
3. **Get Trace ID**: Extract `inference_trace_id` from the alert
4. **Query Source**: Use the trace ID to query source inference data
5. **Display Details**: Show the original inference record with all features

### **Example Dashboard Query**

```sql
-- Dashboard drill-down query
WITH alert_details AS (
    SELECT 
        inference_trace_id,
        inference_source,
        aircraft_id,
        timestamp
    FROM arao.aerodemo.unified_alerts_sql_view
    WHERE inference_trace_id = '${selected_trace_id}'
)
SELECT 
    ad.inference_trace_id,
    ad.inference_source,
    -- Add your specific inference table columns here
    -- This will vary based on the component
    i.*
FROM alert_details ad
JOIN ${ad.inference_source} i 
    ON i.aircraft_id = ad.aircraft_id 
    AND i.timestamp = ad.timestamp;
```

## 🔧 **Component-Specific Inference Tables**

Each component has its own inference table:

| Component | Alert Table | Inference Table |
|-----------|-------------|-----------------|
| **Aircraft** | `aircraft_alerts` | `prediction_results` |
| **Engine** | `engine_alerts` | `prediction_results_engine` |
| **Landing Gear** | `landinggear_alerts` | `prediction_results_landinggear` |
| **Avionics** | `avionics_alerts` | `prediction_results_avionics` |
| **Cabin Pressurization** | `cabinpressurization_alerts` | `prediction_results_cabinpressurization` |
| **Airframe** | `airframe_alerts` | `prediction_results_airframe` |
| **Electrical Systems** | `electricalsystems_alerts` | `prediction_results_electricalsystems` |
| **Fuel Systems** | `fuelsystems_alerts` | `prediction_results_fuelsystems` |
| **Hydraulic Systems** | `hydraulicsystems_alerts` | `prediction_results_hydraulicsystems` |
| **Environmental Systems** | `environmentalsystems_alerts` | `prediction_results_environmentalsystems` |
| **Auxiliary Systems** | `auxiliarysystems_alerts` | `prediction_results_auxiliarysystems` |

## 📋 **Traceability Fields Explained**

### **`inference_trace_id`**
- **Format**: `{aircraft_id}_{component_id}_{timestamp}`
- **Example**: `AC123_ENG_20241218_143022`
- **Purpose**: Unique identifier linking alert to source inference

### **`inference_source`**
- **Value**: Full table name (e.g., `arao.aerodemo.prediction_results_engine`)
- **Purpose**: Direct reference to the source inference table

### **`batch_id`**
- **Value**: Original batch identifier from inference
- **Purpose**: Links to the specific inference batch that generated the alert

## 🚀 **Benefits**

1. **Full Traceability**: Every alert can be traced back to its source inference
2. **Easy Drill-Down**: Simple queries to get source data
3. **Dashboard Ready**: Perfect for interactive dashboards
4. **Audit Trail**: Complete audit trail from inference to alert
5. **Troubleshooting**: Easy to investigate why an alert was generated

## 💡 **Best Practices**

1. **Use `inference_trace_id`** as the primary key for drill-down operations
2. **Cache inference table names** in your application for performance
3. **Implement error handling** for cases where inference tables don't exist
4. **Use the drill-down view** (`alert_to_inference_drilldown`) for simplified queries
5. **Consider partitioning** by date for better performance on large datasets

This enhanced traceability system provides complete visibility from alerts back to the original inference data, enabling deep analysis and investigation capabilities! 🎉 