# 🛫 Aircraft Fleet Digital Twin Dashboard - React App Design

## 🎯 Overview

An interactive React application that provides real-time monitoring and drill-down analysis of aircraft fleet health using the Databricks digital twin system. The app enables users to navigate from fleet-level overviews down to individual component alerts and their underlying inference data.

---

## 🏗️ Application Architecture

### Tech Stack
- **Frontend**: React 18 with TypeScript
- **State Management**: Redux Toolkit + RTK Query
- **UI Framework**: Material-UI (MUI) v5
- **Charts**: Recharts for data visualization
- **Maps**: React-Leaflet for fleet location mapping
- **3D Visualization**: Three.js for aircraft component highlighting
- **API Integration**: Databricks REST API + Unity Catalog SQL endpoints
- **Real-time Updates**: WebSocket connections for live data

### Project Structure
```
aircraft-fleet-dashboard/
├── src/
│   ├── components/
│   │   ├── Fleet/
│   │   ├── Aircraft/
│   │   ├── Components/
│   │   ├── Alerts/
│   │   ├── Analytics/
│   │   └── Common/
│   ├── pages/
│   ├── services/
│   ├── store/
│   ├── types/
│   ├── utils/
│   └── assets/
│       └── aircraft-models/
├── public/
└── package.json
```

---

## 🚀 Core Features

### 1. Fleet-Level Dashboard

#### Fleet Overview Section
- **Fleet Health Score**: Overall fleet health percentage with color-coded status
- **Aircraft Count**: Total aircraft by health status (Healthy, Warning, Critical)
- **Alert Summary**: Total active alerts by severity and component type
- **Fleet Distribution**: Interactive map showing aircraft locations

#### Fleet Analytics
- **Health Trends**: Time-series chart of fleet health over time
- **Alert Patterns**: Heatmap of alerts by aircraft model and component
- **Maintenance Schedule**: Upcoming maintenance events across the fleet
- **Performance Metrics**: Key performance indicators (KPIs)

#### Filtering & Drill-Down Options
- **Aircraft Make/Model**: Filter by manufacturer and model
- **Geographic Region**: Filter by operational regions
- **Health Status**: Filter by overall aircraft health
- **Component Type**: Filter by specific component alerts
- **Date Range**: Historical analysis capabilities

### 2. Aircraft Detail View

#### Aircraft Information Panel
- **Aircraft Image**: High-quality 3D model or photograph of the specific aircraft model
- **Basic Info**: Aircraft ID, registration, model, manufacturer, age
- **Current Status**: Overall health score, last maintenance date, next scheduled maintenance
- **Operational Data**: Current location, flight hours, cycles

#### Interactive Aircraft Visualization
- **3D Aircraft Model**: Interactive 3D representation of the aircraft
- **Component Highlighting**: Color-coded components based on alert status
  - 🟢 Green: Healthy
  - 🟡 Yellow: Warning alerts
  - 🔴 Red: Critical alerts
- **Hover Interactions**: Detailed component information on hover
- **Click Navigation**: Drill-down to component detail views

#### Component Health Overview
- **Health Matrix**: Grid showing all 10 components with health status
- **Alert Summary**: Count of active alerts per component
- **Trend Indicators**: Health trend arrows (improving/declining)
- **Quick Actions**: Direct links to component detail views

### 3. Component Detail View

#### Component Information
- **Component Overview**: Name, type, manufacturer, installation date
- **Current Health**: Health score, status, last assessment
- **Alert History**: Timeline of recent alerts and resolutions

#### Alert Analysis
- **Active Alerts**: List of current alerts with severity and description
- **Alert Details**: Full alert information including:
  - Alert ID and timestamp
  - Severity level and description
  - Affected parameters
  - Recommended actions

#### Inference Drill-Down
- **Inference Trace**: Direct link to the inference that generated the alert
- **Model Information**: Model version, training date, performance metrics
- **Feature Values**: Actual sensor values that triggered the alert
- **Prediction Confidence**: Model confidence scores and thresholds

---

## 🎨 User Interface Design

### Design System
- **Color Palette**: Aviation-inspired colors with clear status indicators
- **Typography**: Clean, readable fonts optimized for dashboard viewing
- **Icons**: Consistent icon set for aircraft components and status indicators
- **Responsive Design**: Works on desktop, tablet, and mobile devices

### Navigation Flow
```
Fleet Dashboard
    ↓
Aircraft List (Filtered)
    ↓
Individual Aircraft View
    ↓
Component Detail
    ↓
Alert Analysis
    ↓
Inference Drill-Down
```

### Interactive Elements
- **Hover Effects**: Component highlighting and tooltips
- **Click Actions**: Drill-down navigation and detail expansion
- **Drag & Drop**: Customizable dashboard layouts
- **Real-time Updates**: Live data refresh with visual indicators

---

## 📊 Data Integration

### Databricks API Endpoints & Required Tables

#### 1. Fleet-Level Dashboard Queries

##### Fleet Health Summary
```sql
-- Overall fleet health metrics
SELECT 
  COUNT(DISTINCT aircraft_id) as total_aircraft,
  SUM(CASE WHEN health_score >= 80 THEN 1 ELSE 0 END) as healthy_count,
  SUM(CASE WHEN health_score < 80 AND health_score >= 60 THEN 1 ELSE 0 END) as warning_count,
  SUM(CASE WHEN health_score < 60 THEN 1 ELSE 0 END) as critical_count,
  AVG(health_score) as avg_fleet_health,
  SUM(alert_count) as total_active_alerts
FROM arao.aerodemo.unified_component_health_view
WHERE date = CURRENT_DATE();

-- Fleet health by aircraft model
SELECT 
  aircraft_model,
  manufacturer,
  COUNT(DISTINCT aircraft_id) as aircraft_count,
  AVG(health_score) as avg_health_score,
  SUM(alert_count) as total_alerts
FROM arao.aerodemo.unified_component_health_view
WHERE date = CURRENT_DATE()
GROUP BY aircraft_model, manufacturer
ORDER BY avg_health_score ASC;

-- Fleet health trends (last 30 days)
SELECT 
  date,
  COUNT(DISTINCT aircraft_id) as active_aircraft,
  AVG(health_score) as avg_health_score,
  SUM(alert_count) as total_alerts
FROM arao.aerodemo.unified_component_health_view
WHERE date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY date
ORDER BY date;
```

##### Alert Summary by Component
```sql
-- Alert distribution by component and severity
SELECT 
  component_name,
  severity_level,
  COUNT(*) as alert_count,
  AVG(severity_score) as avg_severity_score
FROM arao.aerodemo.unified_alerts_view
WHERE alert_date >= DATE_SUB(CURRENT_DATE(), 7)
  AND status = 'ACTIVE'
GROUP BY component_name, severity_level
ORDER BY component_name, severity_level;

-- Alert trends by component (last 30 days)
SELECT 
  DATE(alert_date) as date,
  component_name,
  COUNT(*) as daily_alerts,
  AVG(severity_score) as avg_severity
FROM arao.aerodemo.unified_alerts_view
WHERE alert_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY DATE(alert_date), component_name
ORDER BY date DESC, component_name;
```

##### Geographic Fleet Distribution
```sql
-- Aircraft locations and health status
SELECT 
  aircraft_id,
  aircraft_model,
  manufacturer,
  current_location,
  latitude,
  longitude,
  health_score,
  alert_count,
  last_maintenance_date,
  next_maintenance_date
FROM arao.aerodemo.unified_component_health_view
WHERE date = CURRENT_DATE()
ORDER BY health_score ASC;
```

#### 2. Aircraft Detail View Queries

##### Aircraft Basic Information
```sql
-- Aircraft details and current status
SELECT 
  aircraft_id,
  aircraft_registration,
  aircraft_model,
  manufacturer,
  year_manufactured,
  total_flight_hours,
  total_cycles,
  current_location,
  health_score,
  last_maintenance_date,
  next_maintenance_date,
  maintenance_status
FROM arao.aerodemo.unified_component_health_view
WHERE aircraft_id = 'AIRCRAFT_001'
  AND date = CURRENT_DATE()
LIMIT 1;
```

##### Aircraft Component Health Matrix
```sql
-- All components health status for specific aircraft
SELECT 
  component_name,
  component_id,
  health_score,
  health_status,
  alert_count,
  last_assessment_date,
  next_assessment_date,
  component_manufacturer,
  installation_date,
  warranty_expiry_date
FROM arao.aerodemo.unified_component_health_view
WHERE aircraft_id = 'AIRCRAFT_001'
  AND date = CURRENT_DATE()
ORDER BY health_score ASC;
```

##### Aircraft Alert Summary
```sql
-- Active alerts for specific aircraft
SELECT 
  alert_id,
  component_name,
  alert_date,
  severity_level,
  severity_score,
  description,
  affected_parameters,
  recommended_actions,
  status,
  inference_trace_id
FROM arao.aerodemo.unified_alerts_view
WHERE aircraft_id = 'AIRCRAFT_001'
  AND status = 'ACTIVE'
ORDER BY alert_date DESC, severity_score DESC;
```

#### 3. Component Detail View Queries

##### Component Information
```sql
-- Detailed component information
SELECT 
  aircraft_id,
  component_name,
  component_id,
  component_type,
  manufacturer,
  model_number,
  serial_number,
  installation_date,
  warranty_expiry_date,
  expected_lifetime_hours,
  current_operating_hours,
  health_score,
  health_status,
  last_maintenance_date,
  next_maintenance_date
FROM arao.aerodemo.unified_component_health_view
WHERE aircraft_id = 'AIRCRAFT_001'
  AND component_name = 'Engine'
  AND date = CURRENT_DATE();
```

##### Component Alert History
```sql
-- Alert history for specific component
SELECT 
  alert_id,
  alert_date,
  severity_level,
  severity_score,
  description,
  affected_parameters,
  recommended_actions,
  status,
  resolution_date,
  resolution_notes,
  inference_trace_id
FROM arao.aerodemo.unified_alerts_view
WHERE aircraft_id = 'AIRCRAFT_001'
  AND component_name = 'Engine'
ORDER BY alert_date DESC;
```

##### Component Health Trends
```sql
-- Health score trends over time
SELECT 
  date,
  health_score,
  alert_count,
  maintenance_events
FROM arao.aerodemo.unified_component_health_view
WHERE aircraft_id = 'AIRCRAFT_001'
  AND component_name = 'Engine'
  AND date >= DATE_SUB(CURRENT_DATE(), 90)
ORDER BY date;
```

#### 4. Alert Analysis & Inference Drill-Down Queries

##### Alert Details with Inference Trace
```sql
-- Complete alert information with inference details
SELECT 
  a.alert_id,
  a.aircraft_id,
  a.component_name,
  a.alert_date,
  a.severity_level,
  a.severity_score,
  a.description,
  a.affected_parameters,
  a.recommended_actions,
  a.status,
  a.inference_trace_id,
  i.model_name,
  i.model_version,
  i.prediction_confidence,
  i.prediction_threshold,
  i.feature_values,
  i.training_date,
  i.model_performance_metrics
FROM arao.aerodemo.unified_alerts_view a
JOIN arao.aerodemo.inference_traceability_view i 
  ON a.inference_trace_id = i.trace_id
WHERE a.alert_id = 'ALERT_001';
```

##### Inference Trace Details
```sql
-- Detailed inference information
SELECT 
  trace_id,
  aircraft_id,
  component_name,
  inference_date,
  model_name,
  model_version,
  prediction_confidence,
  prediction_threshold,
  actual_prediction,
  feature_values,
  training_date,
  model_performance_metrics,
  feature_importance_scores
FROM arao.aerodemo.inference_traceability_view
WHERE trace_id = 'TRACE_001';
```

##### Model Performance Metrics
```sql
-- Model performance for specific component
SELECT 
  model_name,
  model_version,
  training_date,
  accuracy_score,
  precision_score,
  recall_score,
  f1_score,
  auc_score,
  feature_count,
  training_samples
FROM arao.aerodemo.model_registry_view
WHERE component_name = 'Engine'
ORDER BY training_date DESC;
```

#### 5. Advanced Analytics Queries

##### Maintenance Schedule
```sql
-- Upcoming maintenance events
SELECT 
  aircraft_id,
  component_name,
  maintenance_type,
  scheduled_date,
  estimated_duration_hours,
  required_parts,
  estimated_cost,
  priority_level
FROM arao.aerodemo.maintenance_schedule_view
WHERE scheduled_date >= CURRENT_DATE()
  AND scheduled_date <= DATE_ADD(CURRENT_DATE(), 30)
ORDER BY scheduled_date, priority_level;
```

##### Performance Benchmarking
```sql
-- Compare aircraft performance across fleet
SELECT 
  aircraft_model,
  manufacturer,
  AVG(health_score) as avg_health_score,
  AVG(alert_count) as avg_alert_count,
  AVG(total_flight_hours) as avg_flight_hours,
  COUNT(DISTINCT aircraft_id) as aircraft_count
FROM arao.aerodemo.unified_component_health_view
WHERE date = CURRENT_DATE()
GROUP BY aircraft_model, manufacturer
ORDER BY avg_health_score DESC;
```

##### Cost Analysis
```sql
-- Maintenance cost analysis
SELECT 
  component_name,
  COUNT(*) as maintenance_events,
  SUM(actual_cost) as total_cost,
  AVG(actual_cost) as avg_cost_per_event,
  SUM(downtime_hours) as total_downtime
FROM arao.aerodemo.maintenance_history_view
WHERE maintenance_date >= DATE_SUB(CURRENT_DATE(), 365)
GROUP BY component_name
ORDER BY total_cost DESC;
```

### Required Table Schema Extensions

To fully support the dashboard, the following additional columns may be needed in existing tables:

#### Enhanced Aircraft Metadata
```sql
-- Additional columns for aircraft_info table
ALTER TABLE arao.aerodemo.aircraft_info ADD COLUMNS (
  current_location STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  operational_status STRING,
  fleet_assignment STRING,
  base_airport STRING,
  last_flight_date TIMESTAMP,
  next_flight_date TIMESTAMP
);
```

#### Enhanced Component Information
```sql
-- Additional columns for component_health table
ALTER TABLE arao.aerodemo.component_health ADD COLUMNS (
  component_manufacturer STRING,
  model_number STRING,
  serial_number STRING,
  installation_date DATE,
  warranty_expiry_date DATE,
  expected_lifetime_hours INT,
  current_operating_hours INT,
  maintenance_status STRING
);
```

#### Enhanced Alert Information
```sql
-- Additional columns for alerts table
ALTER TABLE arao.aerodemo.unified_alerts_view ADD COLUMNS (
  affected_parameters STRING,
  recommended_actions STRING,
  resolution_date TIMESTAMP,
  resolution_notes STRING,
  assigned_technician STRING,
  estimated_resolution_time_hours INT
);
```

### Real-time Data Updates

#### WebSocket Event Schema
```json
{
  "event_type": "alert_generated|health_update|maintenance_completed",
  "aircraft_id": "AIRCRAFT_001",
  "component_name": "Engine",
  "timestamp": "2024-01-15T10:30:00Z",
  "data": {
    "health_score": 75.5,
    "alert_count": 2,
    "severity_level": "WARNING"
  }
}
```

#### Polling Endpoints
- **Fleet Health**: `/api/fleet/health` - Updated every 30 seconds
- **Active Alerts**: `/api/alerts/active` - Updated every 15 seconds  
- **Aircraft Status**: `/api/aircraft/{id}/status` - Updated every 60 seconds
- **Component Health**: `/api/aircraft/{id}/components` - Updated every 120 seconds

### Caching Strategy

#### Cache Keys
- `fleet:health:summary` - TTL: 30 seconds
- `fleet:alerts:active` - TTL: 15 seconds
- `aircraft:{id}:status` - TTL: 60 seconds
- `aircraft:{id}:components` - TTL: 120 seconds
- `component:{id}:alerts` - TTL: 30 seconds
- `inference:{trace_id}` - TTL: 300 seconds

#### Cache Invalidation
- Alert generation triggers invalidation of related caches
- Health score updates invalidate fleet and aircraft caches
- Maintenance events invalidate component and aircraft caches

---

## 🔧 Component Implementation

### 1. FleetDashboard Component
```typescript
interface FleetDashboardProps {
  filters: FleetFilters;
  onAircraftSelect: (aircraftId: string) => void;
}

const FleetDashboard: React.FC<FleetDashboardProps> = ({ filters, onAircraftSelect }) => {
  // Fleet health metrics
  // Interactive map
  // Filter controls
  // Aircraft list/grid
}
```

### 2. AircraftView Component
```typescript
interface AircraftViewProps {
  aircraftId: string;
  onComponentSelect: (componentName: string) => void;
}

const AircraftView: React.FC<AircraftViewProps> = ({ aircraftId, onComponentSelect }) => {
  // 3D aircraft model
  // Component highlighting
  // Health overview
  // Alert summary
}
```

### 3. ComponentDetail Component
```typescript
interface ComponentDetailProps {
  aircraftId: string;
  componentName: string;
  onAlertSelect: (alertId: string) => void;
}

const ComponentDetail: React.FC<ComponentDetailProps> = ({ 
  aircraftId, 
  componentName, 
  onAlertSelect 
}) => {
  // Component information
  // Alert history
  // Health trends
  // Maintenance schedule
}
```

### 4. AlertAnalysis Component
```typescript
interface AlertAnalysisProps {
  alertId: string;
  onInferenceDrillDown: (traceId: string) => void;
}

const AlertAnalysis: React.FC<AlertAnalysisProps> = ({ alertId, onInferenceDrillDown }) => {
  // Alert details
  // Inference trace
  // Model information
  // Feature values
}
```

---

## 🎯 Key User Interactions

### Fleet-Level Interactions
1. **Filter Selection**: User selects aircraft make/model, region, or health status
2. **Map Interaction**: Click on aircraft markers to view details
3. **Chart Drill-Down**: Click on chart elements to filter aircraft list
4. **Alert Summary**: Click on alert counts to view affected aircraft

### Aircraft-Level Interactions
1. **Component Hover**: Hover over highlighted components to see alert preview
2. **Component Click**: Click on components to view detailed analysis
3. **3D Model Rotation**: Rotate aircraft model to view different angles
4. **Health Score Click**: Click on health score to see contributing factors

### Component-Level Interactions
1. **Alert Selection**: Click on specific alerts to view full details
2. **Inference Trace**: Click "View Inference" to drill down to model data
3. **Historical View**: Toggle between current and historical data
4. **Maintenance Link**: Click to view related maintenance records

---

## 📱 Responsive Design

### Desktop Experience
- **Multi-panel Layout**: Sidebar navigation with main content area
- **Large 3D Models**: Full-size aircraft visualizations
- **Detailed Charts**: Comprehensive analytics with multiple metrics
- **Keyboard Shortcuts**: Power user navigation options

### Tablet Experience
- **Adaptive Layout**: Responsive grid that adjusts to screen size
- **Touch-Optimized**: Larger touch targets for component selection
- **Simplified 3D**: Optimized aircraft models for touch interaction
- **Collapsible Panels**: Space-efficient information display

### Mobile Experience
- **Single-Panel Flow**: Sequential navigation through drill-down levels
- **Gesture Support**: Swipe navigation between views
- **Essential Data**: Prioritized information display
- **Offline Capability**: Basic functionality without internet

---

## 🔒 Security & Performance

### Security Features
- **Authentication**: SSO integration with enterprise systems
- **Authorization**: Role-based access to aircraft and component data
- **Data Encryption**: Secure transmission of sensitive aircraft data
- **Audit Logging**: Track user interactions and data access

### Performance Optimizations
- **Lazy Loading**: Load aircraft models and data on demand
- **Image Optimization**: Compressed aircraft images and 3D models
- **Caching Strategy**: Intelligent caching of frequently accessed data
- **CDN Integration**: Global content delivery for fast loading

---

## 🚀 Deployment Strategy

### Development Environment
- **Local Development**: Hot reload with mock data
- **Staging Environment**: Full integration with test Databricks instance
- **Feature Branches**: Isolated development for new features

### Production Deployment
- **Containerization**: Docker containers for consistent deployment
- **CI/CD Pipeline**: Automated testing and deployment
- **Monitoring**: Application performance monitoring and error tracking
- **Backup Strategy**: Regular backups of user configurations

---

## 📈 Future Enhancements

### Advanced Analytics
- **Predictive Insights**: AI-powered maintenance recommendations
- **Anomaly Detection**: Advanced pattern recognition for early warnings
- **Performance Benchmarking**: Compare aircraft performance across fleet
- **Cost Analysis**: Maintenance cost optimization recommendations

### Enhanced Visualization
- **AR/VR Support**: Augmented reality for field technicians
- **Real-time Video**: Live camera feeds from aircraft systems
- **Weather Integration**: Weather impact on aircraft performance
- **Flight Path Tracking**: Real-time flight path visualization

### Integration Capabilities
- **Maintenance Systems**: Integration with existing maintenance software
- **Parts Inventory**: Real-time parts availability and ordering
- **Crew Management**: Pilot and crew assignment integration
- **Regulatory Compliance**: Automated compliance reporting

---

## 🎯 Success Metrics

### User Experience
- **Time to Insight**: Reduce time from alert to resolution
- **User Adoption**: Increase in daily active users
- **Task Completion**: Success rate of drill-down workflows
- **User Satisfaction**: Net Promoter Score (NPS) improvements

### Business Impact
- **Maintenance Efficiency**: Reduced unplanned maintenance events
- **Cost Savings**: Decreased maintenance costs through predictive insights
- **Safety Improvements**: Reduced safety incidents through early detection
- **Operational Uptime**: Increased aircraft availability

---

This comprehensive React app design provides a powerful, interactive interface for aircraft fleet monitoring and maintenance decision support, leveraging the full capabilities of the Databricks digital twin system with seamless drill-down from fleet to component level. 