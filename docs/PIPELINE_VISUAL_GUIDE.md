# Pipeline Visual Guide

**Purpose**: Visual documentation of the CCT Environmental Sensors data pipeline
**Status**: Complete pipeline with 2.9M+ measurements
**Total Duration**: 10-15 minutes end-to-end

---

## Overview

```mermaid
graph TB
    subgraph "Phase 1: Data Ingestion (5-10m)"
        A1[Fetch Air Quality<br/>7 datasets, 2016-2022]
        A2[Fetch Wind Data<br/>5 datasets, 2016-2020]
    end

    subgraph "Phase 2: Staging (3-5m)"
        B1[Stage Air Quality<br/>ZIP/Excel → Parquet]
        B2[Stage Wind<br/>Excel → Parquet]
    end

    subgraph "Phase 3: Normalization (2-3m)"
        C1[Normalize Air Quality<br/>Wide → Tall schema]
        C2[Normalize Wind<br/>Wide → Tall schema]
        C3[Station Dimension<br/>11 stations]
    end

    subgraph "Phase 4: Loading & DQ (8-12m)"
        D1[Initialize DuckDB Schema]
        D2[Load 2.9M Measurements]
        D3[Data Quality Checks]
        D4[Validation & Summary]
    end

    A1 --> B1
    A2 --> B2
    B1 --> C1
    B2 --> C2
    C1 --> C3
    C2 --> C3
    C3 --> D1
    D1 --> D2
    D2 --> D3
    D3 --> D4
```

**Key Metrics**:
- **Total Pipeline Duration**: 10-15 minutes
- **Data Volume**: 2.9M+ measurements
- **Stations**: 11 monitoring locations
- **Temporal Coverage**: 2016-2022 (7 years)
- **Data Quality**: 58.6% valid measurements

---

## Complete Pipeline Flow

### Phase 1: Data Ingestion (Parallel)

```mermaid
flowchart LR
    subgraph ArcGIS["ArcGIS Data Sources"]
        AQ[Air Quality<br/>2016-2022]
        WD[Wind Data<br/>2016-2020]
    end

    subgraph Download["HTTP Download with Retry"]
        AQ --> AQD[7 parallel tasks<br/>ZIP + Excel files]
        WD --> WDD[5 parallel tasks<br/>Excel files]
    end

    subgraph Storage["Raw Storage"]
        AQD --> RAW1[data/raw/<br/>air_quality_*.xlsx/zip]
        WDD --> RAW2[data/raw/<br/>wind_*.xlsx]
    end

    subgraph Error["Error Handling"]
        AQD -.retry failed.-> Q1[data/quarantine/]
        WDD -.retry failed.-> Q1
    end
```

**Features**:
- Parallel downloads (up to 12 simultaneous)
- 3 retry attempts with exponential backoff
- Automatic quarantine of failed downloads
- Content validation and size checks

---

### Phase 2: Staging (Parallel)

```mermaid
flowchart LR
    subgraph Raw["Raw Data"]
        R1[Excel files<br/>ZIP archives]
    end

    subgraph Process["Processing"]
        P1[Extract & Parse<br/>Multi-header handling]
        P2[Station Mapping<br/>Surrogate keys]
        P3[Validation<br/>Range checks]
    end

    subgraph Staged["Staged Data"]
        S1[24 Parquet files<br/>Wide format]
    end

    R1 --> P1
    P1 --> P2
    P2 --> P3
    P3 --> S1
```

**Output**: 24 staged Parquet files (19 air quality + 5 wind)

---

### Phase 3: Normalization (Parallel)

```mermaid
flowchart TB
    subgraph Input["Staged Data (Wide Format)"]
        I1[24 Parquet files]
    end

    subgraph Transform["Transformation"]
        T1[Wide → Tall<br/>Schema conversion]
        T2[Quality Flags<br/>VALID/NODATA]
        T3[Unit Standardization]
    end

    subgraph Output["Normalized Data (Tall Format)"]
        O1[24 normalized files]
        O2[station_dim.parquet<br/>11 stations]
    end

    I1 --> T1
    T1 --> T2
    T2 --> T3
    T3 --> O1
    T3 --> O2
```

**Schema** (Tall Format):
- `datetime` - Timestamp
- `station_pk` - Station surrogate key
- `metric` - Measurement type (wind_speed, pm10, no2, etc.)
- `value` - Measurement value
- `unit` - Unit of measurement
- `quality_flag` - VALID or NODATA
- `source` - wind or air_quality

---

### Phase 4: Loading & Data Quality (Sequential)

```mermaid
flowchart TB
    V[Validate Input Files] --> I[Initialize DuckDB Schema]
    I --> L[Load Measurements<br/>2.9M records]
    L --> DQ[Data Quality Checks<br/>Referential integrity]
    DQ --> VAL[Database Validation<br/>Row counts, indexes]
    VAL --> SUM[Generate Summary Report]

    style DQ fill:#e1f5e1
    style VAL fill:#e1f5e1
```

**Data Quality Results**:
- ✅ **Referential Integrity**: 100% (0 orphaned records)
- ✅ **Primary Keys**: 100% (0 duplicates)
- ✅ **Temporal Integrity**: 100% (0 null timestamps)
- 📊 **Data Completeness**: 58.6% valid measurements

---

## Parallel vs Sequential Execution

```mermaid
gantt
    title Pipeline Execution Timeline
    dateFormat X
    axisFormat %M min

    section Phase 1: Ingestion
    Fetch Air Quality (7 tasks) :0, 10
    Fetch Wind (5 tasks) :0, 10

    section Phase 2: Staging
    Stage Air Quality :10, 15
    Stage Wind :10, 15

    section Phase 3: Normalization
    Normalize Air Quality :15, 18
    Normalize Wind :15, 18

    section Phase 4: Loading
    Validate Input :18, 19
    Initialize Schema :19, 20
    Load Data :20, 28
    DQ Checks :28, 31
    Validation :31, 32
    Summary :32, 33
```

**Parallelism Benefits**:
- **Phase 1-3**: Tasks run in parallel within each phase
- **Phase 4**: Sequential to ensure data consistency
- **Total Duration**: ~13 minutes (vs 50+ minutes if fully sequential)

---

## Error Handling Flow

```mermaid
flowchart TB
    START[Task Starts] --> EXEC[Execute Operation]
    EXEC --> CHECK{Success?}

    CHECK -->|Yes| NEXT[Continue Pipeline]
    CHECK -->|No| RETRY{Retries<br/>Remaining?}

    RETRY -->|Yes| WAIT[Wait<br/>Exponential Backoff]
    WAIT --> EXEC

    RETRY -->|No| QUAR[Quarantine<br/>Log Error Details]
    QUAR --> ALERT[Alert Operator<br/>Manual Review]

    style NEXT fill:#e1f5e1
    style QUAR fill:#ffe1e1
    style ALERT fill:#ffe1e1
```

**Error Handling Layers**:
1. **Airflow Task Retries**: 1-2 retries per task (5 min delay)
2. **Application Retries**: 3 attempts with 4-10s exponential backoff
3. **Quarantine System**: Failed items isolated with error details
4. **XCom Tracking**: Inter-task state visible in Airflow UI
5. **Idempotent Operations**: Safe to re-run entire pipeline

---

## Data Transformation Journey

```mermaid
flowchart LR
    subgraph Raw["RAW<br/>~50MB"]
        R[Excel + ZIP<br/>Multi-row headers<br/>Inconsistent formats]
    end

    subgraph Staged["STAGED<br/>~120MB"]
        S[Parquet Wide<br/>Typed columns<br/>Station keys]
    end

    subgraph Normalized["NORMALIZED<br/>~200MB"]
        N[Parquet Tall<br/>Quality flags<br/>Unified schema]
    end

    subgraph Analytics["ANALYTICS<br/>415MB"]
        A[DuckDB Star Schema<br/>Indexed + Optimized<br/>Query-ready]
    end

    R --> S
    S --> N
    N --> A
```

---

## Star Schema Design

```mermaid
erDiagram
    dim_station ||--o{ fact_measurement : "monitors"

    dim_station {
        int station_pk PK
        string station_code UK
        string station_name
        string location_type
    }

    fact_measurement {
        int measurement_pk PK
        int station_fk FK
        timestamp timestamp
        string metric
        double value
        string unit
        string quality_flag
        string source
    }
```

**Database Statistics**:
- **dim_station**: 11 rows
- **fact_measurement**: 2,963,373 rows
- **Database size**: 414.5 MB
- **Query performance**: <500ms for analytical queries

---

## Monitoring Points

```mermaid
flowchart TB
    subgraph Airflow["Airflow UI (Real-time)"]
        A1[DAG Status Dashboard]
        A2[Task Logs & Retries]
        A3[Gantt Chart Timeline]
    end

    subgraph Container["Container Logs"]
        C1[Scheduler Logs]
        C2[Webserver Logs]
        C3[Task Execution Logs]
    end

    subgraph Quality["Data Quality Metrics"]
        Q1[Referential Integrity]
        Q2[Null Value Analysis]
        Q3[Value Range Checks]
    end

    subgraph Analytics["Jupyter Analytics"]
        J1[Ad-hoc SQL Queries]
        J2[Data Visualizations]
        J3[EDA Notebooks]
    end
```

**Access Points**:
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Jupyter**: http://localhost:8888 (no token)
- **Container Logs**: `docker-compose logs -f [service]`
- **DuckDB**: Direct SQL access via Jupyter or DBeaver

---

## Key Takeaways

### Design Principles

✅ **Maximize Parallelism**: Independent tasks run concurrently (Phases 1-3)
✅ **Ensure Data Consistency**: Sequential execution for database operations (Phase 4)
✅ **Comprehensive Error Handling**: Multi-layer retry with quarantine system
✅ **Idempotent Operations**: Safe to re-run entire pipeline
✅ **Observable**: Multi-layer monitoring (Airflow UI, logs, DQ metrics)
✅ **Analytics-Optimized**: Star schema with columnar storage and indexes

---

**Document Status**: ✅ Complete
**Last Updated**: 2025-10-14
**Purpose**: Tutor feedback response (visual diagram requirement)
