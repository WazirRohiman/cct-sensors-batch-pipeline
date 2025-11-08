# System Architecture

## Overview

The City of Cape Town Environmental Sensors Data Pipeline implements a robust, production-ready batch processing system using containerized Apache Airflow orchestration and DuckDB analytics storage.

## Architecture Diagram

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Ingestion      │    │   Processing    │
│                 │    │                  │    │                 │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │ Air Quality │ │───▶│ │ ArcGIS       │ │───▶│ │ Staging     │ │
│ │ (2016-2022) │ │    │ │ Fetch Logic  │ │    │ │ Pipeline    │ │
│ └─────────────┘ │    │ │              │ │    │ │ (Future)    │ │
│                 │    │ │ • Retry      │ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ │ • Validate   │ │    │                 │
│ │ Wind Data   │ │───▶│ │ • Stream     │ │    │ ┌─────────────┐ │
│ │ (2016-2020) │ │    │ │ • Quarantine │ │    │ │ Normalise   │ │
│ └─────────────┘ │    │ └──────────────┘ │    │ │ (Future)    │ │
└─────────────────┘    └──────────────────┘    │ └─────────────┘ │
                                               └─────────────────┘
                                                         │
                       ┌─────────────────┐              ▼
                       │   Storage       │    ┌─────────────────┐
                       │                 │    │   Analytics     │
                       │ ┌─────────────┐ │    │                 │
                       │ │ Raw Files   │ │◀───│ ┌─────────────┐ │
                       │ │ data/raw/   │ │    │ │ DuckDB      │ │
                       │ └─────────────┘ │    │ │ Star Schema │ │
                       │                 │    │ │ (Future)    │ │
                       │ ┌─────────────┐ │    │ └─────────────┘ │
                       │ │ Quarantine  │ │    │                 │
                       │ │ data/quar./ │ │    │ ┌─────────────┐ │
                       │ └─────────────┘ │    │ │ Query       │ │
                       └─────────────────┘    │ │ Interface   │ │
                                              │ │ (Future)    │ │
                                              │ └─────────────┘ │
                                              └─────────────────┘
```

## Data Flow Pipeline

### ✅ Phase 3: Data Ingestion (Current)

**Step 1: Source Identification**
- Configuration-driven source discovery via `sources.yaml`
- Support for multiple URL types (direct data URLs, ArcGIS item pages)
- Dynamic task generation in Airflow DAGs

**Step 2: HTTP Ingestion**
```
ArcGIS Portal → fetch_item() → data/raw/
             ↓
      Retry Logic (3x)
             ↓  
      Content Validation
             ↓
      Success → Raw Files
             ↓
      Failure → Quarantine
```

**Step 3: File Organization**
- **Raw Storage**: `data/raw/air_quality_YYYY.[xlsx|zip]`, `data/raw/wind_YYYY.xlsx`
- **Error Handling**: `data/quarantine/[source]_YYYY_failed.txt`
- **Logging**: Container logs + Airflow task logs

### ✅ Phase 4: Data Processing COMPLETE

**Step 4: Staging Pipeline (Wind + Air Quality)**
```
data/raw/wind_YYYY.xlsx → Multi-header parsing + station surrogate keys
                       → Validation (ranges, timestamp coercion)
                       → data/staged/wind_YYYY.parquet (5 files)

data/raw/air_quality_YYYY.[xlsx|zip] → ZIP extraction + Excel parsing
                                    → Station surrogate key mapping
                                    → data/staged/air_quality_{pollutant}_{year}.parquet (19 files)
```

**Step 5: Schema Normalization (Wind + Air Quality)**
```
data/staged/*.parquet (24 files)
          ↓
Wide→Tall transformation with unit + quality flags
          ↓
Join station metadata from station_mapping.yaml
          ↓
data/normalised/*.parquet (24 normalised files)
├── wind_YYYY_normalised.parquet (5 files)
└── air_quality_{pollutant}_{year}_normalised.parquet (18 files)
          ↓
Idempotent station_dim.parquet update (11 stations, PKs 1-11)
```

**Step 6: Database Loading (Next - Phase 5)**
```
Normalised Data (24 fact files + 1 dimension)
               ↓
    DuckDB Star Schema Implementation
               ↓
    dim_station (11 stations, locations, metadata)
               ↓
    fact_measurement (datetime, station_pk, metric, value, quality_flag)
               ↓
    Idempotent Loading (DELETE + INSERT or MERGE pattern)
```

## Infrastructure Components

### Container Orchestration

**Docker Compose Services**:
```yaml
postgres          # Airflow metadata database
airflow-init      # Database migration + user setup + permissions
airflow-webserver # UI and API (port 8080)
airflow-scheduler # Task orchestration
airflow-triggerer # Deferred task handling
```

**Volume Mounts**:
```
./dags → /opt/airflow/dags          # DAG definitions
../data → /opt/airflow/data         # Data storage
../src → /opt/airflow/src           # Pipeline modules  
requirements.txt → /opt/airflow/    # Python dependencies
```

### Data Storage Architecture

**Directory Structure**:
```
data/
├── raw/           # ✅ Downloaded files (24 files: Excel, ZIP)
├── quarantine/    # ✅ Failed downloads + error logs
├── staged/        # ✅ Wide-format Parquet (24 files: 5 wind + 19 air quality)
├── normalised/    # ✅ Tall fact tables (24 files) + station dimension (1 file)
├── duckdb/        # 🔄 Database files (Phase 5)
└── logs/          # 🔄 Processing logs (Phase 5)
```

**File Naming Conventions**:
- **Raw**: `air_quality_2022.xlsx`, `air_quality_2021.zip`, `wind_2020.xlsx`
- **Staged**: `wind_YYYY.parquet`, `air_quality_{pollutant}_{year}.parquet`
- **Normalised**: `wind_YYYY_normalised.parquet`, `air_quality_{pollutant}_{year}_normalised.parquet`
- **Dimension**: `station_dim.parquet` (shared across datasets)
- **Errors**: `air_quality_2021_failed.txt`

### Processing Architecture

**Airflow DAG Structure** (8 Operational DAGs):
```python
# Phase 3: Data Ingestion
fetch_air_quality (DAG) → 7 parallel tasks (2016-2022)
fetch_wind (DAG) → 5 parallel tasks (2016-2020)

# Phase 4: Staging
dag_stage_wind (DAG) → 5 parallel tasks (2016-2020)
dag_stage_air (DAG) → 4 parallel tasks (2019-2022)

# Phase 4: Normalization
dag_normalise_wind (DAG) → normalise_all_wind task
dag_normalise_air_quality (DAG) → normalise_all_air_quality task

# Phase 5: Analytics (Placeholder)
dag_load_dq_publish (DAG) → Planned for DuckDB loading + DQ checks
```

**Parallel Execution Model**:
- **LocalExecutor**: Up to 32 concurrent tasks
- **Independent Tasks**: Each year processes simultaneously
- **Resource Management**: Automatic load balancing
- **Fault Isolation**: One year's failure doesn't affect others

## Technology Stack

### Core Technologies

**Orchestration**: Apache Airflow 2.10.2
- LocalExecutor for parallel processing
- PostgreSQL metadata storage
- Web UI for monitoring and control

**Data Storage**: DuckDB (embedded analytical database)
- Zero-server footprint
- Excellent performance for analytics workloads
- Portable `.duckdb` files on mounted volumes

**Containerization**: Docker + Docker Compose
- Reproducible deployment across environments
- Automated permission and directory setup
- Health checks and service dependencies

### Python Dependencies

**Core Libraries**:
- `requests`: HTTP client for ArcGIS API integration
- `tenacity`: Retry logic with exponential backoff
- `PyYAML`: Configuration management
- `pandas`: Data manipulation (future phases)
- `duckdb`: Database connectivity (future phases)

**Data Processing** (Future):
- `pyarrow`: Parquet file format support
- `pandera`: Schema validation framework
- `openpyxl`: Excel file processing

## Security Architecture

### Access Control
- **Airflow Admin**: Username/password authentication
- **Container Isolation**: Services run in isolated containers
- **File Permissions**: Proper user/group ownership (airflow:root)

### Data Security
- **Public Data Only**: No sensitive/private data processing
- **Network Security**: HTTPS-only communications with ArcGIS
- **Error Isolation**: Failed downloads quarantined, not exposed

### Secrets Management
- **Environment Variables**: Sensitive config via `.env` files
- **Airflow Secrets**: Database credentials, admin passwords
- **Container Secrets**: AIRFLOW_UID, secret keys

## Performance Characteristics

### Current Performance (Phase 3)

**Download Throughput**:
- Small Excel files: 2-5 seconds each
- Large Excel files: 10-15 seconds each  
- ZIP archives: 15-30 seconds each
- Parallel downloads: 5-12 files simultaneously

**Resource Usage**:
- Memory: ~2GB total (all containers)
- Disk I/O: Streaming downloads (minimal RAM usage)
- Network: Efficient with retry backoff

### Scalability Design

**Horizontal Scaling** (Future):
- CeleryExecutor for distributed task execution
- Redis/RabbitMQ message broker
- Multiple worker nodes

**Vertical Scaling**:
- Increase LocalExecutor parallelism (currently 32)
- Larger container resource allocations
- Faster storage for DuckDB operations

## Monitoring & Observability

### Logging Architecture

**Log Levels**:
- **INFO**: Task start/completion, file downloads
- **WARNING**: Unexpected content types, retries  
- **ERROR**: Download failures, permission issues
- **DEBUG**: Detailed HTTP request/response info

**Log Destinations**:
- **Airflow UI**: Per-task logs with full details
- **Container Logs**: `docker compose logs [service]`
- **File Logs**: `data/logs/` for persistent storage

### Health Monitoring

**Service Health Checks**:
- PostgreSQL: Database connectivity
- Airflow Webserver: HTTP endpoint response
- Container Health: Docker health check status

**Data Pipeline Monitoring** (Future):
- Task success/failure rates per DAG
- Download completion percentages
- Data quality metrics
- Processing time trends

## Error Handling Strategy

### Retry Logic
```python
@retry(
    stop=stop_after_attempt(3),           # Max attempts
    wait=wait_exponential(                # Backoff strategy
        multiplier=1, min=4, max=10
    )
)
```

### Failure Categories

**Network Failures**: Automatic retry with backoff
**HTTP Errors**: Differentiated handling (4xx vs 5xx)
**File System Errors**: Immediate failure with detailed logging
**Validation Errors**: Quarantine with reason logging

### Quarantine System
- **Purpose**: Isolate problematic downloads for manual inspection
- **Content**: Error description, timestamp, source URL
- **Recovery**: Manual intervention or configuration updates

## Completed Architecture (Phase 4)

### ✅ Phase 4: Processing Pipeline COMPLETE
- ✅ **Staging Module**: Excel/ZIP extraction to standardized Parquet (24 files)
- ✅ **Normalization Engine**: Wide-to-tall schema transformation (24 normalised files)
- ✅ **Station Dimension**: Shared idempotent dimension (11 stations, PKs 1-11)
- ✅ **Airflow Orchestration**: 8 operational DAGs for end-to-end processing
- ✅ **Quality Assurance**: 4 EDA notebooks for validation

### 🔄 Phase 5: Analytics & Publishing (Next)
- **DuckDB Loading**: Star schema implementation (`dim_station` + `fact_measurement`)
- **Data Quality Framework**: Pandera validation + domain checks + quality reporting
- **Query Interface**: SQL-based exploration via JupyterLab/DuckDB
- **Cross-Dataset Analytics**: Wind + air quality correlation analysis
- **Export Capabilities**: CSV, Parquet, JSON format support
- **Performance Optimization**: Indexing, partitioning, and caching strategies

This architecture provides a solid foundation for reliable, scalable environmental data processing while maintaining simplicity and maintainability.
