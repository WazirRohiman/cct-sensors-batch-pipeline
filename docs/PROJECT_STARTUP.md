# Project Startup Guide

This guide walks you through setting up the City of Cape Town Environmental Sensors Data Pipeline from scratch.

## 📋 Prerequisites

Before you begin, ensure you have the following installed on your system:

- **Docker Desktop** (with Docker Compose)
- **Git** for cloning the repository
- **Make** (usually pre-installed on Linux/macOS, available via package managers)
- **uv** (Python package manager) - Optional for local development

### System Requirements
- **Memory**: At least 4GB RAM available for Docker
- **Storage**: ~2GB for Docker images and data
- **OS**: Linux, macOS, or Windows with WSL2

## 🚀 Quick Start (5 Steps)

### Step 1: Clone the Repository
```bash
git clone <repository-url>
cd cct-sensors-batch-pipeline
```

### Step 2: Create Required Directories
```bash
make data-dirs
```
This creates: `data/raw`, `data/staged`, `data/duckdb`, `data/quarantine`, `data/logs`, `notebooks`, `progress_reports`

### Step 3: Start All Services
```bash
make airflow-up
```
This command will:
- Download required Docker images (~1-2GB first time)
- Start PostgreSQL database
- Initialize Airflow with admin user
- Initialize DuckDB with required schema
- Start Airflow webserver, scheduler, and triggerer
- Start Jupyter notebook server

**⏱️ First-time setup takes 5-10 minutes** depending on your internet connection.

### Step 4: Verify Services are Running
Wait for the startup to complete, then check:
```bash
docker ps
```
You should see containers for: `postgres`, `airflow-webserver`, `airflow-scheduler`, `airflow-triggerer`, `viewer`

### Step 5: Access the Web Interfaces

#### Airflow UI
- **URL**: http://localhost:8080
- **Username**: `admin`
- **Password**: `admin`

#### Jupyter Notebooks
- **URL**: http://localhost:8888
- **Token**: None required
- **DuckDB Example**: Open `DuckDB_Example.ipynb` to test database connectivity

## 🔧 Available Commands

| Command | Description |
|---------|-------------|
| `make help` | Show all available commands |
| `make setup` | Create Python virtual environment (optional) |
| `make airflow-up` | Start all services |
| `make airflow-down` | Stop all services |
| `make airflow-reset` | Reset everything (removes data) |
| `make db-init` | Reinitialize DuckDB schema only |
| `make data-dirs` | Create required directories |

## 🧪 Testing Your Setup

### Test 1: Airflow Access
1. Open http://localhost:8080
2. Login with admin/admin
3. You should see the Airflow dashboard with 4 DAGs:
   - `fetch_air_quality`
   - `fetch_wind`
   - `stage_and_normalize`
   - `load_dq_publish`

### Test 2: Jupyter + DuckDB Access
1. Open http://localhost:8888
2. Open the `DuckDB_Example.ipynb` notebook
3. Run the cells to test database connectivity
4. Should show empty tables (until data is loaded)

## 📊 Project Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Raw Data  │ -> │   Airflow   │ -> │   DuckDB    │
│  (External) │    │(Orchestrate)│    │ (Analytics) │
└─────────────┘    └─────────────┘    └─────────────┘
                           │                   │
                           v                   v
                   ┌─────────────┐    ┌─────────────┐
                   │  Quarantine │    │   Jupyter   │
                   │ (Bad Data)  │    │ (Analysis)  │
                   └─────────────┘    └─────────────┘
```

## 🗂️ Directory Structure

```
cct-sensors-batch-pipeline/
├── data/                    # All data files (created by make data-dirs)
│   ├── raw/                # Downloaded source files
│   ├── staged/             # Processed files (Parquet/CSV)
│   ├── duckdb/             # Database files (cct_env.duckdb)
│   ├── quarantine/         # Failed/invalid data
│   └── logs/               # Processing logs
├── docker/                 # Docker configuration
│   ├── airflow/            # Airflow services
│   └── duckdb/             # DuckDB initialization
├── notebooks/              # Jupyter notebooks
├── src/                    # Python source code
│   ├── configs/            # Configuration files
│   ├── pipelines/          # ETL pipeline code
│   └── utils/              # Utility functions
└── docs/                   # Documentation
```

## 🚨 Troubleshooting

### Issue: Containers won't start
**Solution**: 
```bash
make airflow-down
docker system prune -f
make airflow-up
```

### Issue: Port conflicts (8080 or 8888 in use)
**Solution**: Stop conflicting services or modify ports in `docker/airflow/docker-compose.yml`

### Issue: "No space left on device"
**Solution**: Clean up Docker:
```bash
docker system prune -a
docker volume prune
```

### Issue: Airflow UI shows "Airflow is not ready"
**Solution**: Wait 2-3 minutes for initialization, then refresh

### Issue: Can't connect to DuckDB in Jupyter
**Solution**: 
1. Check if DuckDB file exists: `ls -la data/duckdb/`
2. Restart Jupyter: `docker compose restart viewer`
3. Try the `DuckDB_Example.ipynb` notebook

## 🛠️ Development Mode

For local development (optional):

### Setup Python Environment
```bash
make setup
source .venv/bin/activate
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Run Code Quality Checks
```bash
make lint  # Check code style
```

## 📝 Next Steps

Once your setup is running:

1. **Explore the DAGs** in Airflow UI (currently placeholder)
2. **Test DuckDB queries** in Jupyter notebooks
3. **Check logs** in `data/logs/` directory
4. **Review architecture** in `docs/ARCHITECTURE.md`

## 🔄 Daily Operations

### Starting Work
```bash
make airflow-up
# Wait for services to be ready
# Open http://localhost:8080 and http://localhost:8888
```

### Ending Work
```bash
make airflow-down
```

### Clean Reset (removes all data)
```bash
make airflow-reset
```

---

## 📞 Support

If you encounter issues:
1. Check this troubleshooting section
2. Review logs: `docker logs <container-name>`
3. Verify system requirements are met
4. Try a clean reset: `make airflow-reset`