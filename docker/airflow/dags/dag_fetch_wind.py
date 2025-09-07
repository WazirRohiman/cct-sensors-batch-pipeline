import os
import sys
from datetime import datetime

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add src to Python path for imports
sys.path.insert(0, "/opt/airflow/src")

from pipelines.io_arcgis import fetch_item


def load_sources_config():
    """Load data sources configuration."""
    config_path = "/opt/airflow/src/configs/sources.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def fetch_wind_year(year: str, **context):
    """Fetch wind data for a specific year."""
    config = load_sources_config()
    year_config = config["wind"]["years"][year]

    # Get source URL (wind data uses direct URLs)
    source_url = year_config["url"]

    # Set destination path
    dest_path = f"/opt/airflow/data/raw/wind_{year}.xlsx"

    print(f"Fetching wind data for {year}")
    print(f"Source: {source_url}")
    print(f"Destination: {dest_path}")

    # Download file
    result = fetch_item(source_url, dest_path)

    if result:
        print(f"✅ Successfully fetched wind data for {year}")
        return result
    else:
        # Move to quarantine logic would go here
        print(f"❌ Failed to fetch wind data for {year}")
        raise Exception(f"Failed to download wind data for {year}")


def quarantine_failed_download(year: str, error_msg: str, **context):
    """Handle failed downloads by moving to quarantine."""
    quarantine_path = f"/opt/airflow/data/quarantine/wind_{year}_failed.txt"
    os.makedirs(os.path.dirname(quarantine_path), exist_ok=True)

    with open(quarantine_path, "w") as f:
        f.write(f"Failed to download wind data for {year}\n")
        f.write(f"Error: {error_msg}\n")
        f.write(f"Timestamp: {datetime.now()}\n")

    print(f"Quarantined failed download: {quarantine_path}")


# Load configuration to get available years
config = load_sources_config()
wind_years = list(config["wind"]["years"].keys())

with DAG(
    dag_id="fetch_wind",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["data-ingestion", "wind"],
    description="Fetch wind data from City of Cape Town ArcGIS portal",
):

    # Create fetch tasks for each year
    fetch_tasks = []

    for year in wind_years:
        fetch_task = PythonOperator(
            task_id=f"fetch_wind_{year}",
            python_callable=fetch_wind_year,
            op_args=[year],
            retries=2,
        )
        fetch_tasks.append(fetch_task)

    # Set task dependencies - all years can run in parallel
    fetch_tasks
