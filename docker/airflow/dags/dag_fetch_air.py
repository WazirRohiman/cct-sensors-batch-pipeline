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


def fetch_air_quality_year(year: str, **context):
    """Fetch air quality data for a specific year."""
    config = load_sources_config()
    year_config = config["air_quality"]["years"][year]

    # Determine source URL
    if "url" in year_config:
        source_url = year_config["url"]
    elif "item_page" in year_config:
        source_url = year_config["item_page"]
    else:
        raise ValueError(f"No URL or item_page found for air quality {year}")

    # Set destination path
    file_extension = "xlsx" if year_config["type"] == "xlsx" else "zip"
    dest_path = f"/opt/airflow/data/raw/air_quality_{year}.{file_extension}"

    print(f"Fetching air quality data for {year}")
    print(f"Source: {source_url}")
    print(f"Destination: {dest_path}")

    # Download file
    result = fetch_item(source_url, dest_path)

    if result:
        print(f"✅ Successfully fetched air quality data for {year}")
        return result
    else:
        # Move to quarantine logic would go here
        print(f"❌ Failed to fetch air quality data for {year}")
        raise Exception(f"Failed to download air quality data for {year}")


def quarantine_failed_download(year: str, error_msg: str, **context):
    """Handle failed downloads by moving to quarantine."""
    quarantine_path = f"/opt/airflow/data/quarantine/air_quality_{year}_failed.txt"
    os.makedirs(os.path.dirname(quarantine_path), exist_ok=True)

    with open(quarantine_path, "w") as f:
        f.write(f"Failed to download air quality data for {year}\n")
        f.write(f"Error: {error_msg}\n")
        f.write(f"Timestamp: {datetime.now()}\n")

    print(f"Quarantined failed download: {quarantine_path}")


# Load configuration to get available years
config = load_sources_config()
air_quality_years = list(config["air_quality"]["years"].keys())

with DAG(
    dag_id="fetch_air_quality",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["data-ingestion", "air-quality"],
    description="Fetch air quality data from City of Cape Town ArcGIS portal",
):

    # Create fetch tasks for each year
    fetch_tasks = []

    for year in air_quality_years:
        fetch_task = PythonOperator(
            task_id=f"fetch_air_quality_{year}",
            python_callable=fetch_air_quality_year,
            op_args=[year],
            retries=2,
        )
        fetch_tasks.append(fetch_task)

    # Set task dependencies - all years can run in parallel
    fetch_tasks
