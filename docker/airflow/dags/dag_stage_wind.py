"""
Wind Data Staging DAG

This DAG processes raw wind Excel files into clean, validated Parquet files
with surrogate key mapping and data quality validation.

Features:
- Processes wind data from 2016-2020
- Converts multi-header Excel to standardized wide format
- Maps station names to surrogate keys (station_pk)
- Validates data quality and ranges
- Outputs Parquet files ready for normalisation

Schedule: Manual trigger (@once)
Dependencies: Requires wind data files in data/raw/
Output: Staged Parquet files in data/staged/
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Add src to Python path for imports
sys.path.append("/opt/airflow/src")

from pipelines.stage import stage_wind_data_task

# DAG configuration
default_args = {
    "owner": "cct-data-pipeline",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Available wind data years (based on files in data/raw/)
WIND_YEARS = ["2016", "2017", "2018", "2019", "2020"]

# Create the DAG
dag = DAG(
    "stage_wind_data",
    default_args=default_args,
    description="Stage wind data from Excel to Parquet with surrogate keys",
    schedule_interval="@once",  # Manual trigger
    catchup=False,
    tags=["staging", "wind", "phase4"],
    doc_md=__doc__,
)

# Task to check raw data availability
check_raw_data = BashOperator(
    task_id="check_raw_data",
    bash_command="""
    echo "Checking for wind data files in /opt/airflow/data/raw/"
    ls -la /opt/airflow/data/raw/wind_*.xlsx || echo "No wind files found"
    echo "Checking staging directory"
    mkdir -p /opt/airflow/data/staged
    ls -la /opt/airflow/data/staged/ || echo "Staging directory empty"
    """,
    dag=dag,
)

# Task to verify station mapping configuration
check_config = BashOperator(
    task_id="check_station_mapping",
    bash_command="""
    echo "Checking station mapping configuration"
    if [ -f "/opt/airflow/src/configs/station_mapping.yaml" ]; then
        echo "✅ Station mapping config found"
        head -20 /opt/airflow/src/configs/station_mapping.yaml
    else
        echo "❌ Station mapping config missing"
        exit 1
    fi
    """,
    dag=dag,
)

# Create individual staging tasks for each year
staging_tasks = []

for year in WIND_YEARS:
    task_id = f"stage_wind_{year}"

    staging_task = PythonOperator(
        task_id=task_id,
        python_callable=stage_wind_data_task,
        op_kwargs={"year": year},
        dag=dag,
        doc_md=f"""
        Stage wind data for {year}

        This task:
        1. Reads wind_{year}.xlsx from data/raw/
        2. Parses multi-level headers with station names
        3. Maps stations to surrogate keys (station_pk)
        4. Validates data quality and ranges
        5. Saves as wind_{year}.parquet in data/staged/

        Output columns: datetime, station_1_wind_direction, station_1_wind_speed, ...
        """,
    )

    staging_tasks.append(staging_task)

# Task to validate staged data
validate_staged_data = BashOperator(
    task_id="validate_staged_data",
    bash_command="""
    echo "Validating staged wind data files"
    STAGED_DIR="/opt/airflow/data/staged"

    if [ -d "$STAGED_DIR" ]; then
        echo "Staged files:"
        ls -la $STAGED_DIR/wind_*.parquet 2>/dev/null || echo "No staged wind files found"

        # Count total staged files
        STAGED_COUNT=$(ls $STAGED_DIR/wind_*.parquet 2>/dev/null | wc -l)
        echo "Total staged wind files: $STAGED_COUNT"

        if [ $STAGED_COUNT -gt 0 ]; then
            echo "✅ Wind data staging successful"
        else
            echo "❌ No wind data staged"
            exit 1
        fi
    else
        echo "❌ Staged directory not found"
        exit 1
    fi
    """,
    dag=dag,
)

# Task to generate staging summary
generate_summary = PythonOperator(
    task_id="generate_staging_summary",
    python_callable=lambda **context: print(
        f"""
    🎯 Wind Data Staging Complete

    Processed Years: {WIND_YEARS}
    Output Directory: /opt/airflow/data/staged/

    Next Steps:
    1. Review staged Parquet files
    2. Run normalisation pipeline
    3. Load to DuckDB with surrogate keys

    Output Schema:
    - datetime: Timestamp
    - station_{{pk}}_wind_direction: Wind direction (degrees)
    - station_{{pk}}_wind_speed: Wind speed (m/s)

    Station PKs: 1=Atlantis, 2=Bellville South, 3=Bothasig,
                 4=Goodwood, 5=Khayelitsha, 6=Somerset West, 7=Tableview
    """
    ),
    dag=dag,
)

# Define task dependencies
check_raw_data >> check_config >> staging_tasks >> validate_staged_data >> generate_summary

# Set up parallel execution for staging tasks
# All years can be processed simultaneously since they're independent files
if len(staging_tasks) > 1:
    # Tasks are already set to run in parallel after check_config
    pass
