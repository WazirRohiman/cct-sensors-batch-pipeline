"""
Air Quality Data Staging DAG

This DAG processes raw air quality ZIP files into clean, validated Parquet files
with surrogate key mapping and data quality validation.

Features:
- Processes air quality data from 2019-2022 (4-5 pollutants per year)
- Extracts ZIP files containing multiple Excel files (NO2, O3, PM10, PM2.5, SO2)
- Maps station names to surrogate keys (station_pk)
- Validates data quality and ranges for each pollutant
- Outputs separate Parquet files per pollutant ready for normalisation

Schedule: Manual trigger (@once)
Dependencies: Requires air quality zip files in data/raw/
Output: Staged Parquet files in data/staged/
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Add src to Python path for imports
sys.path.append("/opt/airflow/src")

from pipelines.stage import stage_air_quality_data_task

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

# Available air quality data years (focusing on 2019+ for quality)
AIR_QUALITY_YEARS = ["2019", "2020", "2021", "2022"]

# Create the DAG
dag = DAG(
    "stage_air_quality_data",
    default_args=default_args,
    description="Stage air quality data from ZIP files to Parquet with surrogate keys",
    schedule_interval="@once",  # Manual trigger
    catchup=False,
    tags=["staging", "air-quality", "phase4"],
    doc_md=__doc__,
)

# Task to check raw data availability
check_raw_data = BashOperator(
    task_id="check_raw_data",
    bash_command="""
    echo "Checking for air quality data files in /opt/airflow/data/raw/"
    ls -la /opt/airflow/data/raw/air_quality_*.zip || echo "No air quality files found"
    echo "Checking staging directory"
    mkdir -p /opt/airflow/data/staged
    ls -la /opt/airflow/data/staged/ | grep air_quality || echo "No staged air quality files"
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
        echo "Wind stations (1-7):"
        grep -A 2 "station_pk: [1-7]" /opt/airflow/src/configs/station_mapping.yaml | head -20
        echo "Air quality additional stations (8-11):"
        grep -A 2 "station_pk: [8-9]" /opt/airflow/src/configs/station_mapping.yaml
        grep -A 2 "station_pk: 1[01]" /opt/airflow/src/configs/station_mapping.yaml
    else
        echo "❌ Station mapping config missing"
        exit 1
    fi
    """,
    dag=dag,
)

# Create individual staging tasks for each year
staging_tasks = []

for year in AIR_QUALITY_YEARS:
    task_id = f"stage_air_quality_{year}"

    staging_task = PythonOperator(
        task_id=task_id,
        python_callable=stage_air_quality_data_task,
        op_kwargs={"year": year},
        dag=dag,
        doc_md=f"""
        Stage air quality data for {year}

        This task:
        1. Extracts air_quality_{year}.zip from data/raw/
        2. Parses multiple Excel files (NO2, O3, PM10, PM2.5, SO2)
        3. Maps stations to surrogate keys (station_pk 1-11)
        4. Validates data quality and pollutant ranges
        5. Saves separate Parquet files per pollutant in data/staged/

        Expected outputs:
        - air_quality_no2_{year}.parquet
        - air_quality_o3_{year}.parquet
        - air_quality_pm10_{year}.parquet
        - air_quality_pm25_{year}.parquet (2020+)
        - air_quality_so2_{year}.parquet

        Output columns: datetime, station_{{pk}}_{{pollutant}}
        """,
    )

    staging_tasks.append(staging_task)

# Task to validate staged data
validate_staged_data = BashOperator(
    task_id="validate_staged_data",
    bash_command="""
    echo "Validating staged air quality data files"
    STAGED_DIR="/opt/airflow/data/staged"

    if [ -d "$STAGED_DIR" ]; then
        echo "Staged air quality files:"
        ls -la $STAGED_DIR/air_quality_*.parquet 2>/dev/null || echo "No staged files found"

        # Count total staged files
        STAGED_COUNT=$(ls $STAGED_DIR/air_quality_*.parquet 2>/dev/null | wc -l)
        echo "Total staged air quality files: $STAGED_COUNT"

        # Expected files (4 years × ~5 pollutants = ~20 files)
        # 2019: 4 pollutants (no PM2.5)
        # 2020-2022: 5 pollutants each
        EXPECTED_MIN=15
        if [ $STAGED_COUNT -ge $EXPECTED_MIN ]; then
            echo "✅ Air quality data staging successful ($STAGED_COUNT files)"
        else
            echo "❌ Insufficient air quality files staged ($STAGED_COUNT < $EXPECTED_MIN)"
            exit 1
        fi

        # Check file sizes
        echo "File sizes:"
        ls -lh $STAGED_DIR/air_quality_*.parquet 2>/dev/null | awk '{print $5, $9}'
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
    🎯 Air Quality Data Staging Complete

    Processed Years: {AIR_QUALITY_YEARS}
    Output Directory: /opt/airflow/data/staged/

    Expected Output Files:
    ├── air_quality_no2_YYYY.parquet
    ├── air_quality_o3_YYYY.parquet
    ├── air_quality_pm10_YYYY.parquet
    ├── air_quality_pm25_YYYY.parquet (2020+)
    └── air_quality_so2_YYYY.parquet

    Next Steps:
    1. Review staged Parquet files
    2. Run air quality normalisation pipeline
    3. Load to DuckDB with surrogate keys
    4. Combine with wind data for comprehensive analysis

    Output Schema:
    - datetime: Timestamp
    - station_{{pk}}_{{pollutant}}: Concentration (ug/m3(S))

    Station Coverage:
    - Shared stations (1-7): Atlantis, Bellville South, Bothasig,
                            Goodwood, Khayelitsha, Somerset West, Tableview
    - Air quality only (8-11): Foreshore, Molteno, Plattekloof, Wallacedene

    Pollutants: NO2, O3, PM10, PM2.5, SO2
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
