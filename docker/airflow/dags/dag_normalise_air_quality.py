"""
Air Quality Data Normalisation DAG

Transforms staged air quality Parquet files into the tall-format fact tables and
station dimension expected by downstream DuckDB loading. Assumes the air quality
staging DAG has already produced files in /opt/airflow/data/staged/.

Features:
- Validates presence of staged air quality parquet files
- Normalises each pollutant/year independently to isolate failures
- Emits tall-format Parquet files to /opt/airflow/data/normalised/
- Maintains an idempotent station_dim.parquet for shared usage

Schedule: Manual trigger (@once)
Dependencies: Requires staged files from dag_stage_air_quality_data
Outputs: air_quality_{pollutant}_{year}_normalised.parquet + station_dim.parquet
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/src")

from pipelines.normalise import normalise_all_air_quality

default_args = {
    "owner": "cct-data-pipeline",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Air quality years and pollutants
AIR_QUALITY_YEARS = ["2019", "2020", "2021", "2022"]
POLLUTANTS = ["no2", "o3", "pm10", "pm25", "so2"]


dag = DAG(
    "normalise_air_quality_data",
    default_args=default_args,
    description="Normalise staged air quality data to tall fact format",
    schedule_interval="@once",
    catchup=False,
    tags=["normalisation", "air-quality", "phase4"],
    doc_md=__doc__,
)


check_staged_files = BashOperator(
    task_id="check_staged_air_quality",
    bash_command="""
    echo "Checking for staged air quality files in /opt/airflow/data/staged";

    STAGED_COUNT=$(ls /opt/airflow/data/staged/air_quality_*.parquet 2>/dev/null | wc -l);

    if [ $STAGED_COUNT -eq 0 ]; then
        echo "❌ No staged air quality files present";
        exit 1;
    fi;

    echo "Found $STAGED_COUNT staged air quality files";
    ls -la /opt/airflow/data/staged/air_quality_*.parquet;

    mkdir -p /opt/airflow/data/normalised;
    echo "Staged files ready for normalisation.";
    """,
    dag=dag,
)


normalise_all = PythonOperator(
    task_id="normalise_all_air_quality",
    python_callable=normalise_all_air_quality,
    op_kwargs={
        "staged_dir": "/opt/airflow/data/staged",
        "normalised_dir": "/opt/airflow/data/normalised",
    },
    dag=dag,
)


validate_normalised = BashOperator(
    task_id="validate_normalised_outputs",
    bash_command="""
    NORMALISED_DIR="/opt/airflow/data/normalised";
    echo "Validating normalised outputs in $NORMALISED_DIR";

    NORMALISED_COUNT=$(ls $NORMALISED_DIR/air_quality_*_normalised.parquet 2>/dev/null | wc -l);

    if [ $NORMALISED_COUNT -eq 0 ]; then
        echo "❌ No normalised air quality files found";
        exit 1;
    fi;

    echo "Found $NORMALISED_COUNT normalised air quality files";
    ls -la $NORMALISED_DIR/air_quality_*_normalised.parquet;

    if [ ! -f "$NORMALISED_DIR/station_dim.parquet" ]; then
        echo "❌ station_dim.parquet missing";
        exit 1;
    fi;

    echo "✅ Normalised files and station dimension present.";
    """,
    dag=dag,
)


def _log_summary(**context):
    print(
        """
🎯 Air Quality Data Normalisation Complete

- Source directory: /opt/airflow/data/staged
- Output directory: /opt/airflow/data/normalised
- Expected artifacts:
    • air_quality_{pollutant}_{year}_normalised.parquet (tall fact data)
    • station_dim.parquet (station dimension - shared with wind)

Pollutants processed: NO2, O3, PM10, PM2.5, SO2
Years processed: 2019-2022
Total expected files: 19 normalised air quality files

Next steps:
1. Run data-quality checks (dq_checks pipeline).
2. Load normalised data into DuckDB (load_duckdb pipeline).
3. Combine with wind data for cross-dataset analytics.
        """
    )


log_summary = PythonOperator(
    task_id="log_normalisation_summary",
    python_callable=_log_summary,
    dag=dag,
)

check_staged_files >> normalise_all >> validate_normalised >> log_summary
