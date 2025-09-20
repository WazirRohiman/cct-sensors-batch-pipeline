"""
Wind Data Normalisation DAG

Transforms staged wind Parquet files into the tall-format fact tables and
station dimension expected by downstream DuckDB loading. Assumes the wind
staging DAG has already produced files in /opt/airflow/data/staged/.

Features:
- Validates presence of staged wind parquet files
- Normalises each year independently to isolate failures
- Emits tall-format Parquet files to /opt/airflow/data/normalised/
- Maintains an idempotent station_dim.parquet for shared usage

Schedule: Manual trigger (@once)
Dependencies: Requires staged files from dag_stage_wind
Outputs: wind_YYYY_normalised.parquet + station_dim.parquet
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/src")

from pipelines.normalise import normalise_all_wind

default_args = {
    "owner": "cct-data-pipeline",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

WIND_YEARS = ["2016", "2017", "2018", "2019", "2020"]


dag = DAG(
    "normalise_wind_data",
    default_args=default_args,
    description="Normalise staged wind data to tall fact format",
    schedule_interval="@once",
    catchup=False,
    tags=["normalisation", "wind", "phase4"],
    doc_md=__doc__,
)


check_staged_files = BashOperator(
    task_id="check_staged_wind",
    bash_command="""
    echo "Checking for staged wind files in /opt/airflow/data/staged";
    ls -la /opt/airflow/data/staged/wind_*.parquet \
        || (echo "❌ No staged wind files present" && exit 1);
    mkdir -p /opt/airflow/data/normalised;
    echo "Staged files ready for normalisation.";
    """,
    dag=dag,
)


normalise_all = PythonOperator(
    task_id="normalise_all_wind",
    python_callable=normalise_all_wind,
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
    ls -la $NORMALISED_DIR/wind_*_normalised.parquet \
        || (echo "❌ No normalised wind files found" && exit 1);
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
🎯 Wind Data Normalisation Complete

- Source directory: /opt/airflow/data/staged
- Output directory: /opt/airflow/data/normalised
- Expected artifacts:
    • wind_YYYY_normalised.parquet (tall fact data)
    • station_dim.parquet (station dimension)

Next steps:
1. Run data-quality checks (dq_checks pipeline).
2. Load normalised data into DuckDB (load_duckdb pipeline).
3. Combine with air quality once normalisation is implemented there.
        """
    )


log_summary = PythonOperator(
    task_id="log_normalisation_summary",
    python_callable=_log_summary,
    dag=dag,
)

check_staged_files >> normalise_all >> validate_normalised >> log_summary
