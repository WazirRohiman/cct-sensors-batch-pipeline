"""
DuckDB Loading, Data Quality & Publishing DAG

Loads normalised Parquet files into DuckDB star schema, runs data quality
checks, and prepares data for analytics/publishing.

Features:
- Validates normalised data exists
- Initializes DuckDB schema (if needed)
- Loads station dimension and fact measurements
- Runs comprehensive data quality checks
- Generates load summary statistics

Schedule: Manual trigger (@once)
Dependencies: Requires normalised files from normalization DAGs
Outputs: Populated DuckDB database with integrity verification
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/src")

from pipelines.load_duckdb import (
    load_all_normalised,
    verify_data_integrity,
)

default_args = {
    "owner": "cct-data-pipeline",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "load_dq_publish",
    default_args=default_args,
    description="Load normalised data to DuckDB with data quality checks",
    schedule_interval="@once",
    catchup=False,
    tags=["loading", "duckdb", "dq", "phase5"],
    doc_md=__doc__,
)


# Task 1: Check normalised data exists
check_normalised_data = BashOperator(
    task_id="check_normalised_data",
    bash_command="""
    echo "🔍 Checking for normalised data...";

    NORMALISED_DIR="/opt/airflow/data/normalised";

    # Check if directory exists
    if [ ! -d "$NORMALISED_DIR" ]; then
        echo "❌ normalised directory not found: $NORMALISED_DIR";
        exit 1;
    fi;

    # Count normalised files
    FACT_COUNT=$(ls $NORMALISED_DIR/*_normalised.parquet 2>/dev/null | wc -l);
    STATION_EXISTS=$(ls $NORMALISED_DIR/station_dim.parquet 2>/dev/null | wc -l);

    echo "📊 Found $FACT_COUNT normalised fact files";
    echo "📊 Station dimension: $STATION_EXISTS file(s)";

    if [ $FACT_COUNT -eq 0 ]; then
        echo "❌ No normalised files found - run normalization DAGs first";
        exit 1;
    fi;

    if [ $STATION_EXISTS -eq 0 ]; then
        echo "❌ Station dimension not found - run normalization DAGs first";
        exit 1;
    fi;

    echo "✅ normalised data ready for loading";
    echo "   - Fact files: $FACT_COUNT";
    echo "   - Station dimension: present";

    ls -lh $NORMALISED_DIR/*.parquet | head -10;
    """,
    dag=dag,
)


# Task 2: Initialize DuckDB schema (if needed)
init_duckdb_schema = BashOperator(
    task_id="init_duckdb_schema",
    bash_command="""
    echo "🔧 Initializing DuckDB schema...";

    DUCKDB_PATH="/opt/airflow/data/duckdb/cct_env.duckdb";

    # Check if DuckDB file exists
    if [ -f "$DUCKDB_PATH" ]; then
        echo "📦 DuckDB file exists: $DUCKDB_PATH";
        echo "   Size: $(du -h $DUCKDB_PATH | cut -f1)";
    else
        echo "🆕 DuckDB file not found, will be created during initialization";
    fi;

    # Run db_init.py to create/verify schema
    echo "🚀 Running database initialization...";
    cd /opt/airflow;
    python -m src.pipelines.db_init;

    if [ $? -eq 0 ]; then
        echo "✅ DuckDB schema initialized successfully";
    else
        echo "❌ DuckDB schema initialization failed";
        exit 1;
    fi;
    """,
    dag=dag,
)


# Task 3: Load all normalised data
def load_data_wrapper(**context):
    """Wrapper function to load all normalised data and push stats to XCom."""
    stats = load_all_normalised(
        normalised_dir="/opt/airflow/data/normalised",
        duckdb_path="/opt/airflow/data/duckdb/cct_env.duckdb",
    )

    # Push stats to XCom for downstream tasks
    context["task_instance"].xcom_push(key="load_stats", value=stats)

    # Fail task if there were any failed loads
    if stats["failed_loads"] > 0:
        raise ValueError(f"Load completed with {stats['failed_loads']} failures")

    return stats


load_normalised_data = PythonOperator(
    task_id="load_normalised_data",
    python_callable=load_data_wrapper,
    dag=dag,
)


# Task 4: Run data quality checks
def data_quality_checks_wrapper(**context):
    """Wrapper function to run DQ checks and push results to XCom."""
    integrity_results = verify_data_integrity(duckdb_path="/opt/airflow/data/duckdb/cct_env.duckdb")

    # Push results to XCom
    context["task_instance"].xcom_push(key="dq_results", value=integrity_results)

    # Fail task if integrity checks fail
    if not integrity_results["integrity_ok"]:
        raise ValueError("Data integrity checks failed - review logs")

    return integrity_results


run_dq_checks = PythonOperator(
    task_id="run_data_quality_checks",
    python_callable=data_quality_checks_wrapper,
    dag=dag,
)


# Task 5: Validate DuckDB state
def validate_duckdb_wrapper(**context):
    """Wrapper function to validate DuckDB state and push results to XCom."""
    from pipelines.validate_duckdb import validate_duckdb_state

    results = validate_duckdb_state(duckdb_path="/opt/airflow/data/duckdb/cct_env.duckdb")

    # Push results to XCom
    context["task_instance"].xcom_push(key="validation_results", value=results)

    # Fail task if validation fails
    if not results["validation_success"]:
        raise ValueError(f"DuckDB validation failed: {results.get('error', 'Unknown error')}")

    return results


validate_duckdb_state = PythonOperator(
    task_id="validate_duckdb_state",
    python_callable=validate_duckdb_wrapper,
    dag=dag,
)


# Task 6: Generate load summary
def generate_load_summary(**context):
    """Generate comprehensive load summary from XCom data."""
    ti = context["task_instance"]

    # Pull stats from XCom
    load_stats = ti.xcom_pull(task_ids="load_normalised_data", key="load_stats")
    dq_results = ti.xcom_pull(task_ids="run_data_quality_checks", key="dq_results")
    # validation_results = ti.xcom_pull(task_ids="validate_duckdb_state", key="validation_results")

    print("\n" + "=" * 80)
    print("📊 LOADING PIPELINE SUMMARY")
    print("=" * 80)

    if load_stats:
        print("\n📥 DATA LOADING:")
        print(f"   Files processed: {load_stats['files_processed']}")
        print(f"   Successful loads: {load_stats['successful_loads']}")
        print(f"   Failed loads: {load_stats['failed_loads']}")
        print(f"   Total measurements: {load_stats['total_measurements']:,}")

        station_stats = load_stats.get("station_stats", {})
        print("\n🏢 STATION DIMENSION:")
        print(f"   Inserted: {station_stats.get('stations_inserted', 0)}")
        print(f"   Updated: {station_stats.get('stations_updated', 0)}")
        print(f"   Total: {station_stats.get('total_stations', 0)}")

    if dq_results:
        print("\n✅ DATA QUALITY:")
        print(f"   Integrity OK: {dq_results['integrity_ok']}")
        print(f"   Orphaned measurements: {dq_results['orphan_measurements']}")
        print(f"   Duplicate keys: {dq_results['duplicate_keys']}")
        print(f"   Null timestamps: {dq_results['null_timestamps']}")
        print(f"   Null stations: {dq_results['null_stations']}")

    print("\n🎯 PIPELINE STATUS: ✅ COMPLETE")
    print("\n📍 DuckDB Location: /opt/airflow/data/duckdb/cct_env.duckdb")
    print("📍 Mounted at (host): data/duckdb/cct_env.duckdb")

    print("\n🔄 Next Steps:")
    print("   1. Access JupyterLab viewer: http://localhost:8888")
    print("   2. Query database using notebooks or SQL")
    print("   3. Generate analytics reports")
    print("   4. Export data for downstream systems")

    print("\n" + "=" * 80)


log_summary = PythonOperator(
    task_id="log_load_summary",
    python_callable=generate_load_summary,
    dag=dag,
)


# Task dependencies
check_normalised_data >> init_duckdb_schema >> load_normalised_data
load_normalised_data >> run_dq_checks >> validate_duckdb_state >> log_summary
