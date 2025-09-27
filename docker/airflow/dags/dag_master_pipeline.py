"""
Master Pipeline Orchestrator DAG

Orchestrates the complete environmental sensors data pipeline from ingestion
through to analytics-ready DuckDB loading. Triggers all sub-DAGs in correct
dependency order.

Pipeline Flow:
1. INGESTION (Parallel): Fetch air quality + wind data from ArcGIS
2. STAGING (Parallel): Convert raw files to structured Parquet
3. NORMALIZATION (Parallel): Transform to tall fact schema
4. LOADING: Load into DuckDB star schema with data quality checks

Features:
- Full pipeline automation in single trigger
- Proper dependency management between phases
- Wait for completion before proceeding to next phase
- Comprehensive logging and error handling

Schedule: Manual trigger (@once)
Duration: ~10-15 minutes for full pipeline
Outputs: Analytics-ready DuckDB with 2.9M+ measurements
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.append("/opt/airflow/src")

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
    "master_pipeline_orchestrator",
    default_args=default_args,
    description="Master orchestrator for complete environmental sensors pipeline",
    schedule_interval="@once",  # Manual trigger only
    catchup=False,
    tags=["master", "orchestrator", "full-pipeline", "phase5"],
    doc_md=__doc__,
    max_active_runs=1,  # Prevent parallel runs
)

# =============================================================================
# PHASE 1: DATA INGESTION (Parallel)
# =============================================================================

trigger_fetch_air_quality = TriggerDagRunOperator(
    task_id="trigger_fetch_air_quality",
    trigger_dag_id="fetch_air_quality",
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

trigger_fetch_wind = TriggerDagRunOperator(
    task_id="trigger_fetch_wind",
    trigger_dag_id="fetch_wind",
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# =============================================================================
# PHASE 2: DATA STAGING (Parallel, after ingestion)
# =============================================================================

trigger_stage_air_quality = TriggerDagRunOperator(
    task_id="trigger_stage_air_quality",
    trigger_dag_id="stage_air_quality_data",
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

trigger_stage_wind = TriggerDagRunOperator(
    task_id="trigger_stage_wind",
    trigger_dag_id="stage_wind_data",
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# =============================================================================
# PHASE 3: DATA NORMALIZATION (Parallel, after staging)
# =============================================================================

trigger_normalise_air_quality = TriggerDagRunOperator(
    task_id="trigger_normalise_air_quality",
    trigger_dag_id="normalise_air_quality_data",
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

trigger_normalise_wind = TriggerDagRunOperator(
    task_id="trigger_normalise_wind",
    trigger_dag_id="normalise_wind_data",
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# =============================================================================
# PHASE 4: DUCKDB LOADING & ANALYTICS (After all normalization)
# =============================================================================

trigger_load_dq_publish = TriggerDagRunOperator(
    task_id="trigger_load_dq_publish",
    trigger_dag_id="load_dq_publish",
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# =============================================================================
# TASK DEPENDENCIES
# =============================================================================

# Phase 1: Ingestion (parallel)
# No dependencies - both can run in parallel

# Phase 2: Staging (parallel, after ingestion complete)
[trigger_fetch_air_quality, trigger_fetch_wind] >> trigger_stage_air_quality
[trigger_fetch_air_quality, trigger_fetch_wind] >> trigger_stage_wind

# Phase 3: Normalization (parallel, after respective staging)
trigger_stage_air_quality >> trigger_normalise_air_quality
trigger_stage_wind >> trigger_normalise_wind

# Phase 4: Loading (after all normalization complete)
[trigger_normalise_air_quality, trigger_normalise_wind] >> trigger_load_dq_publish
