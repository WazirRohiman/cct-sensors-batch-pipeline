from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="fetch_wind",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["phase2", "setup"],
):
    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")
    start >> done
