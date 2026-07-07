"""Silver + gold transforms via dbt-clickhouse over the Iceberg lake (Phase E).

dbt reads Iceberg bronze through ClickHouse's iceberg() function (bronze views),
deduplicates append-only bronze into current-state silver tables, and builds the gold
datamart (gold.daily_transaction_summary) — replacing the legacy Spark transform + Python
load. Scheduled off the Postgres bronze Dataset so it runs after each bronze refresh.

dbt lives in an isolated venv (/opt/dbt-venv) to avoid dependency clashes with Airflow.
Artifacts go to /tmp (the project dir is a read-only bind mount for the airflow user).
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from module.utilities import get_airflow_variables
from module.bronze_dag_factory import bronze_dataset

DBT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/opt/dbt-venv/bin/dbt"
DBT_FLAGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target-path /tmp/dbt/target --log-path /tmp/dbt/logs"

# Credentials for the dbt process: ClickHouse (profile) + MinIO (iceberg() reads in the
# bronze views). Resolved from Airflow Variables at parse time, matching the repo pattern.
DBT_ENV = {
    "CLICKHOUSE_USER": get_airflow_variables("CLICKHOUSE_USER"),
    "CLICKHOUSE_PASSWORD": get_airflow_variables("CLICKHOUSE_PASSWORD"),
    "MINIO_ACCESS_KEY": get_airflow_variables("MINIO_ACCESS_KEY"),
    "MINIO_SECRET_KEY": get_airflow_variables("MINIO_SECRET_KEY"),
    "MINIO_ENDPOINT": "http://minio:9000",
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_silver_gold_dbt",
    default_args=default_args,
    schedule=[bronze_dataset("postgres")],
    catchup=False,
    tags=["dbt", "silver", "gold", "clickhouse"],
    description="Phase E: dbt silver/gold over Iceberg bronze (dbt-clickhouse)",
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_BIN} run {DBT_FLAGS}",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BIN} test {DBT_FLAGS}",
        env=DBT_ENV,
        append_env=True,
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_run >> dbt_test >> end
