"""Builder for the per-layer/per-cadence dbt DAGs (silver/gold × daily/monthly).

Each DAG runs `dbt run` + `dbt test` for one dbt selection (by tag), in the isolated
/opt/dbt-venv, writing artifacts to /tmp (the project dir is a read-only bind mount).
A `done` marker emits an optional Dataset so the next layer can be scheduled off it.
"""
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from module.alerts import default_args
from module.utilities import get_airflow_variables

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
    "DBT_PACKAGES_DIR": "/opt/dbt-packages",
}

# Datasets that chain the medallion layers across the split DAGs.
SILVER_DAILY = Dataset("dbt://deso/silver_daily")
SILVER_MONTHLY = Dataset("dbt://deso/silver_monthly")

def build_dbt_dag(*, dag_id: str, select: str, schedule, description: str,
                  outlets: list = None, tags: list = None) -> DAG:
    """dbt run + test for one selection. `select` is a dbt node selector (e.g.
    `+tag:silver_daily`); `schedule` is a Dataset list or cron string."""
    with DAG(
        dag_id=dag_id,
        default_args=default_args(),
        schedule=schedule,
        catchup=False,
        tags=tags or ["dbt"],
        description=description,
    ) as dag:
        start = EmptyOperator(task_id="start")

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"{DBT_BIN} run --select '{select}' {DBT_FLAGS}",
            env=DBT_ENV,
            append_env=True,
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"{DBT_BIN} test --select '{select}' {DBT_FLAGS}",
            env=DBT_ENV,
            append_env=True,
        )

        done = EmptyOperator(task_id="done", outlets=outlets or [])

        start >> dbt_run >> dbt_test >> done

    return dag
