"""Silver daily — bronze views + current-state silver tables (item/time/transactions).

Runs `dbt run/test --select +tag:silver_daily` (the `+` builds the upstream bronze views
too). Scheduled off the Postgres bronze Dataset; emits the silver-daily Dataset for the
gold-daily and silver-monthly DAGs.

NB: keep the literal word "airflow" here — the DagBag safe-mode heuristic needs it.
"""
from module.bronze_dag_factory import bronze_dataset
from module.dbt_dag import SILVER_DAILY, build_dbt_dag

dag_silver_daily = build_dbt_dag(
    dag_id="dag_silver_daily",
    select="+tag:silver_daily",
    schedule=[bronze_dataset("postgres")],
    outlets=[SILVER_DAILY],
    description="Silver daily: bronze views + current-state silver tables",
    tags=["dbt", "silver", "daily"],
)
