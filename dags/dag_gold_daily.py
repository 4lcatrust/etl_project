"""Gold daily — daily_transaction_summary (date × item_category) from the daily silver.

Runs `dbt run/test --select tag:gold_daily`, scheduled off the silver-daily Dataset so it
builds after silver refreshes. Reads the already-built silver tables (no upstream rebuild).

NB: keep the literal word "airflow" here — the DagBag safe-mode heuristic needs it.
"""
from module.dbt_dag import SILVER_DAILY, build_dbt_dag

dag_gold_daily = build_dbt_dag(
    dag_id="dag_gold_daily",
    select="tag:gold_daily",
    schedule=[SILVER_DAILY],
    description="Gold daily: daily_transaction_summary from silver",
    tags=["dbt", "gold", "daily"],
)
