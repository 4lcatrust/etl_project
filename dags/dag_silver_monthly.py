"""Silver monthly — monthly per-item sales rollup (silver_monthly_sales).

Runs `dbt run/test --select tag:silver_monthly` on a monthly cron. Reads the current-state
daily silver tables (built by dag_silver_daily), so those must exist — true in steady state
where silver daily runs off every bronze refresh. Emits the silver-monthly Dataset.

NB: keep the literal word "airflow" here — the DagBag safe-mode heuristic needs it.
"""
from module.dbt_dag import SILVER_MONTHLY, build_dbt_dag

dag_silver_monthly = build_dbt_dag(
    dag_id="dag_silver_monthly",
    select="tag:silver_monthly",
    schedule="@monthly",
    outlets=[SILVER_MONTHLY],
    description="Silver monthly: monthly per-item sales rollup",
    tags=["dbt", "silver", "monthly"],
)
