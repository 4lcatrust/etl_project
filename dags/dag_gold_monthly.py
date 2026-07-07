"""Gold monthly — monthly_transaction_summary (month × item_category) from silver monthly.

Runs `dbt run/test --select tag:gold_monthly`, scheduled off the silver-monthly Dataset.
Reads silver_monthly_sales (+ silver_item for the category); no upstream rebuild.

NB: keep the literal word "airflow" here — the DagBag safe-mode heuristic needs it.
"""
from module.dbt_dag import SILVER_MONTHLY, build_dbt_dag

dag_gold_monthly = build_dbt_dag(
    dag_id="dag_gold_monthly",
    select="tag:gold_monthly",
    schedule=[SILVER_MONTHLY],
    description="Gold monthly: monthly_transaction_summary from silver monthly",
    tags=["dbt", "gold", "monthly"],
)
