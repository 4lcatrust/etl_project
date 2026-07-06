"""Postgres bronze ingestion — thin wrapper over the config-driven DAG factory.

All table/schema/rule detail lives in config/postgres_table_list.yaml and
config/validation/postgres_*_vld.yaml. Runs in client deploy-mode (conn `spark`),
the proven Phase 3 path. Switch to cluster mode by passing conn_id="spark_cluster"
and spark_conf=cluster_spark_conf() once that path is preferred.

NB: this file must contain the literal word "airflow" — the DagBag safe-mode
heuristic (might_contain_dag) only parses files that mention both "dag" and
"airflow"; the factory import alone wouldn't trip it.
"""
from module.bronze_dag_factory import build_bronze_dag

dag_bronze_postgres = build_bronze_dag(
    source="postgres",
    jdbc_url_var="POSTGRES_JDBC_URL",
    user_var="POSTGRES_USER",
    password_var="POSTGRES_PASSWORD",
)
