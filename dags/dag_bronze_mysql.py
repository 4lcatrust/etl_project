"""MySQL bronze ingestion — same factory + Scala JAR as Postgres, different source.

Proves multi-source ingestion: only the driver, identifier-quote char, and config
YAMLs differ. Client deploy-mode (conn `spark`).

NB: keep the literal word "airflow" here — the DagBag safe-mode heuristic
(might_contain_dag) only parses files mentioning both "dag" and "airflow".
"""
from module.bronze_dag_factory import build_bronze_dag

dag_bronze_mysql = build_bronze_dag(
    source="mysql",
    jdbc_url_var="MYSQL_JDBC_URL",
    user_var="MYSQL_USER",
    password_var="MYSQL_PASSWORD",
    jdbc_driver="com.mysql.cj.jdbc.Driver",
    identifier_quote="`",
)
