from airflow.models import Variable

# Known Variable keys, kept as a documented allow-list. Resolution is lazy (only the
# requested key hits the metadata DB) so one source's missing Variables don't break
# another source's DAG, and callers don't pay N Variable.get() calls per invocation.
KNOWN_VARIABLES = {
    "LOCAL_AIRFLOW_PATH",
    "POSTGRES_JDBC_URL", "POSTGRES_USER", "POSTGRES_PASSWORD",
    "MYSQL_JDBC_URL", "MYSQL_USER", "MYSQL_PASSWORD",
    "CLICKHOUSE_CONN", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD",
    "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_ENDPOINT",
}


def get_airflow_variables(key: str):
    if key not in KNOWN_VARIABLES:
        raise KeyError(f"Unknown Airflow variable key: {key}")
    return Variable.get(key)
