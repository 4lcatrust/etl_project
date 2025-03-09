from airflow.models import Variable

def get_airflow_variables(key : str):
    airflow_variables = {
        "AIRFLOW_PATH" : Variable.get("LOCAL_AIRFLOW_PATH"),
        "POSTGRES_JDBC_URL" : Variable.get("POSTGRES_JDBC_URL"),
        "POSTGRES_USER" : Variable.get("POSTGRES_USER"),
        "POSTGRES_PASSWORD" : Variable.get("POSTGRES_PASSWORD"),
        "CLICKHOUSE_CONN" : Variable.get("CLICKHOUSE_CONN"),
        "CLICKHOUSE_USER" : Variable.get("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD" : Variable.get("CLICKHOUSE_PASSWORD")
    }
    return airflow_variables[key]