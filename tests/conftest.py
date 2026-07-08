"""Pytest bootstrap for the airflow-marked tests.

Points Airflow at a throwaway AIRFLOW_HOME + sqlite metastore and injects env-var Airflow
Variables (AIRFLOW_VAR_<KEY>) so the DAGs parse without a live database or real secrets --
`get_airflow_variables()` resolves these at DAG-parse time. This runs at import (before any
airflow import) so the config is in place first.

The pure-Python tests (test_config_contract) don't import airflow and ignore all of this.
"""
import os
import sys
import tempfile

_AIRFLOW_HOME = os.path.join(tempfile.gettempdir(), "deso_airflow_test_home")
os.makedirs(_AIRFLOW_HOME, exist_ok=True)
os.environ.setdefault("AIRFLOW_HOME", _AIRFLOW_HOME)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", f"sqlite:///{_AIRFLOW_HOME}/airflow.db"
)

# Dummy values for every key in module.utilities.KNOWN_VARIABLES, exposed via the
# env-var secrets backend so DAG parsing never touches the metastore or real secrets.
_DUMMY_VARS = {
    "LOCAL_AIRFLOW_PATH": "/opt/airflow",
    "POSTGRES_JDBC_URL": "jdbc:postgresql://postgres:5432/deso",
    "POSTGRES_USER": "deso",
    "POSTGRES_PASSWORD": "deso",
    "MYSQL_JDBC_URL": "jdbc:mysql://mysql:3306/deso",
    "MYSQL_USER": "deso",
    "MYSQL_PASSWORD": "deso",
    "CLICKHOUSE_CONN": "clickhouse://default@clickhouse:8123/default",
    "CLICKHOUSE_USER": "default",
    "CLICKHOUSE_PASSWORD": "clickhouse",
    "MINIO_ACCESS_KEY": "minio",
    "MINIO_SECRET_KEY": "minio123",
    "MINIO_ENDPOINT": "http://minio:9000",
}
for _k, _v in _DUMMY_VARS.items():
    os.environ.setdefault(f"AIRFLOW_VAR_{_k}", _v)

# Make `import module.*` resolve as it does inside Airflow (dags/ on sys.path).
_DAGS = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dags"))
if _DAGS not in sys.path:
    sys.path.insert(0, _DAGS)
