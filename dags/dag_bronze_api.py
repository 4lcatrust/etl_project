"""REST API bronze ingestion (Phase F).

Two steps into the same lake as the JDBC sources:
  1. fetch_products  — Python connector pulls the paginated JSON API and lands NDJSON on MinIO.
  2. bronze_extract  — the shared Scala BronzeExtract reads that file (--input_path), validates
                       against the vld rules, and writes iceberg.bronze/quarantine/audit.
Then a bronze_ready marker emits Dataset("iceberg://bronze/api") for downstream (dbt) layers.

NB: keep the literal word "airflow" here — the DagBag safe-mode heuristic needs it.
"""
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from module.alerts import default_args
from module.api_connector import fetch_and_land
from module.bronze_dag_factory import (
    BRONZE_EXTRACTOR_JAR,
    EXTRA_JARS,
    bronze_dataset,
    check_bronze_quality,
    client_spark_conf,
)
from module.config_loader import load_table_list, load_validation
from module.utilities import get_airflow_variables

# Source config
_table_list = load_table_list("api")
_base_url = _table_list["base_url"]
_db_name = _table_list["db_name"]
_table = _table_list["tables"][0]
_endpoint = _table["endpoint"]
_table_name = _table["table_name"]
_vld = load_validation("api", _table_name)

# Landing location on MinIO (staging bucket). {{ ds }} stays literal here and is rendered
# at task run time (op_kwargs and application_args are templated), so both steps agree.
BUCKET = "staging"
OBJECT_KEY = f"api/{_table_name}/ingestion_date={{{{ ds }}}}/data.json"
INPUT_PATH = f"s3a://{BUCKET}/{OBJECT_KEY}"

with DAG(
    dag_id="dag_bronze_api",
    default_args=default_args(),
    schedule=None,
    catchup=False,
    tags=["bronze", "api", "rest"],
    description="Phase F: REST API source -> MinIO -> shared BronzeExtract -> Iceberg",
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_products = PythonOperator(
        task_id="fetch_products",
        python_callable=fetch_and_land,
        op_kwargs={
            "api_url": f"{_base_url.rstrip('/')}/{_endpoint}",
            "bucket": BUCKET,
            "key": OBJECT_KEY,
            "minio_endpoint": get_airflow_variables("MINIO_ENDPOINT"),
            "access_key": get_airflow_variables("MINIO_ACCESS_KEY"),
            "secret_key": get_airflow_variables("MINIO_SECRET_KEY"),
        },
    )

    bronze_extract = SparkSubmitOperator(
        task_id="bronze_extract",
        conn_id="spark",
        pool="spark",  # cap concurrent Spark drivers (memory)
        application=BRONZE_EXTRACTOR_JAR,
        java_class="jobs.BronzeExtract",
        name=f"bronze-extract-{_db_name}-{_table_name}",
        jars=EXTRA_JARS,
        conf=client_spark_conf(),
        application_args=[
            "--input_path", INPUT_PATH,
            "--table_name", _table_name,
            "--db_name", _db_name,
            "--primary_key", _vld["primary_key"],
            "--schema_json", json.dumps(_vld["schema"]),
            "--validation_rules_json", json.dumps(_vld.get("validation_rules") or []),
            "--ingestion_date", "{{ ds }}",
            "--catalog", "iceberg",
        ],
        verbose=True,
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=check_bronze_quality,
        op_kwargs={"db_name": _db_name, "table_name": _table_name, "ingestion_date": "{{ ds }}"},
    )

    bronze_ready = EmptyOperator(task_id="bronze_ready", outlets=[bronze_dataset(_db_name)])

    start >> fetch_products >> bronze_extract >> quality_gate >> bronze_ready
