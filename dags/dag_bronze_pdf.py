"""PDF bronze ingestion via docling (Phase G).

Two steps into the same lake as every other source:
  1. parse_pdf      — docling (isolated /opt/docling-venv, OCR off) extracts the PDF's table
                      and lands it as NDJSON on MinIO. Runs in the `docling` pool (1 slot).
  2. bronze_extract — the shared Scala BronzeExtract reads that file (--input_path), validates
                      against the vld rules, and writes iceberg.bronze/quarantine/audit.
Then bronze_ready emits Dataset("iceberg://bronze/pdf") for downstream (dbt) layers.

docling runs via its own venv python (BashOperator) so torch never touches the Airflow env;
the `docling` pool + the worker mem_limit keep it from competing with Spark for RAM.

NB: keep the literal word "airflow" here — the DagBag safe-mode heuristic needs it.
"""
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from module.alerts import default_args
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
_table_list = load_table_list("pdf")
_db_name = _table_list["db_name"]
_table = _table_list["tables"][0]
_table_name = _table["table_name"]
_pdf_path = _table["pdf_path"]
_vld = load_validation("pdf", _table_name)
_columns = ",".join(c["name"] for c in _vld["schema"])

# Landing location on MinIO (staging bucket). {{ ds }} stays literal and renders at run time.
BUCKET = "staging"
OBJECT_KEY = f"pdf/{_table_name}/ingestion_date={{{{ ds }}}}/data.json"
INPUT_PATH = f"s3a://{BUCKET}/{OBJECT_KEY}"

DOCLING_PY = "/opt/docling-venv/bin/python"
PARSE_SCRIPT = "/opt/airflow/docling/parse_pdf.py"
PDF_ENV = {
    "MINIO_ENDPOINT": get_airflow_variables("MINIO_ENDPOINT"),
    "MINIO_ACCESS_KEY": get_airflow_variables("MINIO_ACCESS_KEY"),
    "MINIO_SECRET_KEY": get_airflow_variables("MINIO_SECRET_KEY"),
}

with DAG(
    dag_id="dag_bronze_pdf",
    default_args=default_args(),
    schedule=None,
    catchup=False,
    tags=["bronze", "pdf", "docling"],
    description="Phase G: PDF (docling) -> MinIO -> shared BronzeExtract -> Iceberg",
) as dag:

    start = EmptyOperator(task_id="start")

    parse_pdf = BashOperator(
        task_id="parse_pdf",
        pool="docling",  # 1-slot pool: keep the heavy docling parse off the Spark budget
        bash_command=(
            f"{DOCLING_PY} {PARSE_SCRIPT} "
            f"--pdf_path {_pdf_path} "
            f"--bucket {BUCKET} "
            f"--key '{OBJECT_KEY}' "
            f"--columns '{_columns}'"
        ),
        env=PDF_ENV,
        append_env=True,
    )

    bronze_extract = SparkSubmitOperator(
        task_id="bronze_extract",
        conn_id="spark",
        pool="spark",
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

    start >> parse_pdf >> bronze_extract >> quality_gate >> bronze_ready
