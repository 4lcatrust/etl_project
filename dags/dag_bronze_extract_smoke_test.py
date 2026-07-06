import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from module.utilities import get_airflow_variables

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Client-mode (B1) verification of the Scala bronze-extract job: the driver runs in
# airflow_worker, which has the assembly jar + dependency jars baked in at /opt/extra-jars.
BRONZE_EXTRACTOR_JAR = "/opt/extra-jars/bronze-extractor-assembly-0.1.0.jar"

EXTRA_JARS = ",".join([
    "/opt/extra-jars/hadoop-aws-3.4.1.jar",
    "/opt/extra-jars/bundle-2.24.6.jar",
    "/opt/extra-jars/postgresql-42.7.5.jar",
    "/opt/extra-jars/iceberg-spark-runtime-4.0_2.13-1.10.2.jar",
])

# dim_item schema (see postgres-init.sql). "desc" needs no special handling here since
# Scala reads it as a plain column name via the --query override below.
SCHEMA = [
    {"name": "item_key", "type": "string"},
    {"name": "item_name", "type": "string"},
    {"name": "desc", "type": "string"},
    {"name": "unit_price", "type": "float64"},
    {"name": "man_country", "type": "string"},
    {"name": "supplier", "type": "string"},
    {"name": "unit", "type": "string"},
]

VALIDATION_RULES = [
    {"id": "not_null_item_key", "rule": "not_null", "columns": ["item_key"]},
    {"id": "positive_unit_price", "rule": "positive_number", "columns": ["unit_price"]},
]

# Real dim_item data has no bad rows, so union in one synthetic bad row (negative
# unit_price) to prove the quarantine path works, without mutating the source table.
SMOKE_QUERY = """
SELECT item_key, item_name, "desc", unit_price, man_country, supplier, unit
FROM public.dim_item WHERE 1=1
UNION ALL
SELECT 'SMOKE_TEST_BAD_ROW', 'Bad Item', 'seeded bad row for validation test', -50.0, 'XX', 'Test', 'ea'
"""

SPARK_CONF = {
    "spark.pyspark.python": "python3",

    "spark.executor.instances": "1",
    "spark.executor.cores": "1",
    "spark.executor.memory": "2g",
    "spark.executor.memoryOverhead": "512m",
    "spark.driver.memory": "2g",

    "spark.network.timeout": "600s",
    "spark.executor.heartbeatInterval": "60s",

    "spark.sql.shuffle.partitions": "8",
    "spark.sql.files.maxRecordsPerFile": "500000",

    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    "spark.hadoop.fs.s3a.committer.name": "directory",
    "spark.hadoop.fs.s3a.fast.upload": "true",

    "spark.driver.host": "airflow-worker",
    "spark.driver.bindAddress": "0.0.0.0",

    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.access.key": get_airflow_variables("MINIO_ACCESS_KEY"),
    "spark.hadoop.fs.s3a.secret.key": get_airflow_variables("MINIO_SECRET_KEY"),
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hadoop",
    "spark.sql.catalog.iceberg.warehouse": "s3a://iceberg-warehouse",
    "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.iceberg.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.iceberg.s3.access-key-id": get_airflow_variables("MINIO_ACCESS_KEY"),
    "spark.sql.catalog.iceberg.s3.secret-access-key": get_airflow_variables("MINIO_SECRET_KEY"),
    "spark.sql.catalog.iceberg.s3.path-style-access": "true",
    "spark.sql.catalog.iceberg.client.region": "us-east-1",
}

with DAG(
    dag_id="dag_bronze_extract_smoke_test",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["scala", "bronze", "smoke-test"],
    description="Phase B1: verify the Scala bronze-extract JAR (client mode) end to end",
) as dag:

    start = EmptyOperator(task_id="start")

    bronze_extract = SparkSubmitOperator(
        task_id="bronze_extract_dim_item",
        conn_id="spark",
        application=BRONZE_EXTRACTOR_JAR,
        java_class="jobs.BronzeExtract",
        name="bronze-extract-smoke-test",
        jars=EXTRA_JARS,
        conf=SPARK_CONF,
        application_args=[
            "--jdbc_url", get_airflow_variables("POSTGRES_JDBC_URL"),
            "--username", get_airflow_variables("POSTGRES_USER"),
            "--password", get_airflow_variables("POSTGRES_PASSWORD"),
            "--schema_name", "public",
            "--table_name", "dim_item",
            "--db_name", "postgres",
            "--primary_key", "item_key",
            "--schema_json", json.dumps(SCHEMA),
            "--validation_rules_json", json.dumps(VALIDATION_RULES),
            "--query", SMOKE_QUERY,
            "--ingestion_date", "{{ ds }}",
            "--catalog", "iceberg",
        ],
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> bronze_extract >> end
