"""Factory that builds a config-driven bronze-ingestion DAG for one source.

One source (postgres, mysql, ...) -> one DAG. The DAG reads <source>_table_list.yaml,
then fans out a single **dynamically-mapped** SparkSubmitOperator (`.expand()`) over the
table list, one mapped instance per table. Each instance loads its own
<source>_<table>_vld.yaml and passes schema_json / validation_rules_json to the shared
Scala bronze-extract JAR (jobs.BronzeExtract), which writes iceberg.bronze/quarantine/audit.

One factory + dynamic task mapping. A `bronze_ready` marker emits a Dataset so the Phase E
silver/gold DAGs can be scheduled off it.
"""
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from module.config_loader import load_table_list, load_validation
from module.utilities import get_airflow_variables
BRONZE_EXTRACTOR_JAR = "/opt/extra-jars/bronze-extractor-assembly-0.1.0.jar"
EXTRA_JARS = ",".join([
    "/opt/extra-jars/hadoop-aws-3.4.1.jar",
    "/opt/extra-jars/bundle-2.24.6.jar",
    "/opt/extra-jars/postgresql-42.7.5.jar",
    "/opt/extra-jars/mysql-connector-j-8.4.0.jar",
    "/opt/extra-jars/iceberg-spark-runtime-4.0_2.13-1.10.2.jar",
])

def client_spark_conf() -> dict:
    """Client deploy-mode Spark conf (driver in airflow_worker). Mirrors the verified
    smoke-test conf: Iceberg catalog over MinIO + S3A + the SimplifyCasts guard."""
    return {
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

        # Spark 4.0's SimplifyCasts optimizer can emit an invalid plan on collation-aware
        # StringType casts; the job avoids no-op casts and we exclude the rule as a guard.
        "spark.sql.optimizer.excludedRules": "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts",

        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        "spark.hadoop.fs.s3a.committer.name": "directory",
        "spark.hadoop.fs.s3a.fast.upload": "true",

        # Client mode: the driver runs in airflow_worker; advertise its hyphenated alias
        # (Spark rejects the underscore in airflow_worker as an RPC hostname).
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

def bronze_dataset(db_name: str) -> Dataset:
    """The Dataset that a source's bronze refresh produces (Phase E schedules off it)."""
    return Dataset(f"iceberg://bronze/{db_name}")

def _build_arg_sets(source: str, db_name: str, schema_name_default: str, tables: list,
                    jdbc_url_var: str, user_var: str, password_var: str,
                    jdbc_driver: str, identifier_quote: str):
    """One application_args list per table, for the mapped extract task."""
    arg_sets = []
    for t in tables:
        table_name = t["table_name"]
        schema_name = t.get("schema_name", schema_name_default)
        vld = load_validation(source, table_name)
        arg_sets.append([
            "--jdbc_url", get_airflow_variables(jdbc_url_var),
            "--username", get_airflow_variables(user_var),
            "--password", get_airflow_variables(password_var),
            "--jdbc_driver", jdbc_driver,
            "--identifier_quote", identifier_quote,
            "--schema_name", schema_name,
            "--table_name", table_name,
            "--db_name", db_name,
            "--primary_key", vld["primary_key"],
            "--schema_json", json.dumps(vld["schema"]),
            "--validation_rules_json", json.dumps(vld.get("validation_rules") or []),
            "--ingestion_date", "{{ ds }}",
            "--catalog", "iceberg",
        ])
    return arg_sets

def build_bronze_dag(*, source: str, jdbc_url_var: str, user_var: str, password_var: str,
                     db_name: str = None, jdbc_driver: str = "org.postgresql.Driver",
                     identifier_quote: str = '"', conn_id: str = "spark",
                     spark_conf: dict = None, schedule=None, tags: list = None) -> DAG:
    """Build the bronze DAG for `source` from config/<source>_table_list.yaml.

    jdbc_driver / identifier_quote default to Postgres; MySQL passes
    com.mysql.cj.jdbc.Driver and a backtick.
    """
    table_list = load_table_list(source)
    db_name = db_name or table_list.get("db_name", source)
    schema_name_default = table_list.get("schema_name_default", "public")
    tables = table_list["tables"]

    conf = spark_conf if spark_conf is not None else client_spark_conf()
    arg_sets = _build_arg_sets(
        source, db_name, schema_name_default, tables, jdbc_url_var, user_var, password_var,
        jdbc_driver, identifier_quote,
    )

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id=f"dag_bronze_{source}",
        default_args=default_args,
        schedule=schedule,
        catchup=False,
        tags=tags or ["bronze", source, "config-driven"],
        description=f"Config-driven bronze ingestion for the {source} source ({len(tables)} tables)",
    ) as dag:
        start = EmptyOperator(task_id="start")

        extract = SparkSubmitOperator.partial(
            task_id="bronze_extract",
            conn_id=conn_id,
            pool="spark",  # cap concurrent Spark drivers (memory); see the spark pool
            application=BRONZE_EXTRACTOR_JAR,
            java_class="jobs.BronzeExtract",
            name=f"bronze-extract-{source}",
            jars=EXTRA_JARS,
            conf=conf,
            verbose=True,
        ).expand(application_args=arg_sets)

        bronze_ready = EmptyOperator(task_id="bronze_ready", outlets=[bronze_dataset(db_name)])

        start >> extract >> bronze_ready

    return dag
