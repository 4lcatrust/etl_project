from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
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

# Spark jobs are bind-mounted onto the Airflow worker so the client-mode driver finds them.
EXTRACT_JOB = "/opt/airflow/spark/jobs/daily_transaction_summary_extract_dq.py"
TRANSFORM_JOB = "/opt/airflow/spark/jobs/daily_transaction_summary_transform.py"
SINK_TABLE = "daily_transaction_summary"

# Baked into the custom-airflow image (see Dockerfile.airflow), shipped to executors via --jars.
EXTRA_JARS = ",".join([
    "/opt/extra-jars/hadoop-aws-3.4.1.jar",
    "/opt/extra-jars/bundle-2.24.6.jar",
    "/opt/extra-jars/postgresql-42.7.5.jar",
])

SPARK_CONF = {
    "spark.pyspark.python": "python3",

    # resources (stable on small cluster)
    "spark.executor.instances": "1",
    "spark.executor.cores": "1",
    "spark.executor.memory": "2g",
    "spark.executor.memoryOverhead": "512m",
    "spark.driver.memory": "2g",

    # stability
    "spark.network.timeout": "600s",
    "spark.executor.heartbeatInterval": "60s",

    # modest shuffle/file sizes
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.files.maxRecordsPerFile": "500000",

    # object-store friendly commit settings
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    "spark.hadoop.fs.s3a.committer.name": "directory",
    "spark.hadoop.fs.s3a.fast.upload": "true",

    # client deploy mode: executors reach the driver via the hyphenated worker alias
    # (Spark rejects RPC hostnames with underscores).
    "spark.driver.host": "airflow-worker",
    "spark.driver.bindAddress": "0.0.0.0",

    # MinIO (S3A). Creds via conf here; JCEKS hardening is a follow-up.
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.access.key": get_airflow_variables("MINIO_ACCESS_KEY"),
    "spark.hadoop.fs.s3a.secret.key": get_airflow_variables("MINIO_SECRET_KEY"),
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}


def load_to_clickhouse(**context):
    """Ingest the gold Parquet for this run's date straight from MinIO into the
    datamart ReplacingMergeTree via ClickHouse's s3() table function, then dedup."""
    import clickhouse_connect

    exec_date = context["ds"]
    client = clickhouse_connect.get_client(
        host=get_airflow_variables("CLICKHOUSE_CONN"),
        port=8123,
        username=get_airflow_variables("CLICKHOUSE_USER"),
        password=get_airflow_variables("CLICKHOUSE_PASSWORD"),
    )
    s3_glob = (
        f"http://minio:9000/transformed/{SINK_TABLE}/ingestion_date={exec_date}/*.parquet"
    )
    client.command(
        f"""
        INSERT INTO datamart.{SINK_TABLE}
            (transaction_date, item_category, total_transaction_value,
             total_goods_sold, count_transacting_customer)
        SELECT transaction_date, item_category, total_transaction_value,
               total_goods_sold, count_transacting_customer
        FROM s3({{url:String}}, {{key:String}}, {{secret:String}}, 'Parquet')
        """,
        parameters={
            "url": s3_glob,
            "key": get_airflow_variables("MINIO_ACCESS_KEY"),
            "secret": get_airflow_variables("MINIO_SECRET_KEY"),
        },
    )
    client.command(f"OPTIMIZE TABLE datamart.{SINK_TABLE} FINAL")


with DAG(
    dag_id="dag_daily_transaction_summary_extract_dq",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_dq = SparkSubmitOperator(
        task_id="extract_dq",
        conn_id="spark",
        application=EXTRACT_JOB,
        name="daily_transaction_summary_extract_and_dq",
        jars=EXTRA_JARS,
        conf=SPARK_CONF,
        application_args=[
            "--pg_url", get_airflow_variables("POSTGRES_JDBC_URL"),
            "--pg_user", get_airflow_variables("POSTGRES_USER"),
            "--pg_pass", get_airflow_variables("POSTGRES_PASSWORD"),
            "--exec_date", "{{ ds }}",
            "--staging_path", "s3a://staging",
            "--dq_path", "s3a://staging-dq",
            "--shuffle_partitions", "8",
            "--records_per_file", "500000",
            "--coalesce_out", "1",
        ],
        verbose=False,
    )

    transform = SparkSubmitOperator(
        task_id="transform",
        conn_id="spark",
        application=TRANSFORM_JOB,
        name="daily_transaction_summary_transform",
        jars=EXTRA_JARS,
        conf=SPARK_CONF,
        application_args=[
            "--exec_date", "{{ ds }}",
            "--staging_path", "s3a://staging",
            "--transformed_path", "s3a://transformed",
            "--shuffle_partitions", "8",
            "--coalesce_out", "1",
        ],
        verbose=False,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_to_clickhouse,
    )

    end = EmptyOperator(task_id="end")

    start >> extract_dq >> transform >> load >> end
