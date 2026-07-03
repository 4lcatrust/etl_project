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

# The Spark job now lives on the Airflow worker filesystem (bind-mounted), so the
# client-mode driver can find it.
SPARK_JOB = "/opt/airflow/spark/jobs/daily_transaction_summary_extract_dq.py"

# Baked into the custom-airflow image (see Dockerfile.airflow) and shipped to
# executors via --jars — avoids the fragile ~558MB Ivy download at submit time.
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

    # modest shuffle/file sizes (also passed as script args)
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.files.maxRecordsPerFile": "500000",

    # object-store friendly commit settings
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    "spark.hadoop.fs.s3a.committer.name": "directory",
    "spark.hadoop.fs.s3a.fast.upload": "true",

    # client deploy mode: executors must be able to reach the driver, which runs
    # inside the airflow worker. Use the hyphenated alias (see docker-compose) —
    # Spark rejects RPC hostnames containing underscores.
    "spark.driver.host": "airflow-worker",
    "spark.driver.bindAddress": "0.0.0.0",

    # MinIO (S3A). NOTE: credentials still passed via conf here; hardened to a
    # Hadoop JCEKS credential provider in the follow-up cred step.
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.access.key": get_airflow_variables("MINIO_ACCESS_KEY"),
    "spark.hadoop.fs.s3a.secret.key": get_airflow_variables("MINIO_SECRET_KEY"),
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}

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
        application=SPARK_JOB,
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

    end = EmptyOperator(task_id="end")

    start >> extract_dq >> end
