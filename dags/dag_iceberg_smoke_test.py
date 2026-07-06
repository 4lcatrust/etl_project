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

SMOKE_TEST_JOB = "/opt/airflow/spark/jobs/iceberg_smoke_test.py"

EXTRA_JARS = ",".join([
    "/opt/extra-jars/hadoop-aws-3.4.1.jar",
    "/opt/extra-jars/bundle-2.24.6.jar",
    "/opt/extra-jars/iceberg-spark-runtime-4.0_2.13-1.10.2.jar",
])

SPARK_CONF = {
    "spark.pyspark.python": "python3",

    # Resources (match existing pattern)
    "spark.executor.instances": "1",
    "spark.executor.cores": "1",
    "spark.executor.memory": "2g",
    "spark.executor.memoryOverhead": "512m",
    "spark.driver.memory": "2g",

    # Stability
    "spark.network.timeout": "600s",
    "spark.executor.heartbeatInterval": "60s",

    # Modest shuffle/file sizes
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.files.maxRecordsPerFile": "500000",

    # Object-store friendly commit settings
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    "spark.hadoop.fs.s3a.committer.name": "directory",
    "spark.hadoop.fs.s3a.fast.upload": "true",

    # Client deploy mode
    "spark.driver.host": "airflow-worker",
    "spark.driver.bindAddress": "0.0.0.0",

    # MinIO (S3A)
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.access.key": get_airflow_variables("MINIO_ACCESS_KEY"),
    "spark.hadoop.fs.s3a.secret.key": get_airflow_variables("MINIO_SECRET_KEY"),
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

    # Iceberg catalog (Hadoop catalog over MinIO)
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hadoop",
    "spark.sql.catalog.iceberg.warehouse": "s3a://iceberg-warehouse",
    "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    # S3FileIO uses the AWS SDK directly (NOT fs.s3a.*), so it needs its own MinIO
    # endpoint/creds/path-style AND a region (the SDK fails region resolution otherwise).
    "spark.sql.catalog.iceberg.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.iceberg.s3.access-key-id": get_airflow_variables("MINIO_ACCESS_KEY"),
    "spark.sql.catalog.iceberg.s3.secret-access-key": get_airflow_variables("MINIO_SECRET_KEY"),
    "spark.sql.catalog.iceberg.s3.path-style-access": "true",
    "spark.sql.catalog.iceberg.client.region": "us-east-1",
}

with DAG(
    dag_id="dag_iceberg_smoke_test",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["iceberg", "smoke-test"],
    description="Phase A: Verify Iceberg foundation on Spark 4.0.3 + ClickHouse 24.8 read capability",
) as dag:

    start = EmptyOperator(task_id="start")

    smoke_test = SparkSubmitOperator(
        task_id="iceberg_smoke_test",
        conn_id="spark",
        application=SMOKE_TEST_JOB,
        name="iceberg-smoke-test",
        jars=EXTRA_JARS,
        conf=SPARK_CONF,
        application_args=[
            "--warehouse_path", "s3a://iceberg-warehouse",
            "--exec_date", "{{ ds }}",
        ],
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> smoke_test >> end
