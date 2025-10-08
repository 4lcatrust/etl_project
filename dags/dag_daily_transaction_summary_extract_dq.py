from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator
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

with DAG(
    dag_id="dag_daily_transaction_summary_extract_dq",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "livy"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_dq = LivyOperator(
        task_id="extract_dq",
        livy_conn_id="livy",
        file="/opt/bitnami/spark/jobs/daily_transaction_summary_extract_dq.py",
        # pass runtime args to the script
        args=[
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
        conf={
            "spark.app.name": "daily_transaction_summary_extract_and_dq",
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

            # object-store friendly commit settings (IMPORTANT)
            "mapreduce.fileoutputcommitter.algorithm.version": "2",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
            "spark.hadoop.fs.s3a.committer.name": "directory",
            "spark.sql.parquet.output.committer.class": "org.apache.parquet.hadoop.ParquetOutputCommitter",
            "spark.hadoop.fs.s3a.fast.upload": "true",

            # local FS defaults
            "spark.hadoop.fs.defaultFS": "file:///",
            "spark.sql.warehouse.dir": "file:/tmp/spark-warehouse",

            # hadoop simple mode
            "spark.hadoop.security.authentication": "simple",
            "spark.hadoop.security.authorization": "false",

            # MinIO (S3A)
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.access.key": get_airflow_variables("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": get_airflow_variables("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

            # JDBC
            "spark.jars.packages": "org.postgresql:postgresql:42.7.4",
        },
        polling_interval=60,  # keep gentle on the webserver; you can lower later
    )

    end = EmptyOperator(task_id="end")

    start >> extract_dq >> end
