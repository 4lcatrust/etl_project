from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator
from datetime import datetime, timedelta
from module.utilities import get_airflow_variables


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_daily_transaction_summary_extract_dq',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    start_task = DummyOperator(
        task_id='start',
        dag=dag
    )

    extract_dq_task = LivyOperator(
        task_id="extract_dq",
        livy_conn_id="livy",
        file="/opt/bitnami/spark/jobs/daily_transaction_summary_extract_dq.py",
        conf = {
        "spark.app.name": "daily_transaction_summary_extract_and_dq",
        "spark.pyspark.python": "python3",

        # keep local FS defaults to avoid HDFS touches during submit
        "spark.hadoop.fs.defaultFS": "file:///",
        "spark.sql.warehouse.dir": "file:/tmp/spark-warehouse",

        # hadoop “simple” mode (still useful once app starts)
        "spark.hadoop.security.authentication": "simple",
        "spark.hadoop.security.authorization": "false",

        # MinIO
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.access.key": get_airflow_variables("MINIO_ACCESS_KEY"),
        "spark.hadoop.fs.s3a.secret.key": get_airflow_variables("MINIO_SECRET_KEY"),
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        # JDBC jar via Maven (no extraClassPath/globs)
        "spark.jars.packages": "org.postgresql:postgresql:42.7.4",

        # resources
        "spark.driver.memory": "1g",
        "spark.executor.memory": "1g",
        "spark.executor.cores": "1",
        "spark.executor.instances": "1",
        "spark.yarn.submit.waitAppCompletion": "true",
        "spark.livy.server.interactive": "false"
        },
        args=[
            "--pg_url", get_airflow_variables("POSTGRES_JDBC_URL"),
            "--pg_user", get_airflow_variables("POSTGRES_USER"),
            "--pg_pass", get_airflow_variables("POSTGRES_PASSWORD"),
            "--exec_date", "{{ ds }}",
        ],
        polling_interval=20
    )

    end_task = DummyOperator(
        task_id='end',
        dag=dag
    )

    start_task >> extract_dq_task >> end_task