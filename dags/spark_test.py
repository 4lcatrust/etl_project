from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def test_spark():
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("SparkTest") \
        .getOrCreate()
    
    # Create a simple dataframe
    data = [("Java", 20000), ("Python", 25000), ("Scala", 15000)]
    columns = ["language", "users_count"]
    df = spark.createDataFrame(data, columns)
    
    # Show the dataframe
    df.show()
    
    # Stop the SparkSession
    spark.stop()
    
    return "Spark test completed successfully!"

with DAG(
    'spark_test_dag',
    default_args=default_args,
    description='Test PySpark in Airflow',
    schedule_interval=None,
) as dag:
    
    test_spark_task = PythonOperator(
        task_id='test_spark',
        python_callable=test_spark,
    )