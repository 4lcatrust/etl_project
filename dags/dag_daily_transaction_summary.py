from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, length, current_timestamp, current_date, date_diff, round, sum as spark_sum, lit, regexp_replace, trim, expr, count_distinct
from pyspark.sql.types import *
import clickhouse_connect
import pendulum
import time
import logging
import json
import os

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.today().add(days=-1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=10),
}

# Logger configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DAG_ID = "dag_daily_transaction_summary"
CURRENT_DATE_STR = pendulum.today().to_date_string()
TABLE_SOURCE = [
    ("public", "fct_transactions"),
    ("public", "dim_item"),
    ("public", "dim_time")
]

EXPECTED_SCHEMA = {
    "fct_transactions" : {
        "payment_key" : StringType(),
        "customer_key" : StringType(),
        "time_key" : StringType(),
        "item_key" : StringType(),
        "store_key" : StringType(),
        "quantity" : IntegerType(),
        "unit" : StringType(),
        "unit_price" : IntegerType(),
        "total_price" : IntegerType()
    },
    "dim_item" : {
        "item_key" : StringType(),
        "item_name" : StringType(),
        "desc" : StringType(),
        "unit_price" : FloatType(),
        "man_country" : StringType(),
        "supplier" : StringType(),
        "unit" : StringType()
    },
    "dim_time" : {
        "time_key" : StringType(),
        "date" : StringType(),
        "hour" : IntegerType(),
        "day" : IntegerType(),
        "week" : StringType(),
        "month" : IntegerType(),
        "quarter" : StringType(),
        "year" : IntegerType()
    }
}

# Airflow Variables for parameterization
AIRFLOW_PATH = Variable.get("LOCAL_AIRFLOW_PATH")
POSTGRES_JDBC_URL = Variable.get("POSTGRES_JDBC_URL")
POSTGRES_PASSWORD = Variable.get("POSTGRES_PASSWORD")
CLICKHOUSE_PASSWORD = Variable.get("CLICKHOUSE_PASSWORD")
SINK_TABLENAME = "daily_transaction_summary"

ch_client = clickhouse_connect.get_client(
    host = "127.0.0.1",
    port = 8123,
    username = "spark",
    password = CLICKHOUSE_PASSWORD
)

def create_spark_session(app_name: str):
    """
    Create a SparkSession with necessary configurations
    """
    clickhouse_jar = os.path.abspath(AIRFLOW_PATH + "driver/clickhouse-jdbc-0.4.6.jar")
    postgres_jar = os.path.abspath(AIRFLOW_PATH + "driver/postgresql-42.7.5.jar")
    jars = f"{clickhouse_jar},{postgres_jar}"
    logger.info(f"Creating SparkSession...")
    try:
        spark = SparkSession.builder.appName(app_name) \
            .config("spark.jars", jars) \
            .config("spark.driver.extraClassPath", jars) \
            .config("spark.executor.extraClassPath", jars) \
            .getOrCreate()
        logger.info(f"Create SparkSession success : {app_name}")
        return spark
    
    except Exception as e:
        logger.error(f"Error SparkSession : {str(e)}")
        raise
        

def stg_dq_checks(df, tablename):
    """
    Add validation for:
    - Schema compliance
    - Null checks for required fields
    - Business rule validation
    """
    start_time = time.perf_counter()
    actual_schema = dict([(field.name, str(field.dataType)) for field in df.schema.fields])
    expected_table_schema = EXPECTED_SCHEMA[tablename]
    quality_metrics = {}
    schema_validity = int(set(expected_table_schema.keys()) == set(actual_schema.keys())) * 100
    quality_metrics[f"{tablename}_schema_validity"] = schema_validity
    logger.info(f"Initiate data quality check for : {tablename}...")
    try:
        if tablename == "fct_transactions":
            checks = [
                "customer_key",
                "item_key",
                "time_key",
                "quantity",
                "unit_price",
                "total_price"
                ]
            for check in checks:
                quality_metrics[f"{tablename}_null_{check}"] = (
                    df.filter(col(check).isNotNull()).count() / df.count() * 100 if df.count() > 0 else 0
                )
            checks_negative = [
                "quantity",
                "unit_price",
                "total_price"]
            for check in checks_negative:
                quality_metrics[f"{tablename}_negative_{check}"] = (
                    df.filter(col(check) > 0).count() / df.count() * 100 if df.count() > 0 else 0
                )
        elif tablename == "dim_item":
            checks = [
                "item_key",
                "desc",
                "item_name",
                "unit_price"]
            for check in checks:
                quality_metrics[f"{tablename}_null_{check}"] = (
                    df.filter(col(check).isNotNull()).count() / df.count() * 100 if df.count() > 0 else 0
                )
            quality_metrics[f"{tablename}_negative_unit_price"] = (
                df.filter(col("unit_price") > 0).count() / df.count() * 100 if df.count() > 0 else 0
            )
        elif tablename == "dim_time":
            quality_metrics[f"{tablename}_null_time_key"] = (
                df.filter(col("time_key").isNotNull()).count() / df.count() * 100 if df.count() > 0 else 0
            )
        end_time = time.perf_counter()
        logger.info(f"Data quality extraction success in {end_time - start_time:.2f} : {tablename}...")
        return quality_metrics
    
    except Exception as e:
        logger.error(f"Error extracting data quality : {str(e)}")
        raise

def extract(table_source : dict):
    """
    Extract data from PostgreSQL and perform initial quality checks
    """
    start_time = time.perf_counter()
    dataframes = {}
    logger.info(f"Initiate data extraction from PostgreSQL...")
    spark = create_spark_session('PostgreSQL-Extract')
    try:
        for schemaname, tablename in table_source:
            df = spark.read \
                .format("jdbc") \
                .option("url", POSTGRES_JDBC_URL) \
                .option("dbtable", f"{schemaname}.{tablename}") \
                .option("user", "spark") \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .option("numPartitions", 8) \
                .load()
            dataframes[tablename] = df
            logger.info(f"Data extraction success : {tablename}")
    except Exception as e:
        logger.error(f"Error extracting data : {str(e)}")
        raise

    all_quality_metrics = {}
    for tablename, df in dataframes.items():
        quality_results = stg_dq_checks(df, tablename)
        all_quality_metrics[tablename] = quality_results
    
    dq_metrics_tablename = "data_quality.dq_metrics"
    ch_dq_ddl = f"""
    CREATE TABLE IF NOT EXISTS {dq_metrics_tablename} (
        dag_id String,
        table_name String,
        metric String,
        value Float32,
        processed_at DateTime DEFAULT NOW()    
    )
    ENGINE = MergeTree()
    ORDER BY (dag_id, table_name, processed_at)
    """
    try:
        ch_client.command(ch_dq_ddl)
        logger.info(f"Create DQ table success")
    except Exception as e:
        logger.error(f"Error creating DQ table: {str(e)}")
        raise

    for table, metrics in all_quality_metrics.items():
        for metric, val in metrics.items():
            ch_client.command(f"""
                INSERT INTO {dq_metrics_tablename} (
                    dag_id,
                    table_name,
                    metric,
                    value            
                )
                VALUES (
                    '{DAG_ID}',
                    '{table}',
                    '{metric}',
                    '{val}'
                );
            """
            )

    failed_checks = {}
    for table, metrics in all_quality_metrics.items():
        failed_metrics = {metric: val for metric, val in metrics.items() if val < 90}

        if failed_metrics:
            failed_checks[table] = failed_metrics

    if failed_checks:
        logger.error("Data quality check failed for the following tables:")
        for table, metrics in failed_checks.items():
            for metric, val in metrics.items():
                logger.error(f"{table}.{metric} = {val}% (Expected ≥ 90%)")
        raise Exception(f"Data quality check failed : {json.dumps(failed_checks, indent=4)}")
    else:
        logger.info("All data quality check passed!")
        logger.info("Initiate storing extracted data into staging phase...")
        try:
            for schemaname, tablename in table_source:
                staging_path_write = AIRFLOW_PATH + f"staging/{CURRENT_DATE_STR}.{schemaname}.{tablename}.parquet"
                dataframes[tablename].write.parquet(staging_path_write, mode="overwrite")
                end_time = time.perf_counter()
                logger.info(f"Success storing staging data in {end_time - start_time:.2f} : {tablename}")
            return all_quality_metrics
        except Exception as e:
            logger.error(f"Error storing staging data : {str(e)}")
            raise

def transform(table_source : dict):
    """
    Transform data using PySpark
    """
    spark = create_spark_session("Spark-Transform")
    dataframes = {}
    try:
        for schemaname, tablename in table_source:
            staging_path_write = AIRFLOW_PATH + f"staging/{CURRENT_DATE_STR}.{schemaname}.{tablename}.parquet"
            df = spark.read \
                .format("parquet") \
                .load(staging_path_write)
            dataframes[tablename] = df
            logger.info(f"Read from staging success : {tablename}")
    except Exception as e:
            logger.error(f"Error reading from staging : {str(e)}")
            raise
    
    logger.info(f"Initiate data transformation from staging...")
    start_time = time.perf_counter()
    try:
        fct_transactions = dataframes["fct_transactions"].alias("ft")
        dim_item = dataframes["dim_item"].alias("di")
        dim_time = dataframes["dim_time"].alias("dt")
        cte_df = (
            fct_transactions
                .join(dim_item, col("ft.item_key") == col("di.item_key"), "left")
                .join(dim_time, col("ft.time_key") == col("dt.time_key"), "left")
                .selectExpr(
                    "MAKE_DATE(year, month, day) AS transaction_date",
                    "quantity",
                    "total_price",
                    "customer_key",
                    "REGEXP_REPLACE(REGEXP_REPLACE(TRIM(desc), '^[a-z]. ', ''), ' - ', ' ') AS item_category"
                )
        )
        transformed_df = (
            cte_df
            .groupBy("transaction_date", "item_category")
            .agg(
                spark_sum("total_price").alias("total_transaction_value"),
                spark_sum("quantity").alias("total_goods_sold"),
                count_distinct("customer_key").alias("count_transacting_customer")
            )
        )
        transformed_path_write = AIRFLOW_PATH + f"transformed/{CURRENT_DATE_STR}.{SINK_TABLENAME}.parquet"
        transformed_df.write.parquet(transformed_path_write, mode="overwrite")
        end_time = time.perf_counter()
        logger.info(f"Data transformed succesfully in {end_time - start_time:.2f} : {SINK_TABLENAME}")
        logger.info(f"Transformed data succesfully saved to : {transformed_path_write}")
    except Exception as e:
        logger.error(f"Error transforming staging data : {str(e)}")
        raise

def load(sink_tablename : str):
    """
    Load transformed data from Parquet into ClickHouse database using JDBC.
    """
    spark = create_spark_session("Load-to-ClickHouse")
    stg_table = f"`intermediate`.{sink_tablename}_stg"
    ch_stg_ddl = f"""
    CREATE TABLE IF NOT EXISTS {stg_table} (
        transaction_date Date,
        item_category String,
        total_transaction_value Float64,
        total_goods_sold Int32,
        count_transacting_customer Int32,
        updated_at UInt32 DEFAULT toUnixTimestamp(now())
    )
    ENGINE = MergeTree()
    ORDER BY (transaction_date, item_category)
    """
    ch_client.command(ch_stg_ddl)

    prod_table = f"`datamart`.{sink_tablename}"
    ch_prod_ddl = f"""
    CREATE TABLE IF NOT EXISTS {prod_table} (
        transaction_date Date,
        item_category String,
        total_transaction_value Float64,
        total_goods_sold Int32,
        count_transacting_customer Int32,
        updated_at UInt32 DEFAULT toUnixTimestamp(now())
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (transaction_date, item_category);
    """
    ch_client.command(ch_prod_ddl)

    transformed_path = AIRFLOW_PATH + f"transformed/{CURRENT_DATE_STR}.{sink_tablename}.parquet"
    try:
        transformed_df = spark.read.parquet(transformed_path)
        logger.info(f"Read from transformed success : {sink_tablename}")
    except Exception as e:
        logger.error(f"Error reading from transformed : {str(e)}")
        raise

    logger.info(f"Initiate data loading from transformed...")
    start_time = time.perf_counter()
    try:
        transformed_df.write \
            .format("jdbc") \
            .option("url", "jdbc:clickhouse://127.0.0.1:8123/default") \
            .option("dbtable", stg_table) \
            .option("user", "spark") \
            .option("password", CLICKHOUSE_PASSWORD) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("batchsize", "50000") \
            .option("numPartitions", 8) \
            .mode("append") \
            .save()
        
        ch_prod_dml = f"""
        INSERT INTO {sink_tablename}
        SELECT * FROM {stg_table}
        """
        ch_client.command(ch_prod_dml)
        ch_prod_merge = f"""
        OPTIMIZE TABLE {prod_table} FINAL
        """
        ch_client.command(ch_prod_merge)
        end_time = time.perf_counter()
        logger.info(f"Data successfully loaded into ClickHouse table in {end_time - start_time:.2f} : {sink_tablename}")
    except Exception as e:
        logger.error(f"Error loading data into ClickHouse table : {str(e)}")
        raise

dag = DAG(
    dag_id = DAG_ID,
    default_args = default_args,
    description = f"ETL pipeline from PostgreSQL to ClickHouse for table {SINK_TABLENAME}",
    schedule_interval = pendulum.duration(days=1),
    catchup = False
)

extract_task = PythonOperator(
    task_id = "extract",
    python_callable = extract,
    op_kwargs = {
        "table_source" : TABLE_SOURCE
        },
    provide_context = True,
    dag = dag
)
extract_task.sla = pendulum.duration(minutes = 30)

transform_task = PythonOperator(
    task_id = "transform",
    python_callable = transform,
    op_kwargs = {
        "table_source" : TABLE_SOURCE
        },
    provide_context = True,
    dag = dag
)
transform_task.sla = pendulum.duration(minutes = 30)

load_task = PythonOperator(
    task_id = "load",
    python_callable = load,
    op_kwargs = {
        "sink_tablename" : SINK_TABLENAME
        },
    provide_context = True,
    dag = dag
)
load_task.sla = pendulum.duration(minutes = 30)

extract_task >> transform_task >> load_task