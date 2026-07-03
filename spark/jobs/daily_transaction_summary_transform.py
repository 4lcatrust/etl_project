"""
Transform step: read the partitioned staging Parquet (fct_transactions, dim_item,
dim_time) for a given ingestion_date, aggregate to a daily-per-category summary,
and write the gold Parquet that ClickHouse ingests in the load step.

S3A credentials/endpoint are supplied via spark-submit --conf (see the DAG).
"""
from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("daily_txn_transform")

SINK_TABLE = "daily_transaction_summary"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--exec_date", default=datetime.today().strftime("%Y-%m-%d"))
    p.add_argument("--staging_path", default="s3a://staging")
    p.add_argument("--transformed_path", default="s3a://transformed")
    p.add_argument("--shuffle_partitions", default="8")
    p.add_argument("--coalesce_out", type=int, default=1)
    return p.parse_args()


def make_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.warehouse.dir", "file:/tmp/spark-warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_staging(spark, staging_path, exec_date, schema, table):
    path = f"{staging_path.rstrip('/')}/{schema}/{table}/ingestion_date={exec_date}"
    return spark.read.parquet(path)


def main():
    args = parse_args()
    spark = make_spark("Daily-Transaction-Summary-Transform")
    spark.conf.set("spark.sql.shuffle.partitions", args.shuffle_partitions)
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    fct = read_staging(spark, args.staging_path, args.exec_date, "public", "fct_transactions").alias("ft")
    dim_item = read_staging(spark, args.staging_path, args.exec_date, "public", "dim_item").alias("di")
    dim_time = read_staging(spark, args.staging_path, args.exec_date, "public", "dim_time").alias("dt")

    cte = (
        fct
        .join(dim_item, F.col("ft.item_key") == F.col("di.item_key"), "left")
        .join(dim_time, F.col("ft.time_key") == F.col("dt.time_key"), "left")
        .selectExpr(
            "MAKE_DATE(year, month, day) AS transaction_date",
            "quantity",
            "total_price",
            "customer_key",
            "REGEXP_REPLACE(REGEXP_REPLACE(TRIM(desc), '^[a-z]. ', ''), ' - ', ' ') AS item_category",
        )
        # rows we cannot date are unusable for a daily summary; ClickHouse Date is non-nullable
        .where(F.col("transaction_date").isNotNull())
        .withColumn("item_category", F.coalesce(F.col("item_category"), F.lit("UNKNOWN")))
    )

    transformed = (
        cte.groupBy("transaction_date", "item_category")
        .agg(
            F.sum("total_price").cast("double").alias("total_transaction_value"),
            F.sum("quantity").cast("long").alias("total_goods_sold"),
            F.countDistinct("customer_key").cast("long").alias("count_transacting_customer"),
        )
    )

    out = transformed.coalesce(args.coalesce_out) if args.coalesce_out > 0 else transformed
    dest = f"{args.transformed_path.rstrip('/')}/{SINK_TABLE}/ingestion_date={args.exec_date}"
    try:
        out.write.mode("overwrite").parquet(dest)
        log.info(f"✅ Transformed → {dest}")
    except Exception as e:
        log.error(f"❌ Failed writing transformed → {dest}: {e}")
        raise

    spark.stop()


if __name__ == "__main__":
    main()
