from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from datetime import datetime
import argparse, logging, json

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("daily_txn_extract_dq")

# ---------- expected schema ----------
EXPECTED_SCHEMA = {
    "fct_transactions": {
        "payment_key": StringType(),
        "customer_key": StringType(),
        "time_key": StringType(),
        "item_key": StringType(),
        "store_key": StringType(),
        "quantity": IntegerType(),
        "unit": StringType(),
        "unit_price": IntegerType(),
        "total_price": IntegerType(),
    },
    "dim_item": {
        "item_key": StringType(),
        "item_name": StringType(),
        "desc": StringType(),
        "unit_price": FloatType(),
        "man_country": StringType(),
        "supplier": StringType(),
        "unit": StringType(),
    },
    "dim_time": {
        "time_key": StringType(),
        "date": StringType(),
        "hour": IntegerType(),
        "day": IntegerType(),
        "week": StringType(),
        "month": IntegerType(),
        "quarter": StringType(),
        "year": IntegerType(),
    },
}

TABLE_SOURCE = [
    ("public", "fct_transactions"),
    ("public", "dim_item"),
    ("public", "dim_time"),
]

# ---------- args ----------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--pg_url", required=True, help="JDBC URL, include timeouts if possible")
    p.add_argument("--pg_user", required=True)
    p.add_argument("--pg_pass", required=True)
    p.add_argument("--exec_date", default=datetime.today().strftime("%Y-%m-%d"))
    p.add_argument("--staging_path", default="s3a://staging")
    p.add_argument("--dq_path", default="s3a://staging-dq")
    p.add_argument("--shuffle_partitions", default="8")
    p.add_argument("--records_per_file", default="500000")
    p.add_argument("--coalesce_out", type=int, default=1)  # small while stabilizing
    return p.parse_args()

# ---------- spark ----------
def make_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        # keep FS local to avoid HDFS touches during submit
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.warehouse.dir", "file:/tmp/spark-warehouse")
        .getOrCreate()
    )
    # quiet logging. NOTE: spark.ui.enabled / spark.eventLog.enabled are static
    # configs in Spark 4.0 and cannot be set at runtime (raises
    # CANNOT_MODIFY_CONFIG); pass them as spark-submit --conf if you need them.
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ---------- helpers ----------
def schema_validity(df, tablename):
    exp = set(EXPECTED_SCHEMA[tablename].keys())
    return 100 if set(df.columns) == exp else 0

def dq_metrics_single_pass(tablename, df):
    df = df.cache()
    n = df.count()
    metrics = {f"{tablename}_schema_validity": schema_validity(df, tablename)}
    if n == 0:
        return metrics

    if tablename == "fct_transactions":
        row = df.agg(
            F.sum(F.col("customer_key").isNotNull().cast("int")).alias("nn_customer_key"),
            F.sum(F.col("item_key").isNotNull().cast("int")).alias("nn_item_key"),
            F.sum(F.col("time_key").isNotNull().cast("int")).alias("nn_time_key"),
            F.sum(F.when(F.col("quantity") > 0, 1).otherwise(0)).alias("pos_quantity"),
            F.sum(F.when(F.col("unit_price") > 0, 1).otherwise(0)).alias("pos_unit_price"),
            F.sum(F.when(F.col("total_price") > 0, 1).otherwise(0)).alias("pos_total_price"),
        ).first()
        metrics.update({
            f"{tablename}_null_customer_key": 100.0 * row.nn_customer_key / n,
            f"{tablename}_null_item_key":     100.0 * row.nn_item_key / n,
            f"{tablename}_null_time_key":     100.0 * row.nn_time_key / n,
            f"{tablename}_negative_quantity": 100.0 * row.pos_quantity / n,
            f"{tablename}_negative_unit_price": 100.0 * row.pos_unit_price / n,
            f"{tablename}_negative_total_price": 100.0 * row.pos_total_price / n,
        })
    elif tablename == "dim_item":
        row = df.agg(
            F.sum(F.col("item_key").isNotNull().cast("int")).alias("nn_item_key"),
            F.sum(F.col("desc").isNotNull().cast("int")).alias("nn_desc"),
            F.sum(F.col("item_name").isNotNull().cast("int")).alias("nn_item_name"),
            F.sum(F.when(F.col("unit_price") > 0, 1).otherwise(0)).alias("pos_unit_price"),
        ).first()
        metrics.update({
            f"{tablename}_null_item_key": 100.0 * row.nn_item_key / n,
            f"{tablename}_null_desc": 100.0 * row.nn_desc / n,
            f"{tablename}_null_item_name": 100.0 * row.nn_item_name / n,
            f"{tablename}_negative_unit_price": 100.0 * row.pos_unit_price / n,
        })
    elif tablename == "dim_time":
        row = df.agg(F.sum(F.col("time_key").isNotNull().cast("int")).alias("nn_time_key")).first()
        metrics.update({f"{tablename}_null_time_key": 100.0 * row.nn_time_key / n})
    return metrics

def read_table(spark, url, user, pwd, schema, table):
    # Tip: add fetchsize; add numeric partitioning later if tables are large
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", f"{schema}.{table}")
        .option("user", user)
        .option("password", pwd)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", 10000)
        .load()
    )

# ---------- main ----------
def main():
    args = parse_args()
    spark = make_spark("Daily-Transaction-Summary-Extract-and-DQ")

    # small, object-store-friendly settings
    spark.conf.set("spark.sql.shuffle.partitions", args.shuffle_partitions)
    spark.conf.set("spark.sql.files.maxRecordsPerFile", args.records_per_file)
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    # early probe write (clear S3A misconfig fast), then delete it so probes
    # don't accumulate in the bucket
    probe = f"{args.staging_path.rstrip('/')}/_probe/{datetime.now().strftime('%Y%m%d%H%M%S')}"
    from pyspark.sql import Row
    spark.createDataFrame([Row(ok=1)]).write.mode("overwrite").parquet(probe)
    log.info(f"✅ S3A probe write OK at {probe}")
    probe_path = spark._jvm.org.apache.hadoop.fs.Path(probe)
    probe_path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(probe_path, True)

    dataframes = {}
    for schema, table in TABLE_SOURCE:
        df = read_table(spark, args.pg_url, args.pg_user, args.pg_pass, schema, table)
        out = df.coalesce(args.coalesce_out) if args.coalesce_out > 0 else df
        # Hive-style layout: <staging>/<schema>/<table>/ingestion_date=<exec_date>/*.parquet
        # so readers can partition-prune on ingestion_date and reruns overwrite one date.
        path = f"{args.staging_path.rstrip('/')}/{schema}/{table}/ingestion_date={args.exec_date}"
        try:
            out.write.mode("overwrite").parquet(path)
            log.info(f"✅ Extracted & saved {table} → {path}")
        except Exception as e:
            log.error(f"❌ Failed writing {table} → {path}: {e}")
            raise
        dataframes[table] = df

    # DQ (single pass per table)
    all_metrics, dq_rows = {}, []
    for table, df in dataframes.items():
        m = dq_metrics_single_pass(table, df)
        all_metrics[table] = m
        for k, v in m.items():
            dq_rows.append({
                "dag_id": "dag_daily_transaction_summary_extract_dq",
                "table_name": table,
                "metric": k,
                "value": float(v),
                "processed_at": datetime.now().isoformat(),
            })

    dq_df = spark.createDataFrame(dq_rows)
    # Same Hive-style layout as the staging tables:
    # <dq_path>/dq_metrics/ingestion_date=<exec_date>/*.parquet. Writing directly
    # to the partition dir with overwrite replaces just this date (a static
    # partitionBy overwrite would wipe every date's metrics).
    dq_out = f"{args.dq_path.rstrip('/')}/dq_metrics/ingestion_date={args.exec_date}"
    dq_df.write.mode("overwrite").parquet(dq_out)
    log.info(f"✅ Wrote {len(dq_rows)} DQ metrics → {dq_out}")

    # thresholds
    failed = {t: {k: v for k, v in m.items() if v < 90} for t, m in all_metrics.items() if any(v < 90 for v in m.values())}
    if failed:
        raise RuntimeError(f"DQ thresholds breached: {json.dumps(failed)}")
    log.info("✅ All DQ checks passed")

    spark.stop()  # clean shutdown (no System.exit / os._exit)

if __name__ == "__main__":
    main()
