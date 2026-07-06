"""
Iceberg smoke test: verify Spark can create/insert/query Iceberg tables on MinIO,
and log table location for ClickHouse verification.

Operations tested:
1. Create Iceberg namespace and table
2. Insert data in 3 batches (creates 3 snapshots for time travel)
3. Read data via Spark SQL
4. Query snapshot history and metadata tables
5. Log table location for ClickHouse iceberg() function testing
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from datetime import datetime
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("iceberg_smoke_test")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--warehouse_path", default="s3a://iceberg-warehouse")
    p.add_argument("--exec_date", default=datetime.today().strftime("%Y-%m-%d"))
    return p.parse_args()


def make_spark(app_name: str) -> SparkSession:
    """Create Spark session with Iceberg catalog configured (configs via spark-submit --conf)."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.warehouse.dir", "file:/tmp/spark-warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    args = parse_args()
    spark = make_spark("Iceberg-Smoke-Test")

    log.info("=" * 80)
    log.info("ICEBERG SMOKE TEST - Phase A Foundation Verification")
    log.info("=" * 80)

    # 1. Create Iceberg namespace (database)
    log.info("\n[1/7] Creating Iceberg namespace: iceberg.smoke_test")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.smoke_test")
    log.info("✅ Namespace created")

    # 2. Create Iceberg table with partitioning
    log.info("\n[2/7] Creating Iceberg table: iceberg.smoke_test.sample_transactions")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.smoke_test.sample_transactions (
            transaction_date DATE,
            item_category STRING,
            total_transaction_value DOUBLE,
            total_goods_sold BIGINT,
            count_transacting_customer BIGINT
        )
        USING iceberg
        PARTITIONED BY (days(transaction_date))
    """)
    log.info("✅ Table created (partitioned by days(transaction_date))")

    # 3. Insert sample data in 3 batches (creates 3 snapshots for time travel testing)
    log.info("\n[3/7] Inserting sample data (3 batches = 3 snapshots)")
    for batch in range(1, 4):
        data = [
            (datetime(2024, 1, batch).date(), f"Category_{batch}", 1000.0 * batch, 100 * batch, 10 * batch)
        ]
        df = spark.createDataFrame(data, ["transaction_date", "item_category", "total_transaction_value",
                                           "total_goods_sold", "count_transacting_customer"])
        df.writeTo("iceberg.smoke_test.sample_transactions").append()
        log.info(f"  ✅ Batch {batch} inserted")

    # 4. Read data via Spark SQL
    log.info("\n[4/7] Reading data from Iceberg table")
    result = spark.sql("SELECT * FROM iceberg.smoke_test.sample_transactions ORDER BY transaction_date")
    count = result.count()
    log.info(f"✅ Read {count} rows from Iceberg table")
    log.info("\nData preview:")
    result.show(truncate=False)

    # 5. Query snapshot history (time travel capability)
    log.info("\n[5/7] Querying snapshot history")
    snapshots = spark.sql("SELECT snapshot_id, committed_at, operation, summary FROM iceberg.smoke_test.sample_transactions.snapshots")
    snapshot_count = snapshots.count()
    log.info(f"✅ Found {snapshot_count} snapshots")
    log.info("\nSnapshot history:")
    snapshots.show(truncate=False)

    # 6. Query metadata (data files)
    log.info("\n[6/7] Querying table metadata (data files)")
    files = spark.sql("SELECT file_path, record_count, file_size_in_bytes FROM iceberg.smoke_test.sample_transactions.files")
    file_count = files.count()
    log.info(f"✅ Found {file_count} data files")
    log.info("\nData files:")
    files.show(truncate=False)

    # 7. Get table location for ClickHouse verification
    log.info("\n[7/7] Table location for ClickHouse verification")
    location_row = spark.sql("DESCRIBE EXTENDED iceberg.smoke_test.sample_transactions") \
                        .filter("col_name = 'Location'")

    if location_row.count() > 0:
        location = location_row.select("data_type").first()[0]
        log.info(f"📍 Table location: {location}")
        log.info("\n" + "=" * 80)
        log.info("CLICKHOUSE VERIFICATION - Run these queries manually:")
        log.info("=" * 80)
        log.info("\nTest Query 1 - iceberg() function:")
        log.info(f"""
SELECT *
FROM iceberg(
    'http://minio:9000/iceberg-warehouse/smoke_test/sample_transactions',
    'minioadmin',
    '<MINIO_ROOT_PASSWORD>'
)
ORDER BY transaction_date;
""")
        log.info("\nTest Query 2 - icebergS3() function (if available):")
        log.info(f"""
SELECT *
FROM icebergS3(
    'http://minio:9000',
    'iceberg-warehouse/smoke_test/sample_transactions',
    'minioadmin',
    '<MINIO_ROOT_PASSWORD>'
)
ORDER BY transaction_date;
""")
    else:
        log.warning("⚠️  Could not retrieve table location")

    # Final summary
    log.info("\n" + "=" * 80)
    log.info("SMOKE TEST COMPLETE")
    log.info("=" * 80)
    log.info(f"✅ Iceberg catalog operational")
    log.info(f"✅ Table created with partitioning")
    log.info(f"✅ {count} rows inserted across {snapshot_count} snapshots")
    log.info(f"✅ Data readable via Spark SQL")
    log.info(f"✅ Snapshot history accessible (time travel ready)")
    log.info(f"✅ Metadata tables queryable")
    log.info(f"✅ {file_count} data files written to MinIO")
    log.info("\n🔎 Next: Test ClickHouse read capability with queries above")
    log.info("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
