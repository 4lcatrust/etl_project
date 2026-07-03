CREATE DATABASE IF NOT EXISTS data_quality ENGINE = Atomic COMMENT 'Data quality metrics storage';
CREATE DATABASE IF NOT EXISTS intermediate ENGINE = Atomic COMMENT 'Intermediate phase database - transformed table';
CREATE DATABASE IF NOT EXISTS datamart ENGINE = Atomic COMMENT 'Production phase database';

-- Gold table: daily transaction summary by (date, item category).
-- ReplacingMergeTree(updated_at) makes reloads idempotent — the newest row per
-- ORDER BY key wins after a merge / OPTIMIZE ... FINAL. Partitioned by month.
CREATE TABLE IF NOT EXISTS datamart.daily_transaction_summary
(
    transaction_date          Date,
    item_category             String,
    total_transaction_value   Float64,
    total_goods_sold          Int64,
    count_transacting_customer Int64,
    updated_at                UInt32 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(transaction_date)
ORDER BY (transaction_date, item_category);
