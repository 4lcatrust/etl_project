{{
    config(
        materialized='table'
        )
}}
-- Current-state facts: transactions has no natural key, so instead of per-row dedup
-- we take the latest ingested snapshot in full (each bronze run appends the same ~1M-row
-- snapshot; this collapses them back to one).
SELECT
    item_key,
    time_key,
    customer_key,
    quantity,
    total_price
FROM {{ ref('br_postgres_transactions') }}
WHERE ingestion_date = (SELECT MAX(ingestion_date) FROM {{ ref('br_postgres_transactions') }})
