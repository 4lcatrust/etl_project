{{ config(materialized='table') }}
-- Current-state facts: fct_transactions has no natural key, so instead of per-row dedup
-- we take the latest ingested snapshot in full (each bronze run appends the same ~1M-row
-- snapshot; this collapses them back to one).
select
    item_key,
    time_key,
    customer_key,
    quantity,
    total_price
from {{ ref('br_postgres_fct_transactions') }}
where ingestion_date = (select max(ingestion_date) from {{ ref('br_postgres_fct_transactions') }})
