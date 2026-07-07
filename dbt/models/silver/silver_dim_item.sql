{{ config(materialized='table', order_by='item_key') }}
-- Current-state dim_item: one row per item_key (latest ingested snapshot wins), with the
-- item_category cleaned from `desc` — the same transform the legacy Spark job did
-- (strip a leading "x. " prefix, collapse " - " to a space). Bronze is append-only, so
-- QUALIFY collapses the repeated snapshots.
select
    item_key,
    replaceRegexpAll(replaceRegexpAll(trimBoth(`desc`), '^[a-z]. ', ''), ' - ', ' ') as item_category
from {{ ref('br_postgres_dim_item') }}
qualify row_number() over (partition by item_key order by ingestion_date desc) = 1
