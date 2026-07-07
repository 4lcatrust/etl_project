{{ config(materialized='table', order_by='item_key') }}
-- Current-state dim_item: one row per item_key (latest ingested snapshot wins), with the
-- item_category cleaned from `desc` — the same transform the legacy Spark job did
-- (strip a leading "x. " prefix, collapse " - " to a space). Bronze is append-only, so
-- QUALIFY collapses the repeated snapshots. Iceberg columns read back as Nullable, and a
-- MergeTree ORDER BY key can't be nullable, so assumeNotNull the key (null item_key is
-- already quarantined out of bronze).
with deduped as (
    select item_key, `desc`
    from {{ ref('br_postgres_dim_item') }}
    qualify row_number() over (partition by item_key order by ingestion_date desc) = 1
)
select
    assumeNotNull(item_key) as item_key,
    replaceRegexpAll(replaceRegexpAll(trimBoth(`desc`), '^[a-z]. ', ''), ' - ', ' ') as item_category
from deduped
