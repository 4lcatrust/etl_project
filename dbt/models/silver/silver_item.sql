{{
    config(
        materialized='table',
        order_by='item_key'
        )
}}
-- Current-state item: one row per item_key (latest ingested snapshot wins), with the
-- item_category cleaned from `desc` — the same transform the legacy Spark job did
-- (strip a leading "x. " prefix, collapse " - " to a space). Bronze is append-only, so
-- QUALIFY collapses the repeated snapshots. Iceberg columns read back as Nullable, and a
-- MergeTree ORDER BY key can't be nullable, so assumeNotNull the key (null item_key is
-- already quarantined out of bronze).
WITH deduped AS (
    SELECT
        item_key,
        `desc`
    FROM {{ ref('br_postgres_item') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY item_key ORDER BY ingestion_date DESC) = 1
)
SELECT
    assumeNotNull(item_key) AS item_key,
    replaceRegexpAll(replaceRegexpAll(trimBoth(`desc`), '^[a-z]. ', ''), ' - ', ' ') AS item_category
FROM deduped
