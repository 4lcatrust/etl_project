{{
    config(
        materialized='table',
        order_by='time_key'
        )
}}
-- Current-state time: one row per time_key, with a real Date derived from the
-- year/month/day parts. Guard the parts to valid ranges (the legacy Spark job dropped
-- rows whose MAKE_DATE came back null). assumeNotNull the key + derived date so neither is
-- a nullable MergeTree sort key (here and downstream in gold).
WITH deduped AS (
    SELECT
        time_key,
        year,
        month,
        day
    FROM {{ ref('br_postgres_time') }}
    WHERE year BETWEEN 1970 AND 2100
      AND month BETWEEN 1 AND 12
      AND day BETWEEN 1 AND 31
    QUALIFY ROW_NUMBER() OVER (PARTITION BY time_key ORDER BY ingestion_date DESC) = 1
)
SELECT
    assumeNotNull(time_key) AS time_key,
    assumeNotNull(makeDate(year, month, day)) AS transaction_date
FROM deduped
