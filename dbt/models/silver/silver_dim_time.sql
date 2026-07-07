{{ config(materialized='table', order_by='time_key') }}
-- Current-state dim_time: one row per time_key, with a real Date derived from the
-- year/month/day parts. Guard the parts to valid ranges (the legacy Spark job dropped
-- rows whose MAKE_DATE came back null). assumeNotNull the key + derived date so neither is
-- a nullable MergeTree sort key (here and downstream in gold).
with deduped as (
    select time_key, year, month, day
    from {{ ref('br_postgres_dim_time') }}
    where year between 1970 and 2100
      and month between 1 and 12
      and day between 1 and 31
    qualify row_number() over (partition by time_key order by ingestion_date desc) = 1
)
select
    assumeNotNull(time_key) as time_key,
    assumeNotNull(makeDate(year, month, day)) as transaction_date
from deduped
