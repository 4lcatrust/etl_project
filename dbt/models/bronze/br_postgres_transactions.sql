{{
    config(
        materialized='view'
        )
}}
-- ClickHouse view over the Iceberg bronze table. Credentials come from the dbt process
-- env (set by the Airflow dbt task). Bronze is append-only (one snapshot per run) —
-- dedup happens in silver.
SELECT *
FROM iceberg(
    '{{ env_var("MINIO_ENDPOINT", "http://minio:9000") }}/iceberg-warehouse/bronze/postgres__transactions',
    '{{ env_var("MINIO_ACCESS_KEY") }}',
    '{{ env_var("MINIO_SECRET_KEY") }}'
)
