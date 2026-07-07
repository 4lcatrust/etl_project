{{ config(materialized='view') }}
-- ClickHouse view over the Iceberg bronze table (see br_postgres_fct_transactions).
select *
from iceberg(
  '{{ env_var("MINIO_ENDPOINT", "http://minio:9000") }}/iceberg-warehouse/bronze/postgres__time',
  '{{ env_var("MINIO_ACCESS_KEY") }}',
  '{{ env_var("MINIO_SECRET_KEY") }}'
)
