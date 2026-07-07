{{
    config(
        materialized='table',
        order_by='(sales_month, item_key)',
        tags=['silver_monthly']
        )
}}
-- Monthly per-item sales rollup built from the current-state (daily) silver tables.
-- sales_month is the first day of the month; item_key is assumeNotNull (already
-- not-null-validated out of bronze) so neither is a nullable MergeTree sort key.
SELECT
    toStartOfMonth(t.transaction_date)       AS sales_month,
    assumeNotNull(f.item_key)                AS item_key,
    toInt64(sum(f.quantity))                 AS total_goods_sold,
    toFloat64(sum(f.total_price))            AS total_transaction_value,
    toInt64(count(DISTINCT f.customer_key))  AS count_transacting_customer
FROM {{ ref('silver_transactions') }} f
INNER JOIN {{ ref('silver_time') }} t ON f.time_key = t.time_key
GROUP BY sales_month, item_key
