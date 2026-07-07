{{
    config(
        materialized='table',
        order_by='(transaction_date, item_category)'
        )
}}
-- Gold datamart: daily transaction summary by (date, item_category). Reproduces the
-- legacy Spark transform (datamart.daily_transaction_summary) from the Iceberg lake via
-- the silver models. INNER JOIN on time drops undatable rows (the old "date IS NOT NULL"
-- filter); LEFT JOIN on item keeps sales of unknown items as 'UNKNOWN'.
SELECT
    t.transaction_date                                AS transaction_date,
    coalesce(nullIf(i.item_category, ''), 'UNKNOWN')  AS item_category,
    toFloat64(sum(f.total_price))                     AS total_transaction_value,
    toInt64(sum(f.quantity))                          AS total_goods_sold,
    toInt64(count(distinct f.customer_key))           AS count_transacting_customer
FROM {{ ref('silver_transactions') }} f
INNER JOIN {{ ref('silver_time') }} t ON f.time_key = t.time_key
LEFT JOIN  {{ ref('silver_item') }} i ON f.item_key = i.item_key
GROUP BY transaction_date, item_category
