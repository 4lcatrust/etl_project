{{
    config(
        materialized='table',
        order_by='(sales_month, item_category)',
        tags=['gold_monthly']
        )
}}
-- Gold monthly datamart: monthly transaction summary by (month, item_category), rolled up
-- from the silver monthly sales. Only additive measures (value, goods sold) are carried up;
-- distinct customer counts aren't additive across items, so they stay at silver grain.
-- LEFT JOIN item for the category (unmatched -> UNKNOWN).
SELECT
    s.sales_month                                     AS sales_month,
    coalesce(nullIf(i.item_category, ''), 'UNKNOWN')  AS item_category,
    toFloat64(sum(s.total_transaction_value))         AS total_transaction_value,
    toInt64(sum(s.total_goods_sold))                  AS total_goods_sold
FROM {{ ref('silver_monthly_sales') }} s
LEFT JOIN {{ ref('silver_item') }} i ON s.item_key = i.item_key
GROUP BY sales_month, item_category
