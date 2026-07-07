{{ config(materialized='table', order_by='(transaction_date, item_category)') }}
-- Gold datamart: daily transaction summary by (date, item_category). Reproduces the
-- legacy Spark transform (datamart.daily_transaction_summary) from the Iceberg lake via
-- the silver models. INNER JOIN on time drops undatable rows (the old "date IS NOT NULL"
-- filter); LEFT JOIN on item keeps sales of unknown items as 'UNKNOWN'.
select
    t.transaction_date                                as transaction_date,
    coalesce(nullIf(i.item_category, ''), 'UNKNOWN')  as item_category,
    toFloat64(sum(f.total_price))                     as total_transaction_value,
    toInt64(sum(f.quantity))                          as total_goods_sold,
    toInt64(count(distinct f.customer_key))           as count_transacting_customer
from {{ ref('silver_transactions') }} f
inner join {{ ref('silver_time') }} t on f.time_key = t.time_key
left  join {{ ref('silver_item') }} i on f.item_key = i.item_key
group by transaction_date, item_category
