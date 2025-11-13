{{ config(materialized='table') }}
select
    o.order_id,
    o.customer_id,
    date(o.order_purchase_timestamp) as order_date,
    extract(year from o.order_purchase_timestamp) as order_year,
    extract(month from o.order_purchase_timestamp) as order_month,
    count(distinct oi.product_id) as total_products,
    sum(oi.price) as total_price,
    sum(oi.freight_value) as total_freight,
    sum(oi.price + oi.freight_value) as total_order_value
from {{ ref('stg_orders') }} as o
join {{ ref('stg_order_items') }} as oi using (order_id)
group by 1,2,3,4,5
