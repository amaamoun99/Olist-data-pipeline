{{ config(materialized='table') }}

select
    o.order_id,
    o.customer_id,
    p.payment_type,
    p.payment_value,
    o.order_purchase_timestamp,
    DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) as order_month
from {{ ref('stg_orders') }} o
join {{ ref('stg_order_payments') }} p
    on o.order_id = p.order_id
