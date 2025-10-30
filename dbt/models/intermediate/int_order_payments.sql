{{ config(materialized='view') }}

select
    o.order_id,
    o.customer_id,
    p.payment_type,
    p.payment_value,
    o.order_purchase_timestamp,
    date_trunc(month, o.order_purchase_timestamp) as order_month
from {{ ref('stg_orders') }} o
join {{ ref('stg_order_payments') }} p
    on o.order_id = p.order_id
