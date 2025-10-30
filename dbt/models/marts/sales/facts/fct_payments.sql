{{ config(materialized='table') }}

select
    order_id,
    customer_id,
    payment_type as payment_method_id,
    sum(payment_value) as total_payment_value,
    DATE_TRUNC(DATE(order_purchase_timestamp), MONTH) as order_month 
from {{ ref('int_order_payments') }}
group by 1, 2, 3, 5
