{{ config(materialized='table') }}

with customer_values as (
    select * from {{ ref('int_customer_order_values') }}
),

customer_details as (
    select
        cv.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        cv.total_order_value,
        cv.total_orders,
        cv.avg_order_value
    from customer_values cv
    left join {{ ref('stg_customers') }} c
        on cv.customer_id = c.customer_id
)

select
    customer_unique_id,
    customer_city,
    customer_state,
    total_orders,
    total_order_value,
    avg_order_value
from customer_details
order by total_order_value desc
limit 10