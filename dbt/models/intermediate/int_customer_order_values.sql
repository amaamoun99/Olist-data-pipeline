{{ config(materialized='table') }}

-- Combine orders, order_items, payments, and customers
with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

payments as (
    select * from {{ ref('stg_order_payments') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- Calculate order-level total value
order_values as (
    select
        oi.order_id,
        sum(oi.price + oi.freight_value) as order_total_value
    from order_items oi
    group by oi.order_id
),

-- Join orders with customers and payments
orders_with_customers as (
    select
        o.order_id,
        c.customer_id,
        o.order_purchase_timestamp,
        ov.order_total_value
    from orders o
    join customers c on o.customer_id = c.customer_id
    join order_values ov on o.order_id = ov.order_id
)

select
    customer_id,
    sum(order_total_value) as total_order_value,
    count(distinct order_id) as total_orders,
    avg(order_total_value) as avg_order_value
from orders_with_customers
group by customer_id
