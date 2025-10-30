-- models/marts/sales/fct_order_items.sql
select
    oi.order_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) as total_value,
    case
        when o.order_delivered_customer_date <= o.order_estimated_delivery_date then true
        else false
    end as delivered_on_time
from {{ ref('stg_order_items') }} as oi
join {{ ref('stg_orders') }} as o using (order_id)
