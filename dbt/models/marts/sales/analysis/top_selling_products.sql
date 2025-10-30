-- question 3
{{ config(materialized='view') }}
with product_sales as (
    select
        oi.product_id,
        count(oi.order_id) as total_quantity_sold
    from {{ ref('fct_order_items') }} as oi
    group by oi.product_id
)
select
    ps.product_id,
    p.product_category_name,
    ps.total_quantity_sold
from product_sales ps
join {{ ref('dim_products') }} p using (product_id)
order by ps.total_quantity_sold desc
limit 10
