
{{ config(materialized='view') }}
with monthly_orders as (
    select
        format_date('%Y-%m', date(o.order_purchase_timestamp)) as year_month,
        count(distinct o.order_id) as total_orders
    from {{ ref('stg_orders') }} as o
    group by 1
)
select
    d.year_month,
    m.total_orders
from {{ ref('dim_date') }} d
left join monthly_orders m using (year_month)
order by d.year_month
