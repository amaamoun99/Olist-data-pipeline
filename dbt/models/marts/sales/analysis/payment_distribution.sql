{{ config(materialized='view') }}

select
    dpm.payment_method_name,
    count(distinct fp.order_id) as total_orders,
    round(100.0 * count(distinct fp.order_id) / sum(count(distinct fp.order_id)) over (), 2) as pct_of_total
from {{ ref('fct_payments') }} fp
join {{ ref('dim_payment_methods') }} dpm
  on fp.payment_method_id = dpm.payment_method_id
group by 1
order by total_orders desc
