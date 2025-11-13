{{ config(materialized='view') }}

select
    dpm.payment_method_name,
    sum(fp.total_payment_value) as total_revenue,
    round(100.0 * sum(fp.total_payment_value) / sum(sum(fp.total_payment_value)) over (), 2) as pct_revenue
from {{ ref('fct_payments') }} fp
join {{ ref('dim_payment_methods') }} dpm
  on fp.payment_method_id = dpm.payment_method_id
group by 1
order by total_revenue desc
