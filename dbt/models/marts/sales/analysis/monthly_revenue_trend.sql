{{ config(materialized='view') }}

select
    dd.month_start,
    sum(fp.total_payment_value) as monthly_revenue
from {{ ref('fct_payments') }} fp
join {{ ref('dim_date') }} dd
  on date_trunc('month', fp.order_month) = dd.month_start
group by 1
order by 1
