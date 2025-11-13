{{ config(materialized='table') }}

with date_spine as (
    select
        day as date_day
    from unnest(
        generate_date_array('2016-01-01', current_date(), interval 1 day)
    ) as day
)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    format_date('%B', date_day) as month_name,
    extract(quarter from date_day) as quarter,
    format_date('%Y-%m', date_day) as year_month,
    date_trunc(date_day, month) as month_start
from date_spine
