{{ config(materialized='table') }}

select distinct
    payment_type as payment_method_id,
    payment_type as payment_method_name
from {{ ref('stg_order_payments') }}
