{{ config(materialized='view') }}

SELECT
  order_id,
  payment_type,
  payment_value
FROM {{ source('raw', 'order_payments') }};
WHERE order_id IS NOT NULL;