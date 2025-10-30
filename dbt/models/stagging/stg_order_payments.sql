{{ config(materialized='table') }}

SELECT
  order_id,
  payment_type,
  payment_value,
  payment_sequential,
  payment_installments
FROM {{ source('abdelrahman_olist_landing', 'order_payments') }}
WHERE order_id IS NOT NULL