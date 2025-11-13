{{ config(materialized='table') }}

SELECT
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_customer_date,
  order_estimated_delivery_date,
  order_delivered_carrier_date
FROM {{ source('abdelrahman_olist_landing', 'orders') }}
WHERE order_id IS NOT NULL AND customer_id IS NOT NULL


