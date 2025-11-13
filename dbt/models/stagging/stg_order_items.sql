{{ config(materialized='table') }}

SELECT
  order_id,
  order_item_id,
  product_id,
  seller_id,
  price,
  freight_value,
  shipping_limit_date
FROM {{ source('abdelrahman_olist_landing', 'order_items') }}
WHERE order_id IS NOT NULL and order_item_id IS NOT NULL and product_id IS NOT NULL and seller_id IS NOT NULL