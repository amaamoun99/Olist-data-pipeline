{{ config(materialized='table') }}

SELECT
  product_id,
  product_category_name,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm,
  product_photos_qty,
  product_name_lenght,
  product_description_lenght
FROM {{ source('abdelrahman_olist_landing', 'products') }}
WHERE product_id IS NOT NULL

