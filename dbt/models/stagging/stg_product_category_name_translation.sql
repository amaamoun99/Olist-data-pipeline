{{ config(materialized='table') }}

SELECT
  product_category_name,
  product_category_name_english
FROM {{ source('abdelrahman_olist_landing', 'product_category_name_translation') }}
WHERE product_category_name_english IS NOT NULL and product_category_name IS NOT NULL