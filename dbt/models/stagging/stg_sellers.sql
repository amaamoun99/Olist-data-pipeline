{{ config(materialized='table') }}

SELECT
  seller_id,
  seller_city,
  seller_state,
  seller_zip_code_prefix
FROM {{ source('abdelrahman_olist_landing', 'sellers') }}
WHERE seller_id IS NOT NULL

