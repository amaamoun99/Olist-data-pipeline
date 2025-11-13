{{ config(materialized='table') }}

SELECT
  customer_id,
  customer_unique_id,
  customer_city,
  customer_state,
  customer_zip_code_prefix
FROM {{ source('abdelrahman_olist_landing', 'customers') }}
WHERE customer_id IS NOT NULL AND customer_unique_id IS NOT NULL