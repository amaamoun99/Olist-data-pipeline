{{ config(materialized='table') }}

WITH orders AS (
  SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_order_value) AS total_order_value
  FROM {{ ref('fct_orders') }}
  GROUP BY customer_id
),

customers AS (
  SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
  FROM {{ ref('dim_customers') }}
)

SELECT
  c.customer_id,
  c.customer_unique_id,
  c.customer_city,
  c.customer_state,
  o.total_orders,
  o.total_order_value,
  ROUND(o.total_order_value / o.total_orders, 2) AS avg_order_value
FROM customers c
LEFT JOIN orders o USING (customer_id);
