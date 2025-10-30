{{config(
    materialized='table',
)}}

select
  won_date,
  seller_id,
  lead_behaviour_profile,
  mql_id,
  has_gtin,
  sr_id,
  declared_product_catalog_size,
  lead_type,
  declared_monthly_revenue,
  business_type,
  average_stock,
  business_segment,
  sdr_id,
  has_company


from {{ source('abdelrahman_olist_landing', 'leads_closed') }}
where won_date is not null and seller_id is not null and lead_behaviour_profile is not null