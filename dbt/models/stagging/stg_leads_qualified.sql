{{config(
    materialized='table'
)}}

select 
  mql_id,
  landing_page_id,
  origin,
  first_contact_date

from {{ source('abdelrahman_olist_landing', 'leads_qualified') }}
where mql_id is not null and landing_page_id is not null 