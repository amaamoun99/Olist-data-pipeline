{{config(materialized='table') }}

select
  geolocation_zip_code_prefix,
  geolocation_city,
  geolocation_state,
  geolocation_lat,
  geolocation_lng
from {{ source('abdelrahman_olist_landing', 'geolocation') }}
where geolocation_zip_code_prefix is not null