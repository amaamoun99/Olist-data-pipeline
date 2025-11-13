{{config(
    materialized='table'
)}}

select 
order_id,
review_creation_date,
review_comment_title,
review_score,
review_id,
review_comment_message
from {{ source('abdelrahman_olist_landing', 'order_reviews') }}
where order_id is not null and review_id is not null and review_score is not null and review_comment_message is not null and review_comment_title is not null

