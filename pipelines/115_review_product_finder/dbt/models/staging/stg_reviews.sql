select
    review_id,
    batch_date,
    parent_asin,
    user_id,
    rating,
    title,
    review_ts,
    coalesce(helpful_vote, 0) as helpful_vote,
    verified_purchase
from {{ source('raw', 'reviews') }}
