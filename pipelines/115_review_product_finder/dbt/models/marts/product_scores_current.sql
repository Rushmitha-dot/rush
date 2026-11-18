{{ config(materialized='view') }}

-- What the chatbot reads. Points at whichever model version ops.model_versions
-- marks active, so switching versions is one update, never a rewrite of scores.

select
    s.model_version,
    s.domain,
    s.concern,
    c.label as concern_label,
    s.type_key,
    s.parent_asin,
    p.title,
    p.store,
    p.average_rating,
    s.n_reviews,
    s.positive_reviews,
    s.negative_reviews,
    s.score,
    s.rank_in_concern
from {{ ref('product_concern_scores') }} s
join {{ source('ops', 'model_versions') }} v
  on v.model_version = s.model_version and v.status = 'active'
join {{ ref('stg_products') }} p on p.parent_asin = s.parent_asin
join {{ ref('concern_taxonomy') }} c on c.concern = s.concern
