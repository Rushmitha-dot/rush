{{ config(materialized='table') }}

-- Concern reviews on products whose domain fits the concern (no frizz scores on a
-- face cream), fanned out to 'any' plus each type the reviewer described.

with concern_reviews as (
    select c.model_version, c.review_id, c.concern, c.polarity, r.parent_asin
    from {{ ref('int_review_concerns') }} c
    join {{ ref('stg_reviews') }} r on r.review_id = c.review_id
    where c.concern <> 'none'
),

fitting as (
    select cr.*, p.domain
    from concern_reviews cr
    join {{ ref('stg_products') }} p on p.parent_asin = cr.parent_asin
    join {{ ref('concern_taxonomy') }} t on t.concern = cr.concern
    where p.domain <> 'other'
      and (t.domain = 'both' or t.domain = p.domain)
)

select model_version, review_id, parent_asin, domain, concern, polarity, 'any' as type_key
from fitting

union all

select f.model_version, f.review_id, f.parent_asin, f.domain, f.concern, f.polarity, t.type_key
from fitting f
join {{ ref('int_review_types') }} t
  on t.review_id = f.review_id
 and t.model_version = f.model_version
 and t.type_group = f.domain
