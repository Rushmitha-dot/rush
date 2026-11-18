-- A hair concern on a skin product (or the reverse) means the domain keywords or
-- the taxonomy drifted.
select s.model_version, s.parent_asin, s.concern, s.domain
from {{ ref('product_concern_scores') }} s
join {{ ref('concern_taxonomy') }} t on t.concern = s.concern
where t.domain <> 'both' and t.domain <> s.domain
