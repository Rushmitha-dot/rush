select distinct c.model_version
from {{ ref('int_review_concerns') }} c
left join {{ source('ops', 'model_versions') }} v on v.model_version = c.model_version
where v.model_version is null
