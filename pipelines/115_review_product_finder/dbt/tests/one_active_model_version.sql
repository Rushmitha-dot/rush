-- The current views only make sense with exactly one active version.
select count(*) as active_versions
from {{ source('ops', 'model_versions') }}
where status = 'active'
having count(*) <> 1
