{{ config(materialized='view') }}

select e.*
from {{ ref('product_concern_evidence') }} e
join {{ source('ops', 'model_versions') }} v
  on v.model_version = e.model_version and v.status = 'active'
