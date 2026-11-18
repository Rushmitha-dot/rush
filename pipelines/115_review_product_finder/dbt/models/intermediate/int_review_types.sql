{{ config(
    materialized='incremental',
    unique_key=['review_id', 'model_version'],
    incremental_strategy='delete+insert'
) }}

-- Skin or hair type the reviewer describes. A review can carry more than one
-- ("combination skin, sensitive around the nose") and counts toward each.

with scored as (
    select review_id, model_version, scored_at
    from {{ source('nlp', 'scored_reviews') }}
    {% if is_incremental() %}
    where scored_at > (select max(scored_at) from {{ this }})
    {% endif %}
),

type_mentions as (
    select distinct
        review_id,
        model_version,
        case label when 'SKIN_TYPE' then 'skin' else 'hair' end as type_group,
        key as type_key
    from {{ ref('stg_mentions') }}
    where label in ('SKIN_TYPE', 'HAIR_TYPE')
)

select
    s.review_id,
    s.model_version,
    coalesce(t.type_group, 'none') as type_group,
    coalesce(t.type_key, 'none') as type_key,
    s.scored_at
from scored s
left join type_mentions t
  on t.review_id = s.review_id and t.model_version = s.model_version
