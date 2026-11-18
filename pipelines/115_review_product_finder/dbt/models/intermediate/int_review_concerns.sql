{{ config(
    materialized='incremental',
    unique_key=['review_id', 'model_version'],
    incremental_strategy='delete+insert'
) }}

-- One row per review, model version and concern. Reviews with no concern keep a
-- single 'none' row, so a rescore that removes every concern from a review still
-- replaces the old rows through delete+insert on (review_id, model_version).

with scored as (
    select review_id, model_version, scored_at
    from {{ source('nlp', 'scored_reviews') }}
    {% if is_incremental() %}
    where scored_at > (select max(scored_at) from {{ this }})
    {% endif %}
),

concern_mentions as (
    select review_id, model_version, key as concern, polarity, sentence, mention_no
    from {{ ref('stg_mentions') }}
    where label = 'CONCERN'
)

select
    s.review_id,
    s.model_version,
    coalesce(m.concern, 'none') as concern,
    count(m.mention_no) as mentions,
    avg(m.polarity) as polarity,
    s.scored_at
from scored s
left join concern_mentions m
  on m.review_id = s.review_id and m.model_version = s.model_version
group by s.review_id, s.model_version, coalesce(m.concern, 'none'), s.scored_at
