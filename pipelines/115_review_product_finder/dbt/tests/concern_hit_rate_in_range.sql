{{ config(severity='warn') }}
-- If the share of reviews with any concern jumps or collapses, the extractor or
-- its prefilter probably broke. The range is wide on purpose.
with per_version as (
    select model_version,
           count(distinct review_id) as reviews,
           count(distinct case when concern <> 'none' then review_id end) as with_concern
    from {{ ref('int_review_concerns') }}
    group by model_version
)
select model_version, reviews, with_concern, 1.0 * with_concern / reviews as hit_rate
from per_version
where 1.0 * with_concern / reviews not between 0.05 and 0.60
