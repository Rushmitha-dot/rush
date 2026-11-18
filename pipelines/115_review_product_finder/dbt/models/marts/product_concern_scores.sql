-- One row per model version, product, concern and reviewer type.
-- Score is the mean mention sentiment, pulled toward the concern's average with
-- prior_weight pseudo-reviews, then mapped from [-1, 1] to 0..100. Rows under
-- min_reviews aren't published at all rather than shown with a shaky number.

with agg as (
    select
        model_version,
        parent_asin,
        domain,
        concern,
        type_key,
        count(*) as n_reviews,
        avg(polarity) as mean_polarity,
        sum(case when polarity >= 0.05 then 1 else 0 end) as positive_reviews,
        sum(case when polarity <= -0.05 then 1 else 0 end) as negative_reviews
    from {{ ref('int_typed_concern_reviews') }}
    group by model_version, parent_asin, domain, concern, type_key
),

prior as (
    select model_version, concern, type_key,
           sum(mean_polarity * n_reviews) / sum(n_reviews) as prior_polarity
    from agg
    group by model_version, concern, type_key
),

scored as (
    select
        a.*,
        p.prior_polarity,
        (a.n_reviews * a.mean_polarity + {{ var('prior_weight') }} * p.prior_polarity)
            / (a.n_reviews + {{ var('prior_weight') }}) as shrunk_polarity
    from agg a
    join prior p
      on p.model_version = a.model_version and p.concern = a.concern and p.type_key = a.type_key
    where a.n_reviews >= {{ var('min_reviews') }}
)

select
    model_version,
    parent_asin,
    domain,
    concern,
    type_key,
    n_reviews,
    positive_reviews,
    negative_reviews,
    round(mean_polarity, 4) as mean_polarity,
    round(prior_polarity, 4) as prior_polarity,
    round(50 * (shrunk_polarity + 1), 1) as score,
    row_number() over (
        partition by model_version, domain, concern, type_key
        order by shrunk_polarity desc, n_reviews desc, parent_asin
    ) as rank_in_concern
from scored
