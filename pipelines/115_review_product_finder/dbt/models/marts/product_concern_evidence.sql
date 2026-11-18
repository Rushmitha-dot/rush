-- Up to three review sentences per published score, so the chatbot can show
-- why a product ranks where it does. Most helpful first, one sentence per review.

with published as (
    select distinct model_version, parent_asin, concern, type_key
    from {{ ref('product_concern_scores') }}
),

candidates as (
    select
        t.model_version, t.parent_asin, t.concern, t.type_key, t.review_id,
        m.sentence, m.polarity, r.rating, r.helpful_vote, r.review_ts,
        row_number() over (
            partition by t.model_version, t.parent_asin, t.concern, t.type_key, t.review_id
            order by abs(m.polarity) desc, m.mention_no
        ) as rn_in_review
    from {{ ref('int_typed_concern_reviews') }} t
    join published p
      on p.model_version = t.model_version and p.parent_asin = t.parent_asin
     and p.concern = t.concern and p.type_key = t.type_key
    join {{ ref('stg_mentions') }} m
      on m.review_id = t.review_id and m.model_version = t.model_version
     and m.label = 'CONCERN' and m.key = t.concern
    join {{ ref('stg_reviews') }} r on r.review_id = t.review_id
)

select model_version, parent_asin, concern, type_key, review_id, sentence, polarity, rating, helpful_vote,
       review_ts, evidence_rank
from (
    select c.*,
           row_number() over (
               partition by model_version, parent_asin, concern, type_key
               order by helpful_vote desc, abs(polarity) desc, review_ts desc, review_id
           ) as evidence_rank
    from candidates c
    where rn_in_review = 1
) ranked
where evidence_rank <= 3
