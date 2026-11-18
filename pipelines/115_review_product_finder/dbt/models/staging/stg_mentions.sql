select
    review_id,
    model_version,
    mention_no,
    label,
    key,
    matched,
    sentence,
    polarity,
    scored_at
from {{ source('nlp', 'mentions') }}
