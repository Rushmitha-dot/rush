select
    night,
    nullif(term, '') as term,
    period,
    nullif(holiday, '') as holiday,
    weekend_night,
    case when weekend_night then 'weekend' else 'weeknight' end as night_type
from {{ ref('academic_calendar') }}
