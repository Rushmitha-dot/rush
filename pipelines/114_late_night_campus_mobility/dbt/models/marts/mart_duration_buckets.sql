select
    night_type,
    start_hour,
    any_value(hour_order) as hour_order,
    duration_bucket,
    count(*) as trips,
    round(count(*) / sum(count(*)) over (partition by night_type, start_hour), 4) as share_of_hour
from {{ ref('fct_late_night_trips') }}
group by night_type, start_hour, duration_bucket
