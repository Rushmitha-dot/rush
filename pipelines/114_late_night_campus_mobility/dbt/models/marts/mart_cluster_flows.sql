select
    start_cluster,
    end_cluster,
    case when start_hour >= 22 then 'before_midnight' else 'after_midnight' end as part_of_night,
    count(*) as trips,
    round(count(*) / sum(count(*)) over (), 4) as share_of_trips,
    round(median(duration_min), 2) as median_duration_min
from {{ ref('fct_late_night_trips') }}
group by all
