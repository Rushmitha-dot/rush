{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='night') }}

select
    night,
    start_hour,
    any_value(hour_order) as hour_order,
    any_value(night_type) as night_type,
    any_value(term) as term,
    any_value(period) as period,
    count(*) as trips,
    count(*) filter (where member_casual = 'member') as member_trips,
    count(*) filter (where rideable_type = 'electric_bike') as ebike_trips,
    count(*) filter (where direction = 'leaving_area') as leaving_area,
    count(*) filter (where direction = 'arriving_area') as arriving_area,
    count(*) filter (where direction = 'within_area') as within_area,
    round(median(duration_min), 2) as median_duration_min
from {{ ref('fct_late_night_trips') }}
where {{ night_filter() }}
group by night, start_hour
