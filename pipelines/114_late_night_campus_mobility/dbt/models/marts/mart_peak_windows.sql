-- Rolling 60 minute demand in 15 minute steps, averaged over nights.
-- Windows starting after 2 a.m. would be cut off at 3 a.m., so they're left out.
with slots as (
    select night, night_type, start_slot_15 as slot_start, count(*) as trips
    from {{ ref('fct_late_night_trips') }}
    group by all
),

rolling as (
    select *, sum(trips) over (partition by night order by slot_start
                               range between current row and interval 45 minute following) as trips_60
    from slots
),

by_window as (
    select
        night_type,
        strftime(slot_start, '%H:%M') as window_start,
        count(distinct night) as nights_with_trips,
        round(avg(trips_60), 1) as avg_trips_60
    from rolling
    where hour(slot_start) <> 2
    group by night_type, strftime(slot_start, '%H:%M')
)

select
    *,
    rank() over (partition by night_type order by avg_trips_60 desc) as demand_rank,
    rank() over (partition by night_type order by avg_trips_60 desc) <= 3 as is_peak
from by_window
