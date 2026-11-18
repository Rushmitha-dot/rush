-- Straight-line speed of short campus trips by hour, next to the MBTA's
-- scheduled bus speed in the same area and hour (along the route, which is
-- longer than a straight line, so bus speed is divided by the detour factor
-- to compare like with like).
with bikes as (
    select night_type, start_hour, rideable_type, median(straight_line_kmh) as kmh, count(*) as trips
    from {{ ref('fct_late_night_trips') }}
    where direction = 'within_area' and straight_line_m between 500 and 5000 and duration_min between 2 and 60
    group by all
)

select
    b.night_type,
    b.start_hour,
    (b.start_hour + 2) % 24 as hour_order,
    round(any_value(b.kmh) filter (where rideable_type = 'classic_bike'), 2) as classic_bike_kmh,
    round(any_value(b.kmh) filter (where rideable_type = 'electric_bike'), 2) as ebike_kmh,
    sum(b.trips) as trips,
    round(any_value(g.bus_kmh) / any_value(g.detour_factor), 2) as bus_straight_line_kmh,
    any_value(g.bus_kmh) as bus_route_kmh
from bikes b
left join {{ ref('stg_gtfs_night_speeds') }} g on g.night_type = b.night_type and g.hour = b.start_hour
group by b.night_type, b.start_hour
