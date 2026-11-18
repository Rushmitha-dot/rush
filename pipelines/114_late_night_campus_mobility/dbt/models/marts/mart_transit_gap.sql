-- Late-night bike departures per campus station next to how much scheduled
-- MBTA service leaves from within 400 m after 10 p.m. Busy stations with
-- little transit nearby are where a night shuttle stop would help most.
with stations as (
    select station_id, station_name, cluster, lat, lon,
           sum(departures_per_night) filter (where night_type = 'weeknight') as weeknight_departures_per_night,
           sum(departures_per_night) filter (where night_type = 'weekend') as weekend_departures_per_night
    from {{ ref('mart_station_hotspots') }}
    group by all
),

pairs as (
    select s.station_id, m.stop_id, m.weeknight_departures_per_night, m.weekend_departures_per_night,
           {{ distance_m('s.lat', 's.lon', 'm.lat', 'm.lon') }} as d
    from stations s cross join {{ ref('stg_mbta_night_stops') }} m
)

select
    s.*,
    round(min(p.d)) as meters_to_nearest_night_stop,
    coalesce(sum(p.weeknight_departures_per_night) filter (where p.d <= 400), 0) as mbta_weeknight_departures_400m,
    coalesce(sum(p.weekend_departures_per_night) filter (where p.d <= 400), 0) as mbta_weekend_departures_400m
from stations s
left join pairs p using (station_id)
group by all
