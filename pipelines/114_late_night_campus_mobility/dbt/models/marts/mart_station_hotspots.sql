-- Departures and arrivals per station. Net flow per night is arrivals minus
-- departures: a station that fills up overnight needs bikes taken away before
-- the morning rush, one that drains needs bikes brought in.
with trips as (
    select * from {{ ref('fct_late_night_trips') }}
),

nights as (
    select night_type, count(distinct night) as nights from trips group by night_type
),

moves as (
    select start_station_id as station_id, night_type, 1 as departures, 0 as arrivals from trips
    where start_station_id is not null
    union all
    select end_station_id, night_type, 0, 1 from trips where end_station_id is not null
)

select
    s.station_id,
    s.station_name,
    s.cluster,
    s.lat,
    s.lon,
    s.capacity,
    m.night_type,
    sum(m.departures) as departures,
    sum(m.arrivals) as arrivals,
    round(sum(m.departures) / any_value(n.nights), 2) as departures_per_night,
    round(sum(m.arrivals) / any_value(n.nights), 2) as arrivals_per_night,
    round((sum(m.arrivals) - sum(m.departures)) / any_value(n.nights), 2) as net_flow_per_night,
    -- how hard the docks get used overnight, a stand-in for dock availability history we don't have
    round((sum(m.departures) + sum(m.arrivals)) / any_value(n.nights) / nullif(s.capacity, 0), 3)
        as trips_per_dock_per_night,
    rank() over (partition by m.night_type order by sum(m.departures) desc) as departure_rank
from moves m
join {{ ref('int_stations') }} s using (station_id)
join nights n on n.night_type = m.night_type
where s.cluster <> 'outside'
group by s.station_id, s.station_name, s.cluster, s.lat, s.lon, s.capacity, m.night_type
