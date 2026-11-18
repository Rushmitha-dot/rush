-- One row per station id seen in the trips. The name is the latest one used,
-- since a few ids were renamed (Mass Ave T Station became Mass Ave T Stop).
-- Location is the median of classic bike trip ends, which is the dock.
with ends as (
    select start_station_id as station_id, start_station_name as station_name, started_at as seen_at,
           start_lat as lat, start_lng as lon, rideable_type
    from {{ ref('stg_bluebikes_trips') }} where start_station_id is not null
    union all
    select end_station_id, end_station_name, ended_at, end_lat, end_lng, rideable_type
    from {{ ref('stg_bluebikes_trips') }} where end_station_id is not null
),

per_station as (
    select
        station_id,
        arg_max(station_name, seen_at) as station_name,
        count(distinct station_name) as names_seen,
        coalesce(median(lat) filter (where rideable_type = 'classic_bike'), median(lat)) as lat,
        coalesce(median(lon) filter (where rideable_type = 'classic_bike'), median(lon)) as lon
    from ends
    group by station_id
),

nearest as (
    select p.station_id, a.cluster, {{ distance_m('p.lat', 'p.lon', 'a.lat', 'a.lon') }} as d
    from per_station p cross join {{ ref('campus_anchors') }} a
    qualify row_number() over (partition by p.station_id order by d) = 1
)

select
    p.*,
    case when n.d <= 1000 then n.cluster else 'outside' end as cluster,
    round(n.d) as meters_to_nearest_anchor,
    g.capacity
from per_station p
join nearest n using (station_id)
left join {{ ref('stg_bluebikes_stations') }} g using (station_id)
