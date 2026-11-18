-- Which campus cluster each end of a trip is in, by the trip's own
-- coordinates. Classic bikes report the dock's location, e-bikes report GPS,
-- and e-bikes can end away from a dock with no station id at all.
with trips as (
    select * from {{ ref('stg_bluebikes_trips') }}
),

anchors as (
    select * from {{ ref('campus_anchors') }}
),

start_near as (
    select t.ride_id, a.cluster, {{ distance_m('t.start_lat', 't.start_lng', 'a.lat', 'a.lon') }} as d
    from trips t cross join anchors a
    qualify row_number() over (partition by t.ride_id order by d) = 1
),

end_near as (
    select t.ride_id, a.cluster, {{ distance_m('t.end_lat', 't.end_lng', 'a.lat', 'a.lon') }} as d
    from trips t cross join anchors a
    where t.end_lat is not null
    qualify row_number() over (partition by t.ride_id order by d) = 1
)

select
    t.*,
    case when s.d <= 1000 then s.cluster else 'outside' end as start_cluster,
    case when e.d is null then 'unknown' when e.d <= 1000 then e.cluster else 'outside' end as end_cluster,
    case
        when s.d <= 1000 and e.d <= 1000 then 'within_area'
        when s.d <= 1000 then 'leaving_area'
        else 'arriving_area'
    end as direction,
    case when t.duration_min > 0 then round(t.straight_line_m / 1000 / (t.duration_min / 60), 2) end
        as straight_line_kmh
from trips t
join start_near s using (ride_id)
left join end_near e using (ride_id)
