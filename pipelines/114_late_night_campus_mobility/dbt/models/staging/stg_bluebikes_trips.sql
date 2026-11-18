-- The files give local Boston time with no offset. On the night clocks fall
-- back, 1 to 2 a.m. happens twice. A trip that ends before it started there
-- ended in the second 1 a.m., so it gets the hour back. Other trips in that
-- hour can't be told apart and just get a flag.
with src as (
    select
        * exclude (ended_at),
        ended_at as ended_at_published,
        case when ended_at < started_at and hour(started_at) = 1 and hour(ended_at) = 1
             then ended_at + interval 1 hour else ended_at end as ended_at
    from {{ source('bluebikes', 'bluebikes_trips') }}
)

select
    night,
    ride_id,
    rideable_type,
    member_casual,
    started_at,
    ended_at,
    ended_at <> ended_at_published as ended_at_shifted_for_dst,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    source_month,
    round((epoch(ended_at) - epoch(started_at)) / 60.0, 2) as duration_min,
    case when end_lat is not null then {{ distance_m('start_lat', 'start_lng', 'end_lat', 'end_lng') }} end
        as straight_line_m,
    hour(started_at) as start_hour,
    time_bucket(interval 15 minute, started_at) as start_slot_15,
    -- US rule since 2007, first Sunday of November
    month(started_at) = 11 and dayofweek(started_at) = 0 and day(started_at) <= 7 and hour(started_at) = 1
        as in_repeated_hour
from src
