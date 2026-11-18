-- The files give local Boston time with no offset. On the night clocks fall
-- back, 1 to 2 a.m. happens twice and a trip across it can look an hour
-- longer or shorter than it was, so those trips get a flag.
select
    night,
    ride_id,
    rideable_type,
    member_casual,
    started_at,
    ended_at,
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
from {{ source('bluebikes', 'bluebikes_trips') }}
