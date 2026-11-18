-- departures are summed over a reference week (5 weeknights, 2 weekend nights)
select
    stop_id,
    stop_name,
    lat,
    lon,
    round(weeknight_departures / 5.0, 1) as weeknight_departures_per_night,
    round(weekend_departures / 2.0, 1) as weekend_departures_per_night,
    feed_version
from {{ ref('mbta_night_stops') }}
