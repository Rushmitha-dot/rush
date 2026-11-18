-- a trip belongs to the night it started in, 10 p.m. on the night's date to 3 a.m. the next day
select ride_id, night, started_at
from {{ ref('fct_late_night_trips') }}
where started_at < night + interval 22 hour or started_at >= night + interval 27 hour
