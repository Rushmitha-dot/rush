select
    short_name as station_id,
    name as station_name,
    lat,
    lon,
    capacity,
    feed_updated_at
from {{ source('bluebikes', 'bluebikes_stations') }}
where short_name is not null
qualify row_number() over (partition by short_name order by capacity desc nulls last) = 1
