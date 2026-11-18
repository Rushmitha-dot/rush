select night_type, hour, segments, routes, bus_kmh, detour_factor, feed_version
from {{ ref('gtfs_night_speeds') }}
