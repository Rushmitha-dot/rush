-- What the load and the models set aside or flagged, per night.
with ledger as (
    select night, trips, quarantined, status from {{ source('ops', 'load_ledger') }}
),

q as (
    select night, string_agg(reason || ' ' || n, ', ' order by reason) as quarantine_reasons
    from (select night, reason, count(*) as n from {{ source('bluebikes', 'bluebikes_trips_quarantine') }} group by all)
    group by night
),

t as (
    select
        night,
        count(*) filter (where end_station_id is null) as trips_without_end_station,
        count(*) filter (where in_repeated_hour) as trips_in_repeated_hour
    from {{ ref('fct_late_night_trips') }}
    group by night
)

select
    l.night,
    l.status,
    l.trips,
    l.quarantined,
    q.quarantine_reasons,
    coalesce(t.trips_without_end_station, 0) as trips_without_end_station,
    coalesce(t.trips_in_repeated_hour, 0) as trips_in_repeated_hour
from ledger l
left join q using (night)
left join t using (night)
