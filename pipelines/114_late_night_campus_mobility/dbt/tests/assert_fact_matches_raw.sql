-- every loaded trip makes it to the fact, nothing extra appears
with r as (select night, count(*) as n from {{ source('bluebikes', 'bluebikes_trips') }} group by 1),
f as (select night, count(*) as n from {{ ref('fct_late_night_trips') }} group by 1)
select coalesce(r.night, f.night) as night, r.n as raw_trips, f.n as fact_trips
from r full join f using (night)
where r.n is distinct from f.n
