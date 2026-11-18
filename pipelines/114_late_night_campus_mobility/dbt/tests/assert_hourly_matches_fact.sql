-- the incremental hourly mart has to agree with the fact table it was built from
with f as (select night, count(*) as n from {{ ref('fct_late_night_trips') }} group by 1),
h as (select night, sum(trips) as n from {{ ref('mart_hourly_demand') }} group by 1)
select coalesce(f.night, h.night) as night, f.n as fact_trips, h.n as hourly_trips
from f full join h using (night)
where f.n is distinct from h.n
