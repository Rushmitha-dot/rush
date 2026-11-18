select night, start_hour, count(*) as n
from {{ ref('mart_hourly_demand') }}
group by all
having count(*) > 1
