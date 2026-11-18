{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='night') }}

-- One row per late-night trip touching the campus area. Rebuilt a night at a
-- time; a rerun of a night deletes and inserts that night only.
select
    t.*,
    c.night_type,
    c.term,
    c.period,
    c.holiday,
    case
        when t.duration_min < 5 then '00-05'
        when t.duration_min < 10 then '05-10'
        when t.duration_min < 15 then '10-15'
        when t.duration_min < 20 then '15-20'
        when t.duration_min < 30 then '20-30'
        else '30+'
    end as duration_bucket,
    -- 10 p.m. sorts first, 2 a.m. last
    (t.start_hour + 2) % 24 as hour_order
from {{ ref('int_trips_with_area') }} t
left join {{ ref('stg_academic_calendar') }} c using (night)
where {{ night_filter('t.night') }}
