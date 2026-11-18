{{ config(severity='warn') }}
-- a handful of renamed stations is normal, many at once means ids were reassigned
select count(*) as renamed from {{ ref('dim_stations') }} where names_seen > 1
having count(*) > 10
