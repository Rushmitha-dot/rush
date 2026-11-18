-- One row per night with two rolling windows. The 7 night window shows the
-- trend; the same weekday window is the fair comparison because Friday and
-- Saturday nights run two to three times a Monday.
with nightly as (
    select
        night,
        any_value(night_type) as night_type,
        any_value(term) as term,
        any_value(period) as period,
        dayname(night) as weekday,
        sum(trips) as trips,
        sum(member_trips) as member_trips,
        sum(ebike_trips) as ebike_trips,
        sum(leaving_area) as leaving_area,
        sum(arriving_area) as arriving_area
    from {{ ref('mart_hourly_demand') }}
    group by night
)

select
    *,
    round(member_trips / trips, 4) as member_share,
    round(ebike_trips / trips, 4) as ebike_share,
    round(avg(trips) over (order by night range between interval 6 day preceding and current row), 1)
        as trips_rolling_7_nights,
    round(avg(trips) over (partition by weekday order by night
        range between interval 28 day preceding and interval 1 day preceding), 1) as trips_same_weekday_prev_4
from nightly
