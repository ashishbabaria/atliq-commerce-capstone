-- Calendar spine 2024-01-01 .. 2026-12-31 (covers order dates + earliest signups).
with days as (
    select explode(
        sequence(to_date('2024-01-01'), to_date('2026-12-31'), interval 1 day)
    ) as date_day
)
select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
    date_day,
    day(date_day)                                  as day,
    month(date_day)                                as month,
    date_format(date_day, 'MMMM')                  as month_name,
    quarter(date_day)                              as quarter,
    year(date_day)                                 as year,
    date_format(date_day, 'EEEE')                  as weekday_name,
    case when dayofweek(date_day) in (1,7) then true else false end as is_weekend
from days