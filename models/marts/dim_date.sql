with calendar as (
    select cast(date_value as date) as date_day
    from generate_series(
        date '2025-01-01',
        date '2025-01-30',
        interval 1 day
    ) as generated(date_value)
),

unknown_member as (
    select
        0::integer as date_sk,
        null::date as date_day,
        null::integer as year_number,
        null::integer as month_number,
        null::varchar as month_name,
        null::integer as iso_week_number,
        null::integer as day_of_month
),

resolved_members as (
    select
        cast(strftime(date_day, '%Y%m%d') as integer) as date_sk,
        date_day,
        year(date_day)::integer as year_number,
        month(date_day)::integer as month_number,
        monthname(date_day)::varchar as month_name,
        week(date_day)::integer as iso_week_number,
        day(date_day)::integer as day_of_month
    from calendar
)

select * from unknown_member
union all by name
select * from resolved_members
