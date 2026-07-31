with current_customers as (
    select * from {{ ref('int_current_customers') }}
),

unknown_member as (
    select
        'customer|unknown'::varchar as customer_sk,
        '__UNKNOWN__'::varchar as customer_id,
        'Unknown'::varchar as customer_name,
        null::varchar as email,
        null::varchar as country_code,
        timestamptz '1970-01-01 00:00:00+00' as updated_at
),

resolved_members as (
    select
        md5('customer|' || customer_id) as customer_sk,
        customer_id,
        customer_name,
        email,
        country_code,
        updated_at
    from current_customers
)

select * from unknown_member
union all by name
select * from resolved_members
