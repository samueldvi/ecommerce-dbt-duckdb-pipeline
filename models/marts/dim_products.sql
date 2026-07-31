with current_products as (
    select * from {{ ref('int_current_products') }}
),

unknown_member as (
    select
        'product|unknown'::varchar as product_sk,
        '__UNKNOWN__'::varchar as product_id,
        'Unknown'::varchar as product_name,
        'Unknown'::varchar as category
),

resolved_members as (
    select
        md5('product|' || product_id) as product_sk,
        product_id,
        product_name,
        category
    from current_products
)

select * from unknown_member
union all by name
select * from resolved_members
