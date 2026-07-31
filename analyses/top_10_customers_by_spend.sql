with customer_spend as (
    select
        customers.customer_id,
        customers.customer_name,
        cast(sum(facts.revenue) as decimal(18, 2)) as total_spend
    from {{ ref('fct_order_lines') }} as facts
    inner join {{ ref('dim_customers') }} as customers
        on facts.customer_sk = customers.customer_sk
    where facts.revenue is not null
    group by
        customers.customer_sk,
        customers.customer_id,
        customers.customer_name
),

ranked as (
    select
        *,
        row_number() over (
            order by total_spend desc, customer_id
        ) as spend_position
    from customer_spend
)

select * exclude (spend_position)
from ranked
where spend_position <= 10
order by
    total_spend desc,
    customer_id
