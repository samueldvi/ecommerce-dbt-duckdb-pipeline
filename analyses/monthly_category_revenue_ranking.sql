with monthly_category_revenue as (
    select
        dates.date_day::date - (day(dates.date_day) - 1)::integer
            as revenue_month,
        products.category,
        cast(sum(facts.revenue) as decimal(18, 2)) as total_revenue
    from {{ ref('fct_order_lines') }} as facts
    inner join {{ ref('dim_products') }} as products
        on facts.product_sk = products.product_sk
    inner join {{ ref('dim_date') }} as dates
        on facts.date_sk = dates.date_sk
    where facts.revenue is not null
    group by
        revenue_month,
        products.category
),

ranked as (
    select
        revenue_month,
        category,
        total_revenue,
        dense_rank() over (
            partition by revenue_month
            order by total_revenue desc
        ) as revenue_position
    from monthly_category_revenue
)

select *
from ranked
order by
    revenue_month,
    revenue_position,
    category
