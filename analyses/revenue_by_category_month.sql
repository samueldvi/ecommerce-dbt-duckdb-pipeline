select
    dates.date_day::date - (day(dates.date_day) - 1)::integer as revenue_month,
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
order by
    revenue_month,
    products.category
