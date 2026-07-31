select
    dates.year_number as order_year,
    dates.iso_week_number,
    coalesce(customers.country_code, 'Unknown') as customer_region,
    count(distinct facts.order_id) as completed_order_count
from {{ ref('fct_order_lines') }} as facts
inner join {{ ref('dim_customers') }} as customers
    on facts.customer_sk = customers.customer_sk
inner join {{ ref('dim_date') }} as dates
    on facts.date_sk = dates.date_sk
where facts.revenue is not null
group by
    dates.year_number,
    dates.iso_week_number,
    customer_region
order by
    order_year,
    iso_week_number,
    customer_region
