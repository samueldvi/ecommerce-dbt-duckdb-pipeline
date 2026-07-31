"""Aggregate-only, parameterized SQL consumed by the dashboard."""

FILTERED_FACTS_CTE = """
with filtered_facts as (
    select
        facts.*,
        dates.date_day,
        dates.year_number,
        dates.iso_week_number,
        products.category,
        customers.customer_id,
        customers.customer_name,
        coalesce(customers.country_code, 'Unknown') as customer_region
    from {schema}.fct_order_lines as facts
    inner join {schema}.dim_products as products
        on facts.product_sk = products.product_sk
    inner join {schema}.dim_customers as customers
        on facts.customer_sk = customers.customer_sk
    inner join {schema}.dim_date as dates
        on facts.date_sk = dates.date_sk
    where dates.date_day between ? and ?
      and (? is null or products.category = ?)
      and (
          ? is null
          or coalesce(customers.country_code, 'Unknown') = ?
      )
)
"""

KPI_QUERY = FILTERED_FACTS_CTE + """
select
    coalesce(sum(revenue), cast(0 as decimal(18, 2))) as total_revenue,
    count(distinct order_id) as distinct_orders,
    count(distinct order_id) filter (where revenue is not null)
        as revenue_orders,
    count(*) as order_lines,
    count(distinct customer_sk) filter (
        where customer_sk <> 'customer|unknown'
    ) as known_customers,
    case
        when count(distinct order_id) filter (where revenue is not null) = 0
            then cast(0 as decimal(18, 2))
        else cast(
            sum(revenue)
            / count(distinct order_id) filter (where revenue is not null)
            as decimal(18, 2)
        )
    end as average_revenue_order_value
from filtered_facts
"""

REVENUE_TREND_QUERY = FILTERED_FACTS_CTE + """
select
    date_day as order_date,
    cast(sum(revenue) as decimal(18, 2)) as total_revenue
from filtered_facts
where revenue is not null
group by date_day
order by date_day
"""

REVENUE_BY_CATEGORY_MONTH_QUERY = FILTERED_FACTS_CTE + """
select
    date_day::date - (day(date_day) - 1)::integer as revenue_month,
    category,
    cast(sum(revenue) as decimal(18, 2)) as total_revenue
from filtered_facts
where revenue is not null
group by revenue_month, category
order by revenue_month, category
"""

TOP_CUSTOMERS_QUERY = FILTERED_FACTS_CTE + """,
customer_spend as (
    select
        customer_id,
        customer_name,
        cast(sum(revenue) as decimal(18, 2)) as total_spend
    from filtered_facts
    where revenue is not null
    group by customer_sk, customer_id, customer_name
),
ranked as (
    select
        *,
        row_number() over (
            order by total_spend desc, customer_id
        ) as spend_position
    from customer_spend
)
select customer_id, customer_name, total_spend
from ranked
where spend_position <= 10
order by total_spend desc, customer_id
"""

ORDERS_BY_REGION_WEEK_QUERY = FILTERED_FACTS_CTE + """
select
    year_number as order_year,
    iso_week_number,
    customer_region,
    count(distinct order_id) as completed_order_count
from filtered_facts
where revenue is not null
group by year_number, iso_week_number, customer_region
order by order_year, iso_week_number, customer_region
"""

CATEGORY_RANKING_QUERY = FILTERED_FACTS_CTE + """,
monthly_category_revenue as (
    select
        date_day::date - (day(date_day) - 1)::integer as revenue_month,
        category,
        cast(sum(revenue) as decimal(18, 2)) as total_revenue
    from filtered_facts
    where revenue is not null
    group by revenue_month, category
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
order by revenue_month, revenue_position, category
"""

REGION_SUMMARY_QUERY = FILTERED_FACTS_CTE + """
select
    customer_region,
    count(distinct order_id) as completed_order_count
from filtered_facts
where revenue is not null
group by customer_region
order by completed_order_count desc, customer_region
"""

INCREMENTAL_LOAD_QUERY = """
with versions as (
    select
        order_line_id,
        source_file,
        case source_file
            when 'run_1/orders.csv' then 1
            when 'run_2/orders.csv' then 2
            when 'run_3/orders.csv' then 3
            when 'run_3/late_arriving/orders.csv' then 4
        end as batch_sequence,
        case source_file
            when 'run_1/orders.csv' then 'Run 1'
            when 'run_2/orders.csv' then 'Run 2'
            when 'run_3/orders.csv' then 'Run 3'
            when 'run_3/late_arriving/orders.csv' then 'Late-arriving'
        end as batch_label
    from raw.order_lines
    where source_file in (
        'run_1/orders.csv',
        'run_2/orders.csv',
        'run_3/orders.csv',
        'run_3/late_arriving/orders.csv'
    )
),
batch_metrics as (
    select
        batch_sequence,
        batch_label,
        source_file,
        count(*) as received_rows,
        count(distinct order_line_id) filter (
            where batch_sequence < 4
                or order_line_id not in (
                    select order_line_id
                    from versions
                    where batch_sequence < 4
                )
        ) as new_keys,
        count(distinct order_line_id) filter (
            where batch_sequence = 4
              and order_line_id in (
                  select order_line_id
                  from versions
                  where batch_sequence = 1
              )
        ) as corrections
    from versions
    group by batch_sequence, batch_label, source_file
)
select
    metrics.batch_sequence,
    metrics.batch_label,
    metrics.received_rows,
    metrics.new_keys,
    metrics.corrections,
    (
        select count(distinct cumulative.order_line_id)
        from versions as cumulative
        where cumulative.batch_sequence <= metrics.batch_sequence
    ) as cumulative_keys,
    manifest.ingested_at
from batch_metrics as metrics
left join raw.ingestion_manifest as manifest
    on metrics.source_file = manifest.source_file
order by metrics.batch_sequence
"""

DATA_QUALITY_QUERY = """
select
    count(*) - count(distinct order_line_id) as duplicate_current_keys,
    count(*) filter (where unit_price is null) as null_unit_prices,
    count(*) filter (where unit_price < 0) as negative_unit_prices,
    count(*) filter (where quantity <= 0) as non_positive_quantities,
    count(*) filter (where order_status = 'cancelled') as cancelled_lines,
    count(*) filter (
        where order_status not in ('completed', 'cancelled')
    ) as invalid_order_statuses,
    count(*) filter (
        where sales_channel is not null
          and sales_channel not in ('web', 'mobile', 'marketplace')
    ) as invalid_sales_channels,
    count(*) filter (where customer_sk = 'customer|unknown')
        as unknown_customers,
    count(*) filter (where product_sk = 'product|unknown')
        as unknown_products,
    count(*) filter (where not is_economically_valid)
        as economically_invalid_lines
from {schema}.fct_order_lines
"""

METADATA_QUERY = """
select
    min(dates.date_day) as period_start,
    max(dates.date_day) as period_end,
    (select max(ingested_at) from raw.ingestion_manifest) as last_ingested_at
from {schema}.fct_order_lines as facts
inner join {schema}.dim_date as dates on facts.date_sk = dates.date_sk
"""

CATEGORY_OPTIONS_QUERY = """
select distinct category
from {schema}.dim_products
where product_sk <> 'product|unknown'
order by category
"""

REGION_OPTIONS_QUERY = """
select distinct coalesce(country_code, 'Unknown') as customer_region
from {schema}.dim_customers
order by customer_region
"""

FILTERED_QUERY_NAMES = frozenset(
    {
        "kpis",
        "revenue_trend",
        "revenue_by_category_month",
        "top_customers",
        "orders_by_region_week",
        "category_ranking",
        "region_summary",
    }
)

QUERY_BY_NAME = {
    "kpis": KPI_QUERY,
    "revenue_trend": REVENUE_TREND_QUERY,
    "revenue_by_category_month": REVENUE_BY_CATEGORY_MONTH_QUERY,
    "top_customers": TOP_CUSTOMERS_QUERY,
    "orders_by_region_week": ORDERS_BY_REGION_WEEK_QUERY,
    "category_ranking": CATEGORY_RANKING_QUERY,
    "region_summary": REGION_SUMMARY_QUERY,
    "incremental_loads": INCREMENTAL_LOAD_QUERY,
    "data_quality": DATA_QUALITY_QUERY,
}
