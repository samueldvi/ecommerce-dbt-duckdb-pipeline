{{
    config(
        materialized='incremental',
        unique_key='order_line_id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns'
    )
}}

with source_versions as (
    select *
    from {{ ref('stg_order_lines') }}
    {% if is_incremental() %}
    where source_ingested_at >= (
        select coalesce(
            max(source_ingested_at),
            timestamptz '1970-01-01 00:00:00+00'
        )
        from {{ this }}
    )
    {% endif %}
),

ranked_incoming as (
    select
        *,
        row_number() over (
            partition by order_line_id
            order by
                updated_at desc,
                {{ order_payload_tuple() }} desc,
                source_ingested_at desc,
                source_file desc,
                source_row_number desc
        ) as version_rank
    from source_versions
),

incoming_current as (
    select * exclude (version_rank)
    from ranked_incoming
    where version_rank = 1
),

dimensionally_resolved as (
    select
        incoming.order_line_id,
        incoming.order_id,
        incoming.customer_id,
        incoming.product_id,
        coalesce(customers.customer_sk, 'customer|unknown') as customer_sk,
        coalesce(products.product_sk, 'product|unknown') as product_sk,
        coalesce(dates.date_sk, 0) as date_sk,
        incoming.quantity,
        incoming.unit_price,
        incoming.order_status,
        incoming.order_date,
        incoming.updated_at,
        incoming.sales_channel,
        (
            incoming.order_status = 'completed'
            and incoming.quantity > 0
            and incoming.unit_price is not null
            and incoming.unit_price >= 0
        ) as is_economically_valid,
        case
            when
                incoming.order_status = 'completed'
                and incoming.quantity > 0
                and incoming.unit_price is not null
                and incoming.unit_price >= 0
            then cast(incoming.quantity * incoming.unit_price as decimal(18, 2))
            else null
        end as revenue,
        incoming.canonical_payload,
        incoming.source_ingestion_id,
        incoming.source_file,
        incoming.source_run,
        incoming.source_row_number,
        incoming.source_row_hash,
        incoming.source_ingested_at
    from incoming_current as incoming
    left join {{ ref('dim_customers') }} as customers
        on incoming.customer_id = customers.customer_id
    left join {{ ref('dim_products') }} as products
        on incoming.product_id = products.product_id
    left join {{ ref('dim_date') }} as dates
        on incoming.order_date = dates.date_day
),

prevalent_changes as (
    select incoming.*
    from dimensionally_resolved as incoming
    {% if is_incremental() %}
    left join {{ this }} as existing
        on incoming.order_line_id = existing.order_line_id
    where
        existing.order_line_id is null
        or incoming.updated_at > existing.updated_at
        or (
            incoming.updated_at = existing.updated_at
            and {{ order_payload_tuple('incoming.') }}
                > {{ order_payload_tuple('existing.') }}
        )
    {% endif %}
)

select * from prevalent_changes
