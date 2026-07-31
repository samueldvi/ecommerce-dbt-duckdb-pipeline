select
    cast(order_line_id as varchar) as order_line_id,
    cast(order_id as varchar) as order_id,
    cast(customer_id as varchar) as customer_id,
    cast(product_id as varchar) as product_id,
    cast(quantity as integer) as quantity,
    cast(unit_price as decimal(12, 2)) as unit_price,
    cast(order_status as varchar) as order_status,
    cast(order_date as date) as order_date,
    cast(updated_at as timestamptz) as updated_at,
    cast(sales_channel as varchar) as sales_channel,
    cast(ingestion_id as varchar) as source_ingestion_id,
    cast(source_file as varchar) as source_file,
    cast(source_run as varchar) as source_run,
    cast(source_row_number as bigint) as source_row_number,
    cast(row_hash as varchar) as source_row_hash,
    cast(ingested_at as timestamptz) as source_ingested_at,
    {{ order_payload_json() }} as canonical_payload
from {{ source('raw', 'order_lines') }}
