select
    cast(product_id as varchar) as product_id,
    cast(product_name as varchar) as product_name,
    cast(category as varchar) as category,
    cast(ingestion_id as varchar) as source_ingestion_id,
    cast(source_file as varchar) as source_file,
    cast(source_run as varchar) as source_run,
    cast(source_row_number as bigint) as source_row_number,
    cast(row_hash as varchar) as source_row_hash,
    cast(ingested_at as timestamptz) as source_ingested_at
from {{ source('raw', 'products') }}
