select
    cast(customer_id as varchar) as customer_id,
    cast(customer_name as varchar) as customer_name,
    cast(email as varchar) as email,
    cast(country_code as varchar) as country_code,
    cast(updated_at as timestamptz) as updated_at,
    cast(ingestion_id as varchar) as source_ingestion_id,
    cast(source_file as varchar) as source_file,
    cast(source_run as varchar) as source_run,
    cast(source_row_number as bigint) as source_row_number,
    cast(row_hash as varchar) as source_row_hash,
    cast(ingested_at as timestamptz) as source_ingested_at
from {{ source('raw', 'customers') }}
