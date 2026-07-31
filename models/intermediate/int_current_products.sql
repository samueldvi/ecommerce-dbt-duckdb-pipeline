with ranked as (
    select
        *,
        row_number() over (
            partition by product_id
            order by
                {{ product_payload_tuple() }} desc,
                source_file desc,
                source_row_number desc
        ) as version_rank
    from {{ ref('stg_products') }}
)

select * exclude (version_rank)
from ranked
where version_rank = 1
