with ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by
                updated_at desc,
                {{ customer_payload_tuple() }} desc,
                source_file desc,
                source_row_number desc
        ) as version_rank
    from {{ ref('stg_customers') }}
)

select * exclude (version_rank)
from ranked
where version_rank = 1
