select *
from {{ ref('fct_order_lines') }}
where
    (sales_channel is not null and sales_channel not in ('web', 'mobile', 'marketplace'))
    or (source_file <> 'run_3/orders.csv' and sales_channel is not null)
