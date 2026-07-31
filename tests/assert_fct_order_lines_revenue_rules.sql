select *
from {{ ref('fct_order_lines') }}
where
    is_economically_valid is distinct from (
        order_status = 'completed'
        and quantity > 0
        and unit_price is not null
        and unit_price >= 0
    )
    or revenue is distinct from (
        case
            when
                order_status = 'completed'
                and quantity > 0
                and unit_price is not null
                and unit_price >= 0
            then cast(quantity * unit_price as decimal(18, 2))
            else null
        end
    )
