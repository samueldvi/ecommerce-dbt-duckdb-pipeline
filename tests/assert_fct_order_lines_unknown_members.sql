with facts as (
    select * from {{ ref('fct_order_lines') }}
),

customers as (
    select customer_id from {{ ref('dim_customers') }}
),

products as (
    select product_id from {{ ref('dim_products') }}
)

select facts.*
from facts
left join customers using (customer_id)
left join products using (product_id)
where
    (customers.customer_id is null and facts.customer_sk <> 'customer|unknown')
    or (products.product_id is null and facts.product_sk <> 'product|unknown')
