{% macro customer_payload_tuple(alias='') -%}
row(
    cast({{ alias }}customer_name as varchar),
    coalesce(cast({{ alias }}email as varchar), '<NULL>'),
    coalesce(cast({{ alias }}country_code as varchar), '<NULL>')
)
{%- endmacro %}

{% macro product_payload_tuple(alias='') -%}
row(
    cast({{ alias }}product_name as varchar),
    cast({{ alias }}category as varchar)
)
{%- endmacro %}

{% macro order_payload_tuple(alias='') -%}
row(
    cast({{ alias }}order_id as varchar),
    cast({{ alias }}customer_id as varchar),
    cast({{ alias }}product_id as varchar),
    cast({{ alias }}quantity as varchar),
    coalesce(cast({{ alias }}unit_price as varchar), '<NULL>'),
    cast({{ alias }}order_status as varchar),
    cast({{ alias }}order_date as varchar),
    strftime({{ alias }}updated_at at time zone 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ'),
    coalesce(cast({{ alias }}sales_channel as varchar), '<NULL>')
)
{%- endmacro %}

{% macro order_payload_json(alias='') -%}
to_json(list_value(
    cast({{ alias }}order_id as varchar),
    cast({{ alias }}customer_id as varchar),
    cast({{ alias }}product_id as varchar),
    cast({{ alias }}quantity as varchar),
    coalesce(cast({{ alias }}unit_price as varchar), '<NULL>'),
    cast({{ alias }}order_status as varchar),
    cast({{ alias }}order_date as varchar),
    strftime({{ alias }}updated_at at time zone 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ'),
    coalesce(cast({{ alias }}sales_channel as varchar), '<NULL>')
))
{%- endmacro %}
