{% macro scd2_close_current_customers_merge(target_relation) %}
{% if execute %}
    {% set rel = adapter.get_relation(database=target_relation.database, schema=target_relation.schema, identifier=target_relation.identifier) %}
    {% if rel is not none %}
MERGE INTO {{ target_relation }} AS tgt
USING (
    with latest_source as (
        select
            customer_id,
            customer_unique_id,
            cast(customer_zip_code_prefix as string) as customer_zip_code_prefix,
            lower(trim(customer_city)) as customer_city,
            upper(trim(customer_state)) as customer_state,
            _ingest_ts,
            row_number() over (partition by customer_id order by _ingest_ts desc) as _rn
        from {{ source('bronze', 'customers') }}
    )
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from latest_source
    where _rn = 1
) AS src
ON tgt.customer_id = src.customer_id
AND tgt.is_current = true
WHEN MATCHED
AND (
    coalesce(tgt.customer_unique_id, '') <> coalesce(src.customer_unique_id, '')
    OR coalesce(tgt.customer_zip_code_prefix, '') <> coalesce(src.customer_zip_code_prefix, '')
    OR coalesce(tgt.customer_city, '') <> coalesce(src.customer_city, '')
    OR coalesce(tgt.customer_state, '') <> coalesce(src.customer_state, '')
)
THEN UPDATE SET
    valid_to = current_timestamp(),
    is_current = false
    {% else %}
SELECT 1
    {% endif %}
{% else %}
SELECT 1
{% endif %}
{% endmacro %}


{% macro scd2_close_current_products_merge(target_relation) %}
{% if execute %}
    {% set rel = adapter.get_relation(database=target_relation.database, schema=target_relation.schema, identifier=target_relation.identifier) %}
    {% if rel is not none %}
MERGE INTO {{ target_relation }} AS tgt
USING (
    with products as (
        select * from {{ source('bronze', 'products') }}
    ),
    translations as (
        select * from {{ source('bronze', 'product_category_name_translation') }}
    ),
    latest_source as (
        select
            p.product_id,
            p.product_category_name,
            coalesce(t.product_category_name_english, 'unknown') as product_category_name_english,
            cast(p.product_name_lenght as int) as product_name_length,
            cast(p.product_description_lenght as int) as product_description_length,
            cast(p.product_photos_qty as int) as product_photos_qty,
            cast(p.product_weight_g as double) as product_weight_g,
            cast(p.product_length_cm as double) as product_length_cm,
            cast(p.product_height_cm as double) as product_height_cm,
            cast(p.product_width_cm as double) as product_width_cm,
            p._ingest_ts,
            row_number() over (partition by p.product_id order by p._ingest_ts desc) as _rn
        from products as p
        left join translations as t
            on p.product_category_name = t.product_category_name
    )
    select
        product_id,
        product_category_name,
        product_category_name_english,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    from latest_source
    where _rn = 1
) AS src
ON tgt.product_id = src.product_id
AND tgt.is_current = true
WHEN MATCHED
AND (
    coalesce(tgt.product_category_name, '') <> coalesce(src.product_category_name, '')
    OR coalesce(tgt.product_category_name_english, '') <> coalesce(src.product_category_name_english, '')
    OR coalesce(tgt.product_name_length, -1) <> coalesce(src.product_name_length, -1)
    OR coalesce(tgt.product_description_length, -1) <> coalesce(src.product_description_length, -1)
    OR coalesce(tgt.product_photos_qty, -1) <> coalesce(src.product_photos_qty, -1)
    OR coalesce(tgt.product_weight_g, -1.0) <> coalesce(src.product_weight_g, -1.0)
    OR coalesce(tgt.product_length_cm, -1.0) <> coalesce(src.product_length_cm, -1.0)
    OR coalesce(tgt.product_height_cm, -1.0) <> coalesce(src.product_height_cm, -1.0)
    OR coalesce(tgt.product_width_cm, -1.0) <> coalesce(src.product_width_cm, -1.0)
)
THEN UPDATE SET
    valid_to = current_timestamp(),
    is_current = false
    {% else %}
SELECT 1
    {% endif %}
{% else %}
SELECT 1
{% endif %}
{% endmacro %}
