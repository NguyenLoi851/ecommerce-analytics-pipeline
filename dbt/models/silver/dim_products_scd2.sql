{{ config(materialized='view') }}

-- Silver SCD Type 2 dimension: dim_products_scd2
-- Backed by dbt snapshot `snap_dim_products_scd2`.

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
    product_width_cm,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(dbt_valid_to as timestamp) as valid_to,
    dbt_valid_to is null as is_current,
    _ingest_ts,
    _batch_id
from {{ ref('snap_dim_products_scd2') }}
