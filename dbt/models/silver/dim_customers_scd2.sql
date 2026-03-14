{{ config(materialized='view') }}

-- Silver SCD Type 2 dimension: dim_customers_scd2
-- Backed by dbt snapshot `snap_dim_customers_scd2`.

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(dbt_valid_to as timestamp) as valid_to,
    dbt_valid_to is null as is_current,
    _ingest_ts,
    _batch_id
from {{ ref('snap_dim_customers_scd2') }}
