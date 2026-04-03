{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='scd_id',
        on_schema_change='sync_all_columns',
        post_hook="{{ scd2_close_current_customers_merge(this) }}",
        external_location='olist_delta_ext_loc'
    )
}}

with latest_source as (

    select
        customer_id,
        customer_unique_id,
        cast(customer_zip_code_prefix as string) as customer_zip_code_prefix,
        lower(trim(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state,
        _ingest_ts,
        _batch_id,
        row_number() over (partition by customer_id order by _ingest_ts desc) as _rn
    from {{ source('bronze', 'customers') }}

),

source_current as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        _ingest_ts,
        _batch_id
    from latest_source
    where _rn = 1

),

target_current as (

    {% if is_incremental() %}
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from {{ this }}
    where is_current = true
    {% else %}
    select
        cast(null as string) as customer_id,
        cast(null as string) as customer_unique_id,
        cast(null as string) as customer_zip_code_prefix,
        cast(null as string) as customer_city,
        cast(null as string) as customer_state
    where 1 = 0
    {% endif %}

),

records_to_insert as (

    select
        s.customer_id,
        s.customer_unique_id,
        s.customer_zip_code_prefix,
        s.customer_city,
        s.customer_state,
        current_timestamp() as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current,
        s._ingest_ts,
        s._batch_id,
        sha2(concat_ws('||', s.customer_id, cast(current_timestamp() as string)), 256) as scd_id
    from source_current as s
    left join target_current as t
        on s.customer_id = t.customer_id
    where t.customer_id is null
       or coalesce(s.customer_unique_id, '') <> coalesce(t.customer_unique_id, '')
       or coalesce(s.customer_zip_code_prefix, '') <> coalesce(t.customer_zip_code_prefix, '')
       or coalesce(s.customer_city, '') <> coalesce(t.customer_city, '')
       or coalesce(s.customer_state, '') <> coalesce(t.customer_state, '')

)

select * from records_to_insert
