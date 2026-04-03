{{ external_table_location() }}

-- Silver dimension: dim_customers
-- Source: dev.bronze.customers
-- Deduplicates on customer_id (one row per order-level customer key),
-- standardises city/state casing, and casts zip code to string.

with source as (

    select * from {{ source('bronze', 'customers') }}

),

deduped as (

    -- Keep the most-recently ingested record per order-level customer key.
    select *,
        row_number() over (
            partition by customer_id
            order by _ingest_ts desc
        ) as _rn
    from source

),

final as (

    select
        -- keys
        customer_id,
        customer_unique_id,

        -- attributes
        cast(customer_zip_code_prefix as string)  as customer_zip_code_prefix,
        lower(trim(customer_city))                as customer_city,
        upper(trim(customer_state))               as customer_state,

        -- metadata
        _ingest_ts,
        _batch_id

    from deduped
    where _rn = 1

)

select * from final
