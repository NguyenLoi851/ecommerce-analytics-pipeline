{{ external_table_location() }}

-- Silver dimension: dim_sellers
-- Source: dev.bronze.sellers
-- Standardises city/state casing and casts zip code to string.
-- Sellers are already unique in the source; no deduplication required.

with source as (

    select * from {{ source('bronze', 'sellers') }}

),

final as (

    select
        -- keys
        seller_id,

        -- attributes
        cast(seller_zip_code_prefix as string)  as seller_zip_code_prefix,
        lower(trim(seller_city))               as seller_city,
        upper(trim(seller_state))              as seller_state,

        -- metadata
        _ingest_ts,
        _batch_id

    from source

)

select * from final
