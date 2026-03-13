-- Silver fact: fct_order_items
-- Source: dev.bronze.order_items
-- One row per (order_id, order_item_id). Casts numeric prices and adds a
-- total_item_value derived column (price + freight_value).

with source as (

    select * from {{ source('bronze', 'order_items') }}

),

final as (

    select
        -- keys
        order_id,
        cast(order_item_id    as int)     as order_item_id,
        product_id,
        seller_id,

        -- timestamps
        cast(shipping_limit_date as timestamp)  as shipping_limit_date,

        -- financials
        cast(price         as double)     as price,
        cast(freight_value as double)     as freight_value,
        cast(price         as double)
            + cast(freight_value as double)     as total_item_value,

        -- metadata
        _ingest_ts,
        _batch_id

    from source

)

select * from final
