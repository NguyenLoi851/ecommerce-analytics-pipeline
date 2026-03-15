-- Silver dimension: dim_products
-- Source: dev.bronze.products + dev.bronze.product_category_name_translation
-- Joins in the English category name and coalesces nulls in dimension attributes.

with products as (

    select * from {{ source('bronze', 'products') }}

),

translations as (

    select * from {{ source('bronze', 'product_category_name_translation') }}

),

final as (

    select
        -- keys
        p.product_id,

        -- category
        p.product_category_name,
        coalesce(t.product_category_name_english, 'unknown')  as product_category_name_english,

        -- dimensions
        cast(p.product_name_length        as int)             as product_name_length,
        cast(p.product_description_length as int)             as product_description_length,
        cast(p.product_photos_qty         as int)             as product_photos_qty,
        cast(p.product_weight_g           as double)          as product_weight_g,
        cast(p.product_length_cm          as double)          as product_length_cm,
        cast(p.product_height_cm          as double)          as product_height_cm,
        cast(p.product_width_cm           as double)          as product_width_cm,

        -- metadata
        p._ingest_ts,
        p._batch_id

    from products as p
    left join translations as t
        on p.product_category_name = t.product_category_name

)

select * from final
