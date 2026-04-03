{% snapshot snap_dim_products_scd2 %}

{{
    config(
        schema=var('silver_schema'),
        unique_key='product_id',
        strategy='check',
        check_cols=[
            'product_category_name',
            'product_category_name_english',
            'product_name_length',
            'product_description_length',
            'product_photos_qty',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm'
        ],
        invalidate_hard_deletes=True,
        location_root='s3://' ~ var('raw_bucket_name') ~ '/delta/olist',
        include_full_name_in_path=true
    )
}}

with products as (

    select * from {{ source('bronze', 'products') }}

),

translations as (

    select * from {{ source('bronze', 'product_category_name_translation') }}

)

select
    p.product_id,
    p.product_category_name,
    coalesce(t.product_category_name_english, 'unknown') as product_category_name_english,
    cast(p.product_name_length as int) as product_name_length,
    cast(p.product_description_length as int) as product_description_length,
    cast(p.product_photos_qty as int) as product_photos_qty,
    cast(p.product_weight_g as double) as product_weight_g,
    cast(p.product_length_cm as double) as product_length_cm,
    cast(p.product_height_cm as double) as product_height_cm,
    cast(p.product_width_cm as double) as product_width_cm,
    p._ingest_ts,
    p._batch_id
from products as p
left join translations as t
    on p.product_category_name = t.product_category_name

{% endsnapshot %}