{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='scd_id',
        on_schema_change='sync_all_columns',
        post_hook="{{ scd2_close_current_products_merge(this) }}",
        location_root='s3://' ~ var('raw_bucket_name') ~ '/delta/olist',
        include_full_name_in_path=true
    )
}}

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
        cast(p.product_name_length as int) as product_name_length,
        cast(p.product_description_length as int) as product_description_length,
        cast(p.product_photos_qty as int) as product_photos_qty,
        cast(p.product_weight_g as double) as product_weight_g,
        cast(p.product_length_cm as double) as product_length_cm,
        cast(p.product_height_cm as double) as product_height_cm,
        cast(p.product_width_cm as double) as product_width_cm,
        p._ingest_ts,
        p._batch_id,
        row_number() over (partition by p.product_id order by p._ingest_ts desc) as _rn
    from products as p
    left join translations as t
        on p.product_category_name = t.product_category_name

),

source_current as (

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
        _ingest_ts,
        _batch_id
    from latest_source
    where _rn = 1

),

target_current as (

    {% if is_incremental() %}
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
    from {{ this }}
    where is_current = true
    {% else %}
    select
        cast(null as string) as product_id,
        cast(null as string) as product_category_name,
        cast(null as string) as product_category_name_english,
        cast(null as int) as product_name_length,
        cast(null as int) as product_description_length,
        cast(null as int) as product_photos_qty,
        cast(null as double) as product_weight_g,
        cast(null as double) as product_length_cm,
        cast(null as double) as product_height_cm,
        cast(null as double) as product_width_cm
    where 1 = 0
    {% endif %}

),

records_to_insert as (

    select
        s.product_id,
        s.product_category_name,
        s.product_category_name_english,
        s.product_name_length,
        s.product_description_length,
        s.product_photos_qty,
        s.product_weight_g,
        s.product_length_cm,
        s.product_height_cm,
        s.product_width_cm,
        current_timestamp() as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current,
        s._ingest_ts,
        s._batch_id,
        sha2(concat_ws('||', s.product_id, cast(current_timestamp() as string)), 256) as scd_id
    from source_current as s
    left join target_current as t
        on s.product_id = t.product_id
    where t.product_id is null
       or coalesce(s.product_category_name, '') <> coalesce(t.product_category_name, '')
       or coalesce(s.product_category_name_english, '') <> coalesce(t.product_category_name_english, '')
       or coalesce(s.product_name_length, -1) <> coalesce(t.product_name_length, -1)
       or coalesce(s.product_description_length, -1) <> coalesce(t.product_description_length, -1)
       or coalesce(s.product_photos_qty, -1) <> coalesce(t.product_photos_qty, -1)
       or coalesce(s.product_weight_g, -1.0) <> coalesce(t.product_weight_g, -1.0)
       or coalesce(s.product_length_cm, -1.0) <> coalesce(t.product_length_cm, -1.0)
       or coalesce(s.product_height_cm, -1.0) <> coalesce(t.product_height_cm, -1.0)
       or coalesce(s.product_width_cm, -1.0) <> coalesce(t.product_width_cm, -1.0)

)

select * from records_to_insert
