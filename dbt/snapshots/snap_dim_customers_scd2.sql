{% snapshot snap_dim_customers_scd2 %}

{{
    config(
        schema=var('silver_schema'),
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state'],
        invalidate_hard_deletes=True,
        location_root='s3://' ~ var('raw_bucket_name') ~ '/delta/olist',
        include_full_name_in_path=true
    )
}}

select
    customer_id,
    customer_unique_id,
    cast(customer_zip_code_prefix as string) as customer_zip_code_prefix,
    lower(trim(customer_city)) as customer_city,
    upper(trim(customer_state)) as customer_state,
    _ingest_ts,
    _batch_id
from {{ source('bronze', 'customers') }}

{% endsnapshot %}