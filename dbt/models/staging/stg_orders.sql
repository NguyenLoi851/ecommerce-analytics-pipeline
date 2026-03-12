-- Staging model: stg_orders
-- Reads from Bronze, casts types, renames columns to snake_case standard.
-- Materialized as a view over dev.bronze.orders.

with source as (
    select * from {{ source('bronze', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        -- metadata pass-through
        _ingest_ts,
        _source_file,
        _batch_id
    from source
)

select * from renamed
