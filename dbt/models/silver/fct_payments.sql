{{ external_table_location() }}

-- Silver fact: fct_payments
-- Source: dev.bronze.order_payments
-- One row per (order_id, payment_sequential). Casts numeric columns and
-- preserves all payment types including split/instalment scenarios.

with source as (

    select * from {{ source('bronze', 'order_payments') }}

),

final as (

    select
        -- keys
        order_id,
        cast(payment_sequential    as int)     as payment_sequential,

        -- attributes
        payment_type,
        cast(payment_installments  as int)     as payment_installments,
        cast(payment_value         as double)  as payment_value,

        -- metadata
        _ingest_ts,
        _batch_id

    from source

)

select * from final
