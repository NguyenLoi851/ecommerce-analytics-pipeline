{{ external_table_location() }}

-- Silver fact: fct_orders
-- Source: dev.bronze.orders
-- Adds derived columns for delivery performance analysis.

with source as (

    select * from {{ source('bronze', 'orders') }}

),

final as (

    select
        -- keys
        order_id,
        customer_id,

        -- status
        order_status,

        -- timestamps (cast to timestamp for type safety)
        cast(order_purchase_timestamp       as timestamp)  as order_purchase_timestamp,
        cast(order_approved_at              as timestamp)  as order_approved_at,
        cast(order_delivered_carrier_date   as timestamp)  as order_delivered_carrier_date,
        cast(order_delivered_customer_date  as timestamp)  as order_delivered_customer_date,
        cast(order_estimated_delivery_date  as timestamp)  as order_estimated_delivery_date,

        -- derived delivery metrics
        case
            when order_status = 'delivered'
             and order_delivered_customer_date is not null
            then true
            else false
        end                                                                    as is_delivered,

        case
            when order_delivered_customer_date is not null
             and order_purchase_timestamp is not null
            then datediff(
                cast(order_delivered_customer_date as date),
                cast(order_purchase_timestamp      as date)
            )
        end                                                                    as actual_delivery_days,

        case
            when order_estimated_delivery_date is not null
             and order_purchase_timestamp is not null
            then datediff(
                cast(order_estimated_delivery_date as date),
                cast(order_purchase_timestamp      as date)
            )
        end                                                                    as estimated_delivery_days,

        -- positive = late, negative = early
        case
            when order_delivered_customer_date is not null
             and order_estimated_delivery_date is not null
            then datediff(
                cast(order_delivered_customer_date as date),
                cast(order_estimated_delivery_date as date)
            )
        end                                                                    as delivery_delay_days,

        -- metadata
        _ingest_ts,
        _batch_id

    from source

)

select * from final
