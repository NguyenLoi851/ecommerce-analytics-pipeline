with delivered_orders as (

    select
        cast(order_purchase_timestamp as date) as order_date,
        order_status,
        is_delivered,
        actual_delivery_days,
        estimated_delivery_days,
        delivery_delay_days
    from {{ ref('fct_orders') }}
    where is_delivered = true

),

final as (

    select
        order_date,
        count(*) as delivered_order_volume,

        count(case when delivery_delay_days <= 0 then 1 end) as on_time_order_volume,
        count(case when delivery_delay_days > 0 then 1 end) as late_order_volume,

        round(
            count(case when delivery_delay_days <= 0 then 1 end) / nullif(count(*), 0),
            4
        ) as on_time_delivery_rate,

        round(avg(actual_delivery_days), 2) as avg_actual_delivery_days,
        round(avg(estimated_delivery_days), 2) as avg_estimated_delivery_days,
        round(avg(delivery_delay_days), 2) as avg_delivery_delay_days,
        max(delivery_delay_days) as worst_delivery_delay_days

    from delivered_orders
    group by order_date

)

select * from final
