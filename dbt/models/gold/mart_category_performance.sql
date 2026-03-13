with order_items as (

    select
        oi.order_id,
        oi.product_id,
        oi.price,
        oi.freight_value,
        oi.total_item_value
    from {{ ref('fct_order_items') }} as oi

),

orders as (

    select
        order_id,
        cast(order_purchase_timestamp as date) as order_date,
        order_status,
        is_delivered
    from {{ ref('fct_orders') }}

),

products as (

    select
        product_id,
        product_category_name_english
    from {{ ref('dim_products') }}

),

final as (

    select
        o.order_date,
        coalesce(p.product_category_name_english, 'unknown') as product_category_name_english,

        count(distinct o.order_id) as order_volume,
        count(*) as item_volume,

        round(sum(oi.price), 2) as item_revenue,
        round(sum(oi.freight_value), 2) as freight_revenue,
        round(sum(oi.total_item_value), 2) as gmv,

        round(sum(oi.total_item_value) / nullif(count(distinct o.order_id), 0), 2) as aov,
        round(avg(oi.total_item_value), 2) as avg_item_value

    from order_items as oi
    inner join orders as o
        on oi.order_id = o.order_id
    left join products as p
        on oi.product_id = p.product_id
    group by
        o.order_date,
        coalesce(p.product_category_name_english, 'unknown')

)

select * from final
