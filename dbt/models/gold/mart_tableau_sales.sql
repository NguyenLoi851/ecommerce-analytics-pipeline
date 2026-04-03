{{ external_table_location() }}

-- Gold mart: mart_tableau_sales
-- Purpose: Wide, pre-joined table for the Tableau Sales Dashboard.
-- Grain: one row per order item.
-- Joins: fct_order_items + fct_orders + dim_customers + dim_products + primary payment per order.

with order_items as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        total_item_value
    from {{ ref('fct_order_items') }}

),

orders as (

    select
        order_id,
        customer_id,
        order_status,
        cast(order_purchase_timestamp as date)                   as order_date,
        date_format(order_purchase_timestamp, 'yyyy-MM')         as order_month,
        date_format(order_purchase_timestamp, 'EEEE')            as order_day_of_week,
        -- numeric day (1=Sunday, 2=Monday … 7=Saturday in Spark SQL)
        -- used for custom sort in Tableau
        case date_format(order_purchase_timestamp, 'EEEE')
            when 'Monday'    then 1
            when 'Tuesday'   then 2
            when 'Wednesday' then 3
            when 'Thursday'  then 4
            when 'Friday'    then 5
            when 'Saturday'  then 6
            when 'Sunday'    then 7
        end                                                       as day_of_week_sort
    from {{ ref('fct_orders') }}

),

customers as (

    select
        customer_id,
        customer_state,
        customer_city
    from {{ ref('dim_customers') }}

),

products as (

    select
        product_id,
        product_category_name_english
    from {{ ref('dim_products') }}

),

-- Pick the primary payment per order: the one with the highest payment_value.
-- This gives one payment_type per order so the pie chart has clean, non-duplicated rows.
primary_payment as (

    select
        order_id,
        payment_type       as primary_payment_type,
        payment_value      as primary_payment_value
    from (
        select
            order_id,
            payment_type,
            payment_value,
            row_number() over (
                partition by order_id
                order by payment_value desc, payment_sequential asc
            ) as rn
        from {{ ref('fct_payments') }}
    ) ranked
    where rn = 1

),

final as (

    select
        -- keys
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        o.customer_id,

        -- order date fields (for Charts 1 and 4)
        o.order_date,
        o.order_month,
        o.order_day_of_week,
        o.day_of_week_sort,

        -- order attributes
        o.order_status,

        -- customer location (for Chart 5)
        c.customer_state,
        c.customer_city,

        -- product category (for Chart 2)
        coalesce(p.product_category_name_english, 'unknown') as product_category_name_english,

        -- revenue (for Charts 1, 2, 5 and KPIs)
        round(oi.total_item_value, 2) as total_item_value,

        -- payment (for Chart 3 and KPIs)
        pp.primary_payment_type,
        round(pp.primary_payment_value, 2) as primary_payment_value

    from order_items oi
    inner join orders           o   on oi.order_id   = o.order_id
    left  join customers        c   on o.customer_id  = c.customer_id
    left  join products         p   on oi.product_id  = p.product_id
    left  join primary_payment  pp  on oi.order_id   = pp.order_id

)

select * from final
