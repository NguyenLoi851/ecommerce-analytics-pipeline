{{ external_table_location() }}

-- Gold mart: mart_tableau_ops
-- Purpose: Wide, pre-joined table for the Tableau Operations & Customer Experience Dashboard.
-- Grain: one row per order.
-- Joins: fct_orders + dim_customers + latest review per order from fct_reviews.

with orders as (

    select
        order_id,
        customer_id,
        order_status,
        cast(order_purchase_timestamp as date)           as order_date,
        year(order_purchase_timestamp)                   as order_year,
        is_delivered,
        actual_delivery_days,
        estimated_delivery_days,
        delivery_delay_days,
        case when delivery_delay_days > 0 then true else false end as is_late_delivery
    from {{ ref('fct_orders') }}

),

customers as (

    select
        customer_id,
        customer_state,
        customer_city
    from {{ ref('dim_customers') }}

),

-- Keep only one review per order (the most recently created).
-- fct_reviews deduplicates on review_id, but multiple review_ids can share the same order_id.
latest_review as (

    select
        order_id,
        review_id,
        review_score,
        review_creation_date
    from (
        select
            order_id,
            review_id,
            review_score,
            review_creation_date,
            row_number() over (
                partition by order_id
                order by review_creation_date desc
            ) as rn
        from {{ ref('fct_reviews') }}
    ) ranked
    where rn = 1

),

final as (

    select
        -- keys
        o.order_id,
        o.customer_id,

        -- order date fields (for Charts 3 and 4)
        o.order_date,
        o.order_year,

        -- order status (for Chart 5)
        o.order_status,

        -- customer location (for Charts 1 and 2)
        c.customer_state,
        c.customer_city,

        -- delivery metrics (for KPIs, Charts 1, 2, 4)
        o.is_delivered,
        o.actual_delivery_days,
        o.estimated_delivery_days,
        o.delivery_delay_days,
        o.is_late_delivery,

        -- review fields (for KPIs, Charts 3 and 4)
        r.review_id,
        r.review_score,
        r.review_creation_date

    from orders o
    left join customers     c on o.customer_id = c.customer_id
    left join latest_review r on o.order_id    = r.order_id

)

select * from final
