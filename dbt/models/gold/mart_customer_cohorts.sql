with customer_orders as (

    select
        c.customer_unique_id,
        o.order_id,
        cast(o.order_purchase_timestamp as date) as order_date,
        date_trunc('month', cast(o.order_purchase_timestamp as date)) as order_month
    from {{ ref('fct_orders') }} as o
    inner join {{ ref('dim_customers') }} as c
        on o.customer_id = c.customer_id

),

customer_first_order as (

    select
        customer_unique_id,
        min(order_month) as cohort_month
    from customer_orders
    group by customer_unique_id

),

cohort_activity as (

    select
        cfo.cohort_month,
        co.order_month,
        floor(months_between(co.order_month, cfo.cohort_month)) as cohort_month_number,
        count(distinct co.customer_unique_id) as active_customers,
        count(distinct co.order_id) as order_volume
    from customer_orders as co
    inner join customer_first_order as cfo
        on co.customer_unique_id = cfo.customer_unique_id
    group by
        cfo.cohort_month,
        co.order_month,
        floor(months_between(co.order_month, cfo.cohort_month))

),

cohort_sizes as (

    select
        cohort_month,
        count(distinct customer_unique_id) as cohort_size
    from customer_first_order
    group by cohort_month

),

final as (

    select
        ca.cohort_month,
        ca.order_month,
        ca.cohort_month_number,
        cs.cohort_size,
        ca.active_customers,
        ca.order_volume,
        round(ca.active_customers / nullif(cs.cohort_size, 0), 4) as retention_rate
    from cohort_activity as ca
    inner join cohort_sizes as cs
        on ca.cohort_month = cs.cohort_month

)

select * from final
