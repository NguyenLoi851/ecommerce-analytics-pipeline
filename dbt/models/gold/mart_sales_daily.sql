{{ external_table_location() }}

with orders as (

    select
        order_id,
        customer_id,
        cast(order_purchase_timestamp as date) as order_date,
        order_status,
        is_delivered
    from {{ ref('fct_orders') }}

),

order_values as (

    select
        order_id,
        sum(total_item_value) as order_gmv
    from {{ ref('fct_order_items') }}
    group by order_id

),

order_payments as (

    select
        order_id,
        sum(payment_value) as payment_value_total,
        sum(case when payment_type = 'credit_card' then payment_value else 0 end) as payment_value_credit_card,
        sum(case when payment_type = 'boleto' then payment_value else 0 end) as payment_value_boleto,
        sum(case when payment_type = 'voucher' then payment_value else 0 end) as payment_value_voucher,
        sum(case when payment_type = 'debit_card' then payment_value else 0 end) as payment_value_debit_card,
        sum(case when payment_type = 'not_defined' then payment_value else 0 end) as payment_value_not_defined
    from {{ ref('fct_payments') }}
    group by order_id

),

daily as (

    select
        o.order_date,

        count(distinct o.order_id) as order_volume,
        count(distinct case when o.is_delivered then o.order_id end) as delivered_order_volume,

        round(sum(coalesce(ov.order_gmv, 0)), 2) as gmv,
        round(sum(coalesce(ov.order_gmv, 0)) / nullif(count(distinct o.order_id), 0), 2) as aov,

        round(sum(coalesce(op.payment_value_total, 0)), 2) as payment_value_total,
        round(sum(coalesce(op.payment_value_credit_card, 0)), 2) as payment_value_credit_card,
        round(sum(coalesce(op.payment_value_boleto, 0)), 2) as payment_value_boleto,
        round(sum(coalesce(op.payment_value_voucher, 0)), 2) as payment_value_voucher,
        round(sum(coalesce(op.payment_value_debit_card, 0)), 2) as payment_value_debit_card,
        round(sum(coalesce(op.payment_value_not_defined, 0)), 2) as payment_value_not_defined,

        round(
            sum(coalesce(op.payment_value_credit_card, 0))
            / nullif(sum(coalesce(op.payment_value_total, 0)), 0),
            4
        ) as payment_mix_credit_card_pct,
        round(
            sum(coalesce(op.payment_value_boleto, 0))
            / nullif(sum(coalesce(op.payment_value_total, 0)), 0),
            4
        ) as payment_mix_boleto_pct,
        round(
            sum(coalesce(op.payment_value_voucher, 0))
            / nullif(sum(coalesce(op.payment_value_total, 0)), 0),
            4
        ) as payment_mix_voucher_pct,
        round(
            sum(coalesce(op.payment_value_debit_card, 0))
            / nullif(sum(coalesce(op.payment_value_total, 0)), 0),
            4
        ) as payment_mix_debit_card_pct,
        round(
            sum(coalesce(op.payment_value_not_defined, 0))
            / nullif(sum(coalesce(op.payment_value_total, 0)), 0),
            4
        ) as payment_mix_not_defined_pct

    from orders as o
    left join order_values as ov
        on o.order_id = ov.order_id
    left join order_payments as op
        on o.order_id = op.order_id
    group by o.order_date

)

select * from daily
