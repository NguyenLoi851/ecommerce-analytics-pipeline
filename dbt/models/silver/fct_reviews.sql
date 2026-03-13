-- Silver fact: fct_reviews
-- Source: dev.bronze.order_reviews
-- One row per review_id. Deduplicates on review_id (keep the latest entry)
-- and casts timestamps.

with source as (

    select * from {{ source('bronze', 'order_reviews') }}

),

deduped as (

    select *,
        row_number() over (
            partition by review_id
            order by _ingest_ts desc
        ) as _rn
    from source

),

final as (

    select
        -- keys
        review_id,
        order_id,

        -- review attributes
        cast(review_score as int)                         as review_score,
        cast(review_comment_title   as string)            as review_comment_title,
        cast(review_comment_message as string)            as review_comment_message,

        -- timestamps
        cast(review_creation_date    as timestamp)        as review_creation_date,
        cast(review_answer_timestamp as timestamp)        as review_answer_timestamp,

        -- derived: days between order review creation and seller response
        case
            when review_answer_timestamp is not null
             and review_creation_date    is not null
            then datediff(
                cast(review_answer_timestamp as date),
                cast(review_creation_date    as date)
            )
        end                                               as review_response_days,

        -- metadata
        _ingest_ts,
        _batch_id

    from deduped
    where _rn = 1

)

select * from final
