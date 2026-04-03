{{ external_table_location() }}

-- Silver dimension: dim_geolocation
-- Source: dev.bronze.geolocation
-- The raw geolocation table has many rows per ZIP prefix (multiple lat/lng points).
-- This model aggregates to one canonical centroid per prefix, keeping the first
-- seen city and state (after normalisation).

with source as (

    select * from {{ source('bronze', 'geolocation') }}

),

normalised as (

    select
        cast(geolocation_zip_code_prefix as string)  as geolocation_zip_code_prefix,
        cast(geolocation_lat             as double)  as geolocation_lat,
        cast(geolocation_lng             as double)  as geolocation_lng,
        lower(trim(geolocation_city))               as geolocation_city,
        upper(trim(geolocation_state))              as geolocation_state
    from source

),

-- Deduplicate: compute centroid per ZIP prefix
aggregated as (

    select
        geolocation_zip_code_prefix,
        round(avg(geolocation_lat), 6)  as geolocation_lat,
        round(avg(geolocation_lng), 6)  as geolocation_lng,
        -- pick any single city/state label for the prefix
        first_value(geolocation_city)  over (
            partition by geolocation_zip_code_prefix
            order by geolocation_city
        )                               as geolocation_city,
        first_value(geolocation_state) over (
            partition by geolocation_zip_code_prefix
            order by geolocation_city
        )                               as geolocation_state
    from normalised
    group by
        geolocation_zip_code_prefix,
        geolocation_city,
        geolocation_state

),

-- One row per zip prefix
deduped as (

    select *,
        row_number() over (
            partition by geolocation_zip_code_prefix
            order by geolocation_city
        ) as _rn
    from aggregated

)

select
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
from deduped
where _rn = 1
