-- models/staging/stg_calendar.sql

{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('raw', 'CALENDAR') }}
),

cleaned as (
    select
        cast(LISTING_ID as integer) as listing_id,
        cast(DATE as date) as date,
        AVAILABLE as is_available,
        RESERVATION_ID as reservation_id,
        PRICE as nightly_price,
        cast(MINIMUM_NIGHTS as integer) as minimum_nights,
        cast(MAXIMUM_NIGHTS as integer) as maximum_nights
    from source
)

select * from cleaned