-- models/staging/stg_listings.sql

{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('raw', 'LISTINGS') }}
),

cleaned as (
    select
        cast(ID as integer) as listing_id,
        cast(NAME as string) as listing_name,
        cast(HOST_ID as integer) as host_id,
        HOST_NAME as host_name,
        cast(HOST_SINCE as date) as host_since,
        HOST_LOCATION as host_location,
        HOST_VERIFICATIONS as host_verifications,
        NEIGHBORHOOD as neighborhood,
        PROPERTY_TYPE as property_type,
        ROOM_TYPE as room_type,
        cast(ACCOMMODATES as integer) as accommodates,
        BATHROOMS_TEXT as bathrooms_text,
        cast(BEDROOMS as integer) as bedrooms,
        cast(BEDS as integer) as beds,
        AMENITIES as amenities_raw,
        PRICE as base_price,
        cast(NUMBER_OF_REVIEWS as integer) as number_of_reviews,
        cast(FIRST_REVIEW as date) as first_review_date,
        cast(LAST_REVIEW as date) as last_review_date,
        cast(REVIEW_SCORES_RATING as numeric) as review_score_rating
    from source
)

select * from cleaned
