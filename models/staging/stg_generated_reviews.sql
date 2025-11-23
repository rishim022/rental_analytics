-- models/staging/stg_generated_reviews.sql

{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('raw', 'GENERATED_REVIEWS') }}
),

cleaned as (
    select
        cast(ID as integer) as review_id,
        cast(LISTING_ID as integer) as listing_id,
        cast(REVIEW_SCORE as integer) as review_score,
        cast(REVIEW_DATE as date) as review_date
    from source
    where ID is not null
)

select * from cleaned