-- models/staging/stg_amenities_changelog.sql

{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('raw', 'AMENITIES_CHANGELOG') }}
),

cleaned as (
    select
        cast(LISTING_ID as integer) as listing_id,
        cast(CHANGE_AT as timestamp) as change_at,
        AMENITIES as amenities_json
    from source
)

select * from cleaned