-- models/intermediate/int_amenities_daily.sql
-- This model builds a day-level snapshot of amenities per listing by
-- joining the calendar to the amenities changelog and taking the
-- latest change on or before each date. It then exposes boolean
-- amenity flags (has_air_conditioning, has_lockbox, etc.)

{{ config(
    materialized='view',
    tags=['intermediate']
) }}

with calendar as (
    select 
    listing_id,
    date
    from {{ ref('stg_calendar') }}
),

changes as (
    -- Normalized amenity changes: one row per listing per change_at
    -- change_at is cast to date so we can do a join by day
    select
        listing_id,
        cast(change_at as date) as change_date,
        amenities_json
    from {{ ref('stg_amenities_changelog') }}
),

calendar_with_change as (
    -- For each listing + date, find the most recent amenity change
    select
        c.listing_id,
        c.date,
        max(ch.change_date) as last_change_date
    from calendar c
    left join changes ch
      on c.listing_id = ch.listing_id
      and ch.change_date <= c.date
    group by
    c.listing_id,
    c.date
),

amenities_as_of_day as (
    -- Attach the amenity JSON for that last_change_date to each day
    select
        c.listing_id,
        c.date,
        ch.amenities_json
    from calendar_with_change c
    left join changes ch
      on c.listing_id = ch.listing_id
      and c.last_change_date = ch.change_date
)

-- Final output: one row per listing + date with boolean amenity flags
select
    listing_id,
    date,
    amenities_json,

    -- Core amenity flags used in analysis
    {{ has_amenity('amenities_json', 'air conditioning') }} as has_air_conditioning,
    {{ has_amenity('amenities_json', 'lockbox') }} as has_lockbox,
    {{ has_amenity('amenities_json', 'first aid kit') }} as has_first_aid_kit,

    -- Additional commonly used amenities
    {{ has_amenity('amenities_json', 'wifi') }} as has_wifi,
    {{ has_amenity('amenities_json', 'essentials') }} as has_essentials,
    {{ has_amenity('amenities_json', 'long term stays allowed') }} as has_long_term_stay_allowed,
    {{ has_amenity('amenities_json', 'hot water') }} as has_hotwater,
    {{ has_amenity('amenities_json', 'bed linens') }} as has_bed_linens,
    {{ has_amenity('amenities_json', 'cooking basics') }} as has_cooking_basics,
    {{ has_amenity('amenities_json', 'kitchen') }} as has_kitchen

from amenities_as_of_day
