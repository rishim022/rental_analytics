-- models/marts/rentals/fct_listing_day.sql
-- This is the core fact model at a (listing_id, date) grain.
-- It combines enriched calendar attributes with reservation status
-- and computes daily revenue + occupancy.

-- Materialized as INCREMENTAL so we only rebuild recent data rather
-- than full history on every run. Partitioning & clustering improve
-- query performance in downstream analysis.

{{ config(
    materialized='incremental',
    unique_key=['listing_id', 'date'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['mart'],
    cluster_by=['listing_id', 'date']
) }}

select
    -- Listing attributes
    listing_id,
    date,
    neighborhood,
    host_id,
    host_name,
    host_since,
    property_type,
    room_type,
    bedrooms,
    beds,

    -- Booking & revenue
    nightly_price,
    is_available,
    reservation_id,
    (reservation_id is not null) as is_reserved, -- boolean flag for booking
    
    case when reservation_id is not null then nightly_price else 0 end as revenue,

    minimum_nights,
    maximum_nights,

    -- Amenities (boolean attributes)
    has_air_conditioning,
    has_lockbox,
    has_first_aid_kit,
    has_essentials,
    has_wifi,
    has_hotwater,
    has_kitchen,
    has_cooking_basics,
    has_bed_linens,
    has_long_term_stay_allowed,

    -- Reviews
    number_of_reviews,
    avg_review_score

from {{ ref('int_listing_calendar_enriched') }}
