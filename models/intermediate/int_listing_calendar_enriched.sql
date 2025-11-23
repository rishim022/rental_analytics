-- models/intermediate/int_listing_calendar_enriched.sql

-- This model enriches the daily calendar grain (listing_id + date)
-- with listing attributes, day-level amenities, and review metrics
-- It becomes the main upstream input into fct_listing_day.


{{ config(
    materialized='view',
    tags=['intermediate']
) }}

with cal as (
    -- Base calendar: one row per listing per date, with price and availability
    select
        listing_id,
        date,
        is_available,
        reservation_id,
        nightly_price,
        minimum_nights,
        maximum_nights
    from {{ ref('stg_calendar') }}
),
listings as (
    -- Listing-level attributes (host, property, etc.)
    select
        listing_id,
        listing_name,
        host_id,
        host_name,
        host_since,
        neighborhood,
        property_type,
        room_type,
        accommodates,
        bedrooms,
        beds,
        bathrooms_text,
        number_of_reviews
    from {{ ref('stg_listings') }}
),
amenities as (
    -- Day-level amenity flags
    select
        listing_id,
        date,
        has_air_conditioning,
        has_lockbox,
        has_first_aid_kit,
        has_essentials,
        has_wifi,
        has_hotwater,
        has_kitchen,
        has_cooking_basics,
        has_bed_linens,
        has_long_term_stay_allowed
    from {{ ref('int_amenities_daily') }}
),
reviews as (
    -- Daily review metric per listing + date
     select
        listing_id,
        date,
        avg_review_score
    from {{ ref('int_reviews_daily') }}
)

select
    -- Calendar_listings_reservations / availability
    cal.listing_id,
    cal.date,
    cal.is_available,
    cal.reservation_id,
    cal.nightly_price,
    cal.minimum_nights,
    cal.maximum_nights,

    -- Listing attributes
    l.listing_name,
    l.host_id,
    l.host_name,
    l.host_since,
    l.neighborhood,
    l.property_type,
    l.room_type,
    l.accommodates,
    l.bedrooms,
    l.beds,
    l.bathrooms_text,

    -- Amenities
    a.has_air_conditioning,
    a.has_lockbox,
    a.has_first_aid_kit,
    a.has_essentials,
    a.has_wifi,
    a.has_hotwater,
    a.has_kitchen,
    a.has_cooking_basics,
    a.has_bed_linens,
    a.has_long_term_stay_allowed,

    -- Reviews
    l.number_of_reviews, -- lifetime count from listings
    r.avg_review_score -- cumulative average review score metric from int_reviews_daily

from cal
join listings l
  on cal.listing_id = l.listing_id
left join amenities a
  on cal.listing_id = a.listing_id
  and cal.date = a.date
left join reviews r
  on cal.listing_id = r.listing_id
  and cal.date = r.date
