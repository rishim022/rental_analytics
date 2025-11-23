-- models/intermediate/int_reviews_daily.sql
-- This model produces a day-level review metric per listing by
-- combining the calendar dates with the generated reviews.
-- For each (listing_id, date), it calculates the average review
-- score of all reviews that have occurred on or before that date.
-- The result is a "to-date" / cumulative average score per day.

{{ config(
    materialized='view',
    tags=['intermediate']
) }}

with reviews as (
    -- Raw review events: one row per (listing_id, review_date, review_score)
    select 
    listing_id,
    review_date, 
    review_score
    from {{ ref('stg_generated_reviews') }}
),

dates as (
    -- All listing + date combinations
    select 
    distinct listing_id,
    date
    from {{ ref('stg_calendar') }}
),

joined as (
    -- For each listing + calendar date join to all reviews for that listing
    select
        d.listing_id,
        d.date,
        avg(r.review_score) as avg_review_score
    from dates d
    left join reviews r
    on d.listing_id = r.listing_id
    and r.review_date <= d.date

    group by 
    d.listing_id,
    d.date
)
-- Final output: one row per listing + date with a average review score
select
    listing_id,
    date,
    avg_review_score
from joined
