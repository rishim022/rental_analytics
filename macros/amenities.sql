-- macros/amenities.sql
-- This macro returns a SQL boolean expression indicating whether a given amenity
-- It is present in the amenities column (which is stored as a JSON array).
-- We cast the column to string and lower-case both sides to ensure case-insensitive matching.
-- This allows consistent and reusable amenity parsing across staging + mart layers.

{% macro has_amenity(amenities_col, amenity_name) %}
  lower({{ amenities_col }}) like concat('%', lower('{{ amenity_name }}'), '%')
{% endmacro %}