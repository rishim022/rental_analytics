--Purpose:
--Convert a textual bathroom description (e.g. '1 bath', '2 baths','1.5 baths', '2.5 baths', '1 shared bath') into a numeric bathroom count as FLOAT64.
--Notes:
-- Used REGEXP_EXTRACT to pull the first integer or decimal number.
-- Used SAFE_CAST so invalid values return NULL instead of erroring.
 
{% macro extract_bathroom_count(col) %}
    SAFE_CAST(
      REGEXP_EXTRACT(
        CAST({{ col }} AS STRING),
        r'[0-9]+(?:\.[0-9]+)?'
      )
      AS FLOAT64
    )
{% endmacro %}

--Macro: extract_bathroom_type
--Purpose:Classify the bathroom description into 'shared' or 'private'.
-- Rules considered:
-- If the text contains 'shared' (case-insensitive) -> 'shared'
-- If it contains 'private' -> 'private'
-- If neither is present (e.g. '1 bath', '2 baths') -> defaults to 'private'

{% macro extract_bathroom_type(col) %}
    case
        when lower({{ col }}) like '%shared%' then 'shared'
        when lower({{ col }}) like '%private%' then 'private'
        else 'private' -- assumption if only bath mentioned then considered as private bath
    end
{% endmacro %}