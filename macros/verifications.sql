-- macros/verfications.sql
-- This macro returns a SQL boolean expression indicating whether a given host verification
-- It is present in the verification column (which is stored as a JSON array).
-- We cast the column to string and lower-case both sides to ensure case-insensitive matching.
-- This allows consistent and reusable verification parsing across staging + mart layers.

{% macro verification(host_verifications_col, host_verifications_name) %}
  lower({{ host_verifications_col }}) like concat('%', lower('{{ host_verifications_name }}'), '%')
{% endmacro %}