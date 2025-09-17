{% macro ntz_hour(ts) -%}
  date_trunc('hour', ({{ ts }})::timestamp_ntz)
{%- endmacro %}
