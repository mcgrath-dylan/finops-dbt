{% macro ntz_hour(ts) -%}
  date_trunc(
    'hour',
    coalesce(
      try_to_timestamp_ntz({{ ts }}),
      try_to_timestamp_ltz({{ ts }})::timestamp_ntz,
      try_to_timestamp_tz({{ ts }})::timestamp_ntz,
      ({{ ts }})::timestamp_ntz
    )
  )
{%- endmacro %}
