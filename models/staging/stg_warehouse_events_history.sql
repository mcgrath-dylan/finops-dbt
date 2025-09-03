{{ config(
    enabled=var('enable_pro_pack', false),
    materialized='incremental'
) }}

{#-- Introspect columns because ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY can vary by account --#}
{% set rel = source('account_usage', 'WAREHOUSE_EVENTS_HISTORY') %}
{% set cols = adapter.get_columns_in_relation(rel) %}
{% set names = [] %}
{% for c in cols %}{% do names.append(c.name | lower) %}{% endfor %}
{% set has_timestamp = 'timestamp' in names %}
{% set has_start    = 'start_time' in names %}
{% set has_end      = 'end_time' in names %}
{% set has_evt_type = 'event_type' in names %}
{% set has_evt_name = 'event_name' in names %}
{% set has_evt      = 'event' in names %}

with src as (
  select
    warehouse_name,

    /* normalize to event_type */
    {% if has_evt_type %} event_type
    {% elif has_evt_name %} event_name as event_type
    {% elif has_evt %}     event      as event_type
    {% else %}             null       as event_type
    {% endif %},

    /* normalize to event_ts */
    {% if has_timestamp %} "TIMESTAMP"
    {% elif has_end %}     end_time
    {% elif has_start %}   start_time
    {% else %}             to_timestamp_ntz(null)
    {% endif %} as event_ts

  from {{ rel }}
  {% if is_incremental() %}
    where
      {% if has_timestamp %} "TIMESTAMP"
      {% elif has_end %}     end_time
      {% elif has_start %}   start_time
      {% else %}             to_timestamp_ntz('1970-01-01')
      {% endif %}
      >= dateadd('day', - ( {{ var('WINDOW_DAYS', 30) }} )::int, current_timestamp())
  {% endif %}
)

select warehouse_name, event_type, event_ts
from src
