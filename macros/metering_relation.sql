{% macro metering_relation() -%}
  {% if var('DEMO_MODE', false) %}
    {{ ref('metering_overlay') }}   {# DEMO.WAREHOUSE_METERING_HISTORY, created below #}
  {% else %}
    {{ source('account_usage','WAREHOUSE_METERING_HISTORY') }}
  {% endif %}
{%- endmacro %}
