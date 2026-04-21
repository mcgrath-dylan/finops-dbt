{% macro storage_relation() -%}
  {% if var('DEMO_MODE', false) %}
    {{ ref('storage_overlay') }}   {# DEMO.DATABASE_STORAGE_USAGE_HISTORY, from storage_demo_seed #}
  {% else %}
    {{ source('account_usage','DATABASE_STORAGE_USAGE_HISTORY') }}
  {% endif %}
{%- endmacro %}
