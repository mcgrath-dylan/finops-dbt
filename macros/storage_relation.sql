{% macro storage_relation() -%}
  {% if var('DEMO_MODE', false) %}
    {{ ref('storage_overlay') }}   {# DEMO.STORAGE_USAGE, from storage_demo_seed #}
  {% else %}
    {{ source('account_usage','STORAGE_USAGE') }}
  {% endif %}
{%- endmacro %}
