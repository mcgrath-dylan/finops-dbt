{% macro query_history_relation() -%}
  {% if var('DEMO_MODE', false) %}
    {{ ref('query_history_overlay') }}
  {% else %}
    {{ source('account_usage','QUERY_HISTORY') }}
  {% endif %}
{%- endmacro %}
