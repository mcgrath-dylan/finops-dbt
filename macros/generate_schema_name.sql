{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Route to DEMO schema when DEMO_MODE is true #}
    {%- if var('DEMO_MODE', false) -%}
        {{ return('DEMO') }}
    {%- endif -%}

    {# Respect any custom schema; else fall back to target.schema #}
    {%- if custom_schema_name is none or custom_schema_name | length == 0 -%}
        {{ return(target.schema) }}
    {%- else -%}
        {{ return(custom_schema_name) }}
    {%- endif -%}
{%- endmacro %}
