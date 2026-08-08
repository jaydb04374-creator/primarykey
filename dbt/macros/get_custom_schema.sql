{#
    Override dbt's default schema naming so that a model's `schema` config
    is used exactly as specified (e.g. 'test2_silver'), instead of being
    concatenated with the profile's default schema (e.g. 'test1_silver_test2_silver').
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
