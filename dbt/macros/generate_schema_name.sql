{#
  Use the model's +schema value verbatim as the ClickHouse database, instead of dbt's
  default "<target_schema>_<custom>" concatenation. Lets bronze/silver/gold map to clean
  database names.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
