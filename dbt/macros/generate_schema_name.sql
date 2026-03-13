-- Override dbt's default schema name generation.
--
-- Default behaviour: {default_schema}_{custom_schema}  e.g. bronze_silver
-- This override:     {custom_schema}                   e.g. silver
--
-- When no custom schema is set on a model the connection's default schema
-- (profiles.yml → schema) is used unchanged.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
