{% macro external_table_location() -%}
    {{ config(
        external_location='olist_delta_ext_loc'
    ) }}
{%- endmacro %}