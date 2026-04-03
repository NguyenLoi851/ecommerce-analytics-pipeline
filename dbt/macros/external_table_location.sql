{% macro external_table_location() -%}
    {{ config(
        location_root='s3://' ~ var('raw_bucket_name') ~ '/delta/olist',
        include_full_name_in_path=true
    ) }}
{%- endmacro %}