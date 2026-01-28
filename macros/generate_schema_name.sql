{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {#- Logic: If a custom schema is provided in dbt_project.yml, use ONLY that -#}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}

    {#- If no custom schema is provided, use the dataset from profiles.yml -#}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}