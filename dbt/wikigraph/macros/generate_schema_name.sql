{#-
  Override dbt's default schema naming.
  Default behaviour: <profile_schema>_<model_schema>  (e.g. "stg_mart")
  What we want:      <model_schema>                   (e.g. "mart")
  This is the single most common source of "why is my table in the wrong schema".
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}