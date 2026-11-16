{#-
  Select a column that may not exist yet. payments.channel is added by a
  source schema change part-way through the demo workload; before that the
  silver table does not have it, and the model should still build.
-#}
{% macro optional_column(relation, column, type='varchar') -%}
    {%- if execute -%}
        {%- set names = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
        {%- if column | lower in names -%}
            {{ column }}
        {%- else -%}
            cast(null as {{ type }}) as {{ column }}
        {%- endif -%}
    {%- else -%}
        cast(null as {{ type }}) as {{ column }}
    {%- endif -%}
{%- endmacro %}
