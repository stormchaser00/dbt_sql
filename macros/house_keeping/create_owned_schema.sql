{#
  Macro create_owned_schema creates or replaces a schema,
  and assigns ownership to a named role
#}

{% macro create_owned_schema(schema_name, schema_owner='') %}
  {% set query %}
    create or replace schema {{ schema_name }}
  {% endset %}

  {% if execute %}
    {{ log("query: " ~ query)}}
    {% do run_query(query) %}
  {% endif %}
{% endmacro %}
