{# 
This macro deletes everything in your target schema that isn’t in your dbt project

Reference: 
https://discourse.getdbt.com/t/clean-your-warehouse-of-old-and-deprecated-models-snowflake/1547

1. Provides the option of a dry run where the commands are printed, but not executed
2. Will match any schema prefixed with the target schema, 
rather than only the exact target schema (since we use multiple schemas 
of the form <target_schema>_<schema_name>). The exact target schema will still be matched. 
This introduces some risk if target schemas could overlap with prefixes of other schema names, 
so use with caution. It only matches strictly prefixes though, not a full wildcard, 
so if you’re consistent with prefixing schemas this shouldn’t be an issue.

#}


{% macro drop_old_relations(dry_run='false') %}
{% if execute %}
  {% set current_models=[] %}
  {% for node in graph.nodes.values()
     | selectattr("resource_type", "in", ["model", "seed", "snapshot"])%}
    {% do current_models.append(node.name) %}
  {% endfor %}
{% endif %}
{% set cleanup_query %}
      with models_to_drop as (
        select
          case 
            when table_type = 'BASE TABLE' then 'TABLE'
            when table_type = 'VIEW' then 'VIEW'
          end as relation_type,
          concat_ws('.', table_catalog, table_schema, table_name) as relation_name
        from 
          {{ target.database }}.information_schema.tables
        where table_schema ilike '{{ target.schema }}%'
          and table_name not in
            ({%- for model in current_models -%}
                '{{ model.upper() }}'
                {%- if not loop.last -%}
                    ,
                {% endif %}
            {%- endfor -%}))
      select 
        'drop ' || relation_type || ' ' || relation_name || ';' as drop_commands
      from 
        models_to_drop
      
      -- intentionally exclude unhandled table_types, including 'external table`
      where drop_commands is not null
  {% endset %}
{% do log(cleanup_query, info=True) %}
{% set drop_commands = run_query(cleanup_query).columns[0].values() %}
{% if drop_commands %}
  {% for drop_command in drop_commands %}
    {% do log(drop_command, True) %}
    {% if dry_run == 'false' %}
      {% do run_query(drop_command) %}
    {% endif %}
  {% endfor %}
{% else %}
  {% do log('No relations to clean.', True) %}
{% endif %}
{%- endmacro -%}
