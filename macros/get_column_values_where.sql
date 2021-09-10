{#
This macro fetches the unique values for `column` in the table `table` with `where conditions` applied.
It derived from get_column_values.

Arguments:
    table: A model `ref`, or a schema.table string for the table to query (Required)
    column: The column to query for unique values
    max_records: If provided, the maximum number of unique records to return (default: none)
    where_condition: The data will be filtered by this where condition. 

Returns:
    A list of distinct values for the specified columns with where condition applied.

Note:
    This macro needs be copied into dbt_utils/macros/sql, it is not working if placed in manticore_utils or macros folder in the project

Updated:
10 Sep 2021, S.C

#}


{% macro get_column_values_where(table, column, where_condition, max_records=none, default=none) -%}
    {{ return(adapter.dispatch('get_column_values_where', packages = dbt_utils._get_utils_namespaces())(table, column, where_condition, max_records, default)) }}
{% endmacro %}

{% macro default__get_column_values_where(table, column, where_condition, max_records=none, default=none) -%}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}
{#--  #}

    {%- set target_relation = adapter.get_relation(database=table.database,
                                          schema=table.schema,
                                         identifier=table.identifier) -%}

    {%- call statement('get_column_values_where', fetch_result=true) %}

        {%- if not target_relation and default is none -%}

          {{ exceptions.raise_compiler_error("In get_column_values_where(): relation " ~ table ~ " does not exist and no default value was provided.") }}

        {%- elif not target_relation and default is not none -%}

          {{ log("Relation " ~ table ~ " does not exist. Returning the default value: " ~ default) }}

          {{ return(default) }}

        {%- else -%}

            select
                {{ column }} as value

            from {{ target_relation }}
             {{ where_condition }}
            group by 1
            order by count(*) desc

            {% if max_records is not none %}
            limit {{ max_records }}
            {% endif %}

        {% endif %}

    {%- endcall -%}

    {%- set value_list = load_result('get_column_values_where') -%}

    {%- if value_list and value_list['data'] -%}
        {%- set values = value_list['data'] | map(attribute=0) | list %}
        {{ return(values) }}
    {%- else -%}
        {{ return(default) }}
    {%- endif -%}

{%- endmacro %}
