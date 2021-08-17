


{% macro get_unique_value_for_column(table, column_name) %}

{% set unique_value_query %}
    select distinct
    {{ column_name }}
    from {{ table }}
    order by 1
{% endset %}

{% set results = run_query(unique_value_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}




{#
usage:

{% for selected_value in get_unique_value_for_column(table, column_name) %}
sum(case when column_name = '{{selected_value}}' then amount end) as {{selected_value}}_amount
{% if not loop.last %},{% endif %}
{% endfor %}

-- dbt_utils 
{%- set payment_methods = dbt_utils.get_column_values(
    table=ref('raw_payments'),
    column='payment_method'
) -%}

#}
