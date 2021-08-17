
{#  
https://github.com/vwsergewv/dynamic_pivot_dbt/blob/main/README.md

#}

{% macro dynamic_pivot_column (columns_to_show, pivot_column, aggregate_column, my_table) %}

{%- set my_query -%}
SELECT distinct {{pivot_column}}  FROM {{my_table}} ;
{%- endset -%}

{%- set results = run_query(my_query) -%} 

{%- if execute -%}
{#- Return the first column -#}
{%- set items = results.columns[0].values() -%}
{%- else -%}
{%- set items = [] -%}
{%- endif %}

SELECT {{columns_to_show}} ,
{%- for i in items %}
sum("{{i}}_views") AS "{{i}}_views"
{%- if not loop.last %} , {% endif -%}
{%- endfor %}
FROM (
        SELECT  {{columns_to_show}},  
        {%- for i in items %}
        CASE WHEN {{pivot_column}} = '{{i}}'  THEN {{aggregate_column}} ELSE 0 END AS "{{i}}_views"
        {%- if not loop.last %} , {% endif -%}
        {%- endfor %}
        FROM {{my_table}}   )
GROUP BY  {{columns_to_show}} 
{% endmacro %}
