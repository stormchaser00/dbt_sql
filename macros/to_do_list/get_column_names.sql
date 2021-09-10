--  https://emilyriederer.netlify.app/post/convo-dbt/
--  get_column_names() simply queries the databases’ built in INFORMATION_SCHEMA11 
--  to collect all column names of a given table. 
--  In the case of the model_monitor.sql script, the table provided is the staging table 
--  (model_monitor_staging) which was made in the previous step.

{% macro get_column_names(relation) %}

{% set relation_query %}
select column_name
FROM {{relation.database}}.{{relation.schema}}.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = '{{relation.identifier}}';
{% endset %}

{% set results = run_query(relation_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}
