{% macro execute_query_with_warehouse(query, warehouse_name) %}
  {{ return(adapter.dispatch('execute_query_with_warehouse', 'dbt_macro_polo')(query, warehouse_name)) }}
{% endmacro %}

{% macro default__execute_query_with_warehouse(query, warehouse_name) %}

    {% if warehouse_name %}
        {% do run_query('use warehouse ' ~ warehouse_name) %}
    {% endif %}

    {% set result = run_query(query) %}

    {% if warehouse_name %}
        {% do run_query('use warehouse ' ~ target.warehouse) %}
    {% endif %}

    {{ return(result) }}
{% endmacro %}
