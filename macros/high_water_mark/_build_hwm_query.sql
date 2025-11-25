{% macro _build_hwm_query(column_name, relation, predicate) %}
  {{ return(adapter.dispatch('_build_hwm_query', 'dbt_macro_polo')(column_name, relation, predicate)) }}
{% endmacro %}

{% macro default___build_hwm_query(column_name, relation, predicate) %}
    {% set query %}
        select coalesce(max({{ column_name }}), '0') as max_value
        from {{ relation }}
        {% if predicate %}
            where {{ predicate }}
        {% endif %}
    {% endset %}
    {{ return(query) }}
{% endmacro %}
