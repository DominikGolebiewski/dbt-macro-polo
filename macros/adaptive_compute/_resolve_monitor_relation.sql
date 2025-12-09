{% macro _resolve_monitor_relation(monitor) %}
  {{ return(adapter.dispatch('_resolve_monitor_relation', 'dbt_macro_polo')(monitor)) }}
{% endmacro %}

{% macro default___resolve_monitor_relation(monitor) %}

    {% if '.' in monitor %}
        {% set parts = monitor.split('.') %}
        {{ return(source(parts[0], parts[1])) }}
    {% else %}
        {{ return(ref(monitor)) }}
    {% endif %}
    
{% endmacro %}
