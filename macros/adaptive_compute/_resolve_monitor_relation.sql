{% macro _resolve_monitor_relation(monitor) %}
  {{ return(adapter.dispatch('_resolve_monitor_relation', 'dbt_macro_polo')(monitor)) }}
{% endmacro %}

{% macro default___resolve_monitor_relation(monitor) %}
    {#
    Resolves a monitor string (ref or source) into a Relation object.

    Args:
        monitor (str): 'model_name' or 'source_name.table_name'

    Returns:
        Relation: The resolved relation object
    #}
    {% if '.' in monitor %}
        {% set parts = monitor.split('.') %}
        {{ return(source(parts[0], parts[1])) }}
    {% else %}
        {{ return(ref(monitor)) }}
    {% endif %}
{% endmacro %}
