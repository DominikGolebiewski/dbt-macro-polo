{% macro _get_warehouse_prefix(infrastructure_definition) %}
  {{ return(adapter.dispatch('_get_warehouse_prefix', 'dbt_macro_polo')(infrastructure_definition)) }}
{% endmacro %}

{% macro default___get_warehouse_prefix(infrastructure_definition) %}
    
    {% set macro_name = '_get_warehouse_prefix' %}
    {% set warehouse_prefix = infrastructure_definition.get('environment_context', {}).get(target.name, {}).get('warehouse_name_prefix', none) %}

    {% if warehouse_prefix is none or warehouse_prefix == '' %}
        {% set msg = "Configuration Error (dbt_project.yml): warehouse_name_prefix value cannot be none or empty for environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {{ return(warehouse_prefix) }}

{% endmacro %}
