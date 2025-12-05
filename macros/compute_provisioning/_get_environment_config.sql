{% macro _get_environment_config(infrastructure_definition) %}
  {{ return(adapter.dispatch('_get_environment_config', 'dbt_macro_polo')(infrastructure_definition)) }}
{% endmacro %}

{% macro default___get_environment_config(infrastructure_definition) %}

    {% set macro_name = '_get_environment_config' %}

    {% if 'environment_context' not in infrastructure_definition %}
        {% set msg = "Configuration Error (dbt_project.yml): environment_context must be defined." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}

    {#/* Get environment configuration */#}
    {% set environments = infrastructure_definition.get('environment_context', {}) %}
    {% set environment_config = environments.get(target.name, {}) %}

    {% if environment_config == {}%}
        {% set msg = "Configuration Error (dbt_project.yml): No configuration found in environment_context for target environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}

    {% if 'warehouse_name_prefix' not in environment_config %}
        {% set msg = "Configuration Error (dbt_project.yml): warehouse_name_prefix missing for environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {#/* Get warehouse prefix */#}
    {% set warehouse_prefix = environment_config.get('warehouse_name_prefix', none) %}

    {% if warehouse_prefix is none or warehouse_prefix == '' %}
        {% set msg = "Configuration Error (dbt_project.yml): warehouse_name_prefix value cannot be none or empty for environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {{ return(warehouse_prefix) }}

{% endmacro %}
