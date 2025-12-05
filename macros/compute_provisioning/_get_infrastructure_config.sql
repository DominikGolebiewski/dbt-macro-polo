{% macro _get_infrastructure_config() %}
  {{ return(adapter.dispatch('_get_infrastructure_config', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default___get_infrastructure_config() %}
    
    {% set macro_name = '_get_infrastructure_config' %}

    {% if var('macro_polo', {}) == {} %}
        {% set msg = "Configuration Error (dbt_project.yml): macro_polo must be defined in project variable." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}

    {% set macro_polo = var('macro_polo', {}) %}
    
    {% if 'infrastructure_definition' not in macro_polo %}
        {% set msg = "Configuration Error (dbt_project.yml): infrastructure_definition must be defined in macro_polo project variable." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}

    {% set infrastructure_definition = macro_polo.get('infrastructure_definition', {}) %}

    {% if 'allowed_sizes' not in infrastructure_definition %}
        {% set msg = "Configuration Error (dbt_project.yml): allowed_sizes must be defined in infrastructure_definition variable." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}

    {% if 'environment_context' not in infrastructure_definition %}
        {% set msg = "Configuration Error (dbt_project.yml): environment_context must be defined in infrastructure_definition variable." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}

    {% set environments = infrastructure_definition.get('environment_context', {}) %}
    {% set environment_config = environments.get(target.name, {}) %}

    {% if environment_config == {} %}
        {% set msg = "Configuration Error (dbt_project.yml): No configuration found in environment_context for target environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}

    {% if 'warehouse_name_prefix' not in environment_config %}
        {% set msg = "Configuration Error (dbt_project.yml): warehouse_name_prefix missing for environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {{ return(infrastructure_definition) }}

{% endmacro %}
