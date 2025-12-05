{% macro _get_infrastructure_config() %}
  {{ return(adapter.dispatch('_get_infrastructure_config', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default___get_infrastructure_config() %}
    
    {% set macro_polo = var('macro_polo', {}) %}
    {% set macro_name = '_get_infrastructure_config' %}
    
    {% if 'infrastructure_definition' not in macro_polo %}
        {% set msg = "Configuration Error (dbt_project.yml): infrastructure_definition must be defined in macro_polo project variable." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}

    {% set infrastructure_definition = macro_polo.get('infrastructure_definition', {}) %}

    {#/* Validate allowed_sizes */#}
    {% set available_sizes = ['xs', 's', 'm', 'l', 'xl', '2xl', '3xl', '4xl', '5xl', '6xl'] %}

    {% if 'allowed_sizes' not in infrastructure_definition %}
        {% set msg = "Configuration Error (dbt_project.yml): allowed_sizes must be defined in infrastructure_definition object." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}
    
    {% set allowed_sizes = infrastructure_definition.get('allowed_sizes', []) %}

    {% if allowed_sizes == [] or not (allowed_sizes is sequence and allowed_sizes is not string and allowed_sizes is not mapping) %}
        {% set msg = "Configuration Error (dbt_project.yml): allowed_sizes parameter must be a list of strings. Current value: " ~ allowed_sizes %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return([]) }}
    {% endif %}

    {% set invalid_allowed_sizes = [] %}
    {% for size in allowed_sizes %}
        {% if size | lower not in available_sizes %}
            {% do invalid_allowed_sizes.append(size) %}
        {% endif %}
    {% endfor %}

    {% if invalid_allowed_sizes != [] %}
        {% set msg = "Configuration Error (dbt_project.yml): Invalid warehouse size(s) configured: " ~ invalid_allowed_sizes ~ ". Valid sizes are: " ~ available_sizes %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return([]) }}
    {% endif %}

    {% if 'environment_context' not in infrastructure_definition %}
        {% set msg = "Configuration Error (dbt_project.yml): environment_context must be defined." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}

    {#/* Get environment configuration */#}
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

    {#/* Get warehouse prefix */#}
    {% set warehouse_prefix = environment_config.get('warehouse_name_prefix', none) %}

    {% if warehouse_prefix is none or warehouse_prefix == '' %}
        {% set msg = "Configuration Error (dbt_project.yml): warehouse_name_prefix value cannot be none or empty for environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {{ return(infrastructure_definition) }}

{% endmacro %}
