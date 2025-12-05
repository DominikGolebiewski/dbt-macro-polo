{% macro _get_infrastructure_config() %}
  {{ return(adapter.dispatch('_get_infrastructure_config', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default___get_infrastructure_config() %}
    
    {% set macro_polo = var('macro_polo', {}) %}
    {% set macro_name = '_get_infrastructure_config' %}
    {% set infrastructure_definition = macro_polo.get('infrastructure_definition', {}) %}

    {% if not infrastructure_definition %}
        {% set msg = "Configuration Error (dbt_project.yml): infrastructure_definition project variable must be defined. Please refer to link: https://github.com/macro-polo/dbt-macro-polo/blob/main/macros/compute_provisioning/schema.md#configuration-requirements for more information." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {# Validate allowed_sizes #}
    {% set available_sizes = ['xs', 's', 'm', 'l', 'xl', '2xl', '3xl', '4xl', '5xl', '6xl'] %}
    {% set allowed_sizes = infrastructure_def.get('allowed_sizes') %}

    {% if not allowed_sizes or not (allowed_sizes is sequence and allowed_sizes is not string and allowed_sizes is not mapping) %}
        {% set msg = "Configuration Error (dbt_project.yml): allowed_sizes parameter must be a list of strings. Current value: " ~ allowed_sizes %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=this, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {% set invalid_allowed_sizes = [] %}
    {% for size in allowed_sizes %}
        {% if size not in available_sizes %}
            {% do invalid_allowed_sizes.append(size) %}
        {% endif %}
    {% endfor %}

    {% if invalid_allowed_sizes %}
        {% set msg = "Configuration Error (dbt_project.yml): Invalid warehouse size(s) configured: " ~ invalid_allowed_sizes ~ ". Valid sizes are: " ~ available_sizes %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=this, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {{ return(infrastructure_def) }}
{% endmacro %}
