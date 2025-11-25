{% macro _get_environment_config(infrastructure_def, model_id, macro_name) %}
  {{ return(adapter.dispatch('_get_environment_config', 'dbt_macro_polo')(infrastructure_def, model_id, macro_name)) }}
{% endmacro %}

{% macro default___get_environment_config(infrastructure_def, model_id, macro_name) %}
    {% set environments = infrastructure_def.get('environment_context', {}) %}
    {% set env_config = environments.get(target.name) %}

    {% if not env_config %}
        {% set msg = "Configuration Error (dbt_project.yml): No configuration found for target environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {% set warehouse_prefix = env_config.get('warehouse_name_prefix') %}
    {% if not warehouse_prefix %}
        {% set msg = "Configuration Error (dbt_project.yml): warehouse_name_prefix missing for environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {{ return(warehouse_prefix) }}
{% endmacro %}
