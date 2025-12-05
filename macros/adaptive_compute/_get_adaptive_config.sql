{% macro _get_adaptive_config(operation) %}
  {{ return(adapter.dispatch('_get_adaptive_config', 'dbt_macro_polo')(operation)) }}
{% endmacro %}

{% macro default___get_adaptive_config(operation) %}

    {% set macro_name = '_get_adaptive_config' %}
    {% set macro_polo = dbt_macro_polo.validate_macro_polo_var() %}

    {% if 'adaptive_compute' not in macro_polo %}
        {% set msg = "adaptive_compute not defined in macro_polo project variable. Adaptive compute is disabled." %}
        {{ dbt_macro_polo.log_event(message=msg, level='DEBUG', macro_name=macro_name) }}
    {% endif %}

    {% set global_config = macro_polo.get('adaptive_compute', {}) %}

    {% if 'adaptive_compute' not in model.config.get('meta', {}) %}
        {% set msg = "adaptive_compute not defined in model config. Adaptive compute is disabled." %}
        {{ dbt_macro_polo.log_event(message=msg, level='DEBUG', model_id=this, macro_name=macro_name) }}
    {% endif %}

    {% set model_config = model.config.get('meta', {}).get('adaptive_compute', {}) %}

    {#/* Check enablement of both flags */#}
    {% if not (global_config.get('enabled', false) and model_config.get('enabled', false)) %}
        {% if operation == 'build' %}
            {% set msg = "Adaptive compute is disabled. Global config: " ~ global_config.get('enabled', false) ~ " Model config: " ~ model_config.get('enabled', false) %}
            {{ dbt_macro_polo.log_event(message=msg, level='INFO', model_id=this, macro_name=macro_name) }}
        {% endif %}
        {{ return(none) }}
    {% endif %}

    {#/* Check incremental strategy */#}
    {% set is_incremental = model.config.get('materialized', 'undefined') | trim | lower == 'incremental' %}
    {% set is_delete_insert = model.config.get('incremental_strategy', 'undefined') | trim | lower == 'delete+insert' %}

    {% if not (is_incremental and is_delete_insert) %}
        {% set msg = "Requires incremental materialisation with 'delete+insert' strategy." %}
        {{ dbt_macro_polo.log_event(message=msg, level='WARN', model_id=this, macro_name=macro_name) }}
        {#/* {{ return(none) }} */#}
    {% endif %}

    {{ return(model_config) }}

{% endmacro %}
