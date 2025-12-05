{% macro trigger_adaptive_compute() %}

    {% set adaptive_config = var('macro_polo', {}).get('adaptive_compute', {}) %}
    {% set model_config = model.config.get('meta', {}).get('adaptive_compute', {}) %}

    {% if adaptive_config.get('enabled', false) and model_config.get('enabled', false) %}
        {{ dbt_macro_polo.adaptive_compute(operation='build') }}
    {% endif %}
    
{% endmacro %}
