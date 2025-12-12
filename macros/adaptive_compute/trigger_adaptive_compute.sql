{% macro trigger_adaptive_compute() %}
    {{ return(adapter.dispatch('trigger_adaptive_compute', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default__trigger_adaptive_compute() %}

    {% set adaptive_config = var('macro_polo', {}).get('adaptive_compute', {}) %}
    {% set model_config = model.config.get('meta', {}).get('adaptive_compute', {}) %}

    {% if adaptive_config.get('enabled', false) and model_config.get('enabled', false) %}
        {{ dbt_macro_polo.log_event(message="Triggering adaptive compute", level='INFO', model_id=this, macro_name='trigger_adaptive_compute') }}
        {{ dbt_macro_polo.adaptive_compute() }}
    {% else %}
        {{ dbt_macro_polo.log_event(message="Adaptive compute not enabled", level='INFO', model_id=this, macro_name='trigger_adaptive_compute') }}
    {% endif %}
    
{% endmacro %}