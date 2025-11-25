{% macro _get_adaptive_config(model, this, operation, macro_name) %}
  {{ return(adapter.dispatch('_get_adaptive_config', 'dbt_macro_polo')(model, this, operation, macro_name)) }}
{% endmacro %}

{% macro default___get_adaptive_config(model, this, operation, macro_name) %}

    {% set model_id = this.schema ~ "." ~ this.name %}
    {% set global_config = var('macro_polo', {}).get('adaptive_compute', {}) %}
    {% set model_config = model.config.get('meta', {}).get('adaptive_compute', {}) %}

    {# Check enablement #}
    {% if not (global_config.get('enabled', false) and model_config.get('enabled', false)) %}
        {% if operation == 'build' %}
            {{ dbt_macro_polo.log_event(
                message="Adaptive Compute disabled",
                level='INFO',
                model_id=model_id,
                macro_name=macro_name
            ) }}
        {% endif %}
        {{ return(none) }}
    {% endif %}

    {# Check incremental strategy #}
    {% set is_incremental = model.config.get('materialized') == 'incremental' %}
    {% if not (is_incremental and model.config.get('incremental_strategy') == 'delete+insert') %}
        {{ dbt_macro_polo.log_event(
            message="Requires incremental materialization with 'delete+insert' strategy",
            level='ERROR',
            model_id=model_id,
            macro_name=macro_name
        ) }}
        {{ return(none) }}
    {% endif %}

    {{ return(model_config) }}

{% endmacro %}
