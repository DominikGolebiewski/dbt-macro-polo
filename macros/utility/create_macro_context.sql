{% macro create_macro_context(macro_name) %}
    {{ return(adapter.dispatch('create_macro_context', 'dbt_macro_polo')(macro_name)) }}
{% endmacro %}

{% macro default__create_macro_context(macro_name) %}
    {% set macro_polo = var('macro_polo', {}) %}
    {% set logging_level = macro_polo.get('logging_level', 'info') %}

    {% set model_id = 'unknown_model' %}
    {% if this %}
        {% set model_id = this.schema | lower ~ '.' ~ this.name | lower %}
    {% endif %}

    {% set context = {
        'macro_name': macro_name | upper,
        'model_id': model_id,
        'materialisation': config.get('materialized', 'undefined'),
        'logging_level': logging_level,
        'is_adaptive_compute_enabled': macro_polo.get('adaptive_compute', {}).get('enabled', false)
     } %}
    {{ return(context) }}
{% endmacro %}
