{% macro get_adaptive_config(include_model_meta=true) %}
    {{ return(adapter.dispatch('get_adaptive_config', 'dbt_macro_polo')(include_model_meta)) }}
{% endmacro %}

{% macro default__get_adaptive_config(include_model_meta=true) %}

    {% set macro_polo = var('macro_polo', {}) %}

    {# Safe-off defaults: adaptive mode never activates unless explicitly enabled #}
    {% set adaptive_config = {
        'enabled': false,
        'audit_schema': 'polo_audit',
        'audit_table': 'warehouse_optimiser_runs',
        'recommendations_table': 'warehouse_optimiser_recommendations',
        'model_name': 'polo_warehouse_classifier',
        'training_source': 'auto',
        'history_query_tag_pattern': none,
        'training_models': [],
        'target_duration_seconds': 300,
        'confidence_threshold': 0.7,
        'deviation_threshold': 0.25,
        'min_training_samples': 50,
        'live_predict_enabled': false,
        'account_usage_lookback_days': 30,
        'max_warehouse_size': none
    } %}

    {# Project-level overrides #}
    {% set project_adaptive = macro_polo.get('warehouse_optimiser', {}).get('adaptive', {}) %}
    {% if project_adaptive is mapping %}
        {% do adaptive_config.update(project_adaptive) %}
    {% endif %}

    {# Model-level overrides (meta.warehouse_optimiser.adaptive) - only in node context #}
    {% if include_model_meta and model is defined %}
        {% set model_adaptive = model.config.get('meta', {}).get('warehouse_optimiser', {}).get('adaptive', {}) %}
        {% if model_adaptive is mapping %}
            {% do adaptive_config.update(model_adaptive) %}
        {% endif %}
    {% endif %}

    {{ return(adaptive_config) }}
{% endmacro %}
