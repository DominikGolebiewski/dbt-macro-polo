{% macro load_adaptive_recommendations(adaptive_config) %}
    {{ return(adapter.dispatch('load_adaptive_recommendations', 'dbt_macro_polo')(adaptive_config)) }}
{% endmacro %}

{% macro snowflake__load_adaptive_recommendations(adaptive_config) %}

    {% if not execute %}
        {{ return(none) }}
    {% endif %}

    {% set macro_polo = var('macro_polo', {}) %}

    {# One lookup per dbt invocation: the whole recommendations table is cached #}
    {% set cache_key = '_polo_adaptive_recommendations' %}
    {% set cache_value = dbt_macro_polo.get_cache_value(cache_key) %}
    {% if cache_value == 'unavailable' %}
        {{ return(none) }}
    {% elif cache_value %}
        {{ return(cache_value) }}
    {% endif %}

    {% set relation = adapter.get_relation(target.database, adaptive_config.audit_schema, adaptive_config.recommendations_table) %}
    {% if relation is none %}
        {{ dbt_macro_polo.logging(message="Adaptive recommendations table not found (cold start). Run"
            ~ " 'dbt run-operation polo_train_warehouse_model' once enough history is collected."
            ~ " Falling back to configured thresholds.", level='WARN') }}
        {% do macro_polo.get('cache', {}).update({cache_key: 'unavailable'}) %}
        {{ return(none) }}
    {% endif %}

    {% set query %}
        select
            model_id,
            recommended_warehouse_size,
            confidence,
            row_count_p10,
            row_count_p50,
            row_count_p90,
            training_samples
        from {{ relation }}
    {% endset %}

    {% set results = run_query(query) %}
    {% set recommendations = {} %}
    {% for row in results.rows %}
        {% do recommendations.update({row[0]: {
            'size': row[1],
            'confidence': row[2],
            'p10': row[3],
            'p50': row[4],
            'p90': row[5],
            'samples': row[6]
        }}) %}
    {% endfor %}

    {% if recommendations | length == 0 %}
        {{ dbt_macro_polo.logging(message="Adaptive recommendations table is empty, falling back to configured thresholds.", level='WARN') }}
        {% do macro_polo.get('cache', {}).update({cache_key: 'unavailable'}) %}
        {{ return(none) }}
    {% endif %}

    {{ dbt_macro_polo.logging(message="Loaded adaptive recommendations for " ~ recommendations | length ~ " model(s)") }}
    {% do macro_polo.get('cache', {}).update({cache_key: recommendations}) %}
    {{ return(recommendations) }}
{% endmacro %}
