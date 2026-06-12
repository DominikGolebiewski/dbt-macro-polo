{% macro handle_adaptive(model_id, query_operation, row_count) %}
    {{ return(adapter.dispatch('handle_adaptive', 'dbt_macro_polo')(model_id, query_operation, row_count)) }}
{% endmacro %}

{# Adaptive (ML) warehouse sizing. Returns a validated warehouse size, or '' to make
   the caller fall through to the existing thresholds/scheduling/default chain.
   Must never raise: every failure path logs at WARN/INFO/DEBUG and returns ''. #}
{% macro snowflake__handle_adaptive(model_id, query_operation, row_count) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set macro_ctx = dbt_macro_polo.create_macro_context('handle_adaptive') %}
    {% set macro_name = macro_ctx.macro_name %}
    {% set model_id = macro_ctx.model_id %}
    {% set macro_polo = var('macro_polo', {}) %}

    {% set adaptive_config = dbt_macro_polo.get_adaptive_config() %}
    {% if not adaptive_config.enabled %}
        {{ return('') }}
    {% endif %}

    {# Reuse the decision made for the first operation phase (ctas) for delete/insert.
       The sentinel 'none' caches a negative decision so fallback isn't re-evaluated. #}
    {% set size_cache_key = '_polo_adaptive_size_' ~ model_id | replace('.', '_') %}
    {% set cached_size = dbt_macro_polo.get_cache_value(size_cache_key) %}
    {% if cached_size == 'none' %}
        {{ return('') }}
    {% elif cached_size %}
        {{ dbt_macro_polo.logging(message="Adaptive warehouse size from cache", model_id=model_id, status=cached_size | upper) }}
        {{ return(cached_size) }}
    {% endif %}

    {% set recommendations = dbt_macro_polo.load_adaptive_recommendations(adaptive_config) %}
    {% if not recommendations %}
        {% do macro_polo.get('cache', {}).update({size_cache_key: 'none'}) %}
        {{ return('') }}
    {% endif %}

    {% set rec = recommendations.get(model_id) %}
    {% if not rec %}
        {{ dbt_macro_polo.logging(message="No adaptive recommendation for this model yet (cold start), falling back", model_id=model_id) }}
        {% do macro_polo.get('cache', {}).update({size_cache_key: 'none'}) %}
        {{ return('') }}
    {% endif %}

    {% if rec.confidence is none or rec.confidence < adaptive_config.confidence_threshold %}
        {{ dbt_macro_polo.logging(message="Adaptive recommendation below confidence threshold ("
            ~ rec.confidence ~ " < " ~ adaptive_config.confidence_threshold ~ "), falling back", model_id=model_id) }}
        {% do macro_polo.get('cache', {}).update({size_cache_key: 'none'}) %}
        {{ return('') }}
    {% endif %}

    {# Hybrid deviation check: use the precomputed recommendation while the incoming
       batch looks like the batches the model was trained on #}
    {% set ns = namespace(candidate='') %}
    {% set deviation = adaptive_config.deviation_threshold %}
    {% set in_range = true %}
    {% if rec.p10 is not none and rec.p90 is not none %}
        {% set lower_bound = rec.p10 * (1 - deviation) %}
        {% set upper_bound = rec.p90 * (1 + deviation) %}
        {% set in_range = row_count >= lower_bound and row_count <= upper_bound %}
    {% endif %}

    {% if in_range %}
        {% set ns.candidate = rec.size %}
        {{ dbt_macro_polo.logging(macro_name, "Batch within trained range (rows: " ~ row_count ~ "), using precomputed recommendation", model_id=model_id, level='DEBUG') }}
    {% elif adaptive_config.live_predict_enabled %}
        {{ dbt_macro_polo.logging(message="Batch outside trained range (rows: " ~ row_count ~ "), requesting live prediction", model_id=model_id) }}

        {% set features = dbt_macro_polo.extract_compile_features(model.raw_code, model.depends_on.nodes | length) %}
        {% set relation = adapter.get_relation(this.database, this.schema, this.name) %}
        {% do features.update({
            'model_id': model_id,
            'upstream_row_count': row_count,
            'is_full_refresh': dbt_macro_polo.should_full_refresh(),
            'column_count': adapter.get_columns_in_relation(relation) | length if relation is not none else 0
        }) %}

        {% set prediction = dbt_macro_polo.predict_warehouse_size(adaptive_config, features) %}
        {% if prediction and prediction.confidence >= adaptive_config.confidence_threshold %}
            {% set ns.candidate = prediction.size %}
        {% else %}
            {{ dbt_macro_polo.logging(message="Live prediction unavailable or below confidence threshold, falling back", model_id=model_id, level='WARN') }}
            {% do macro_polo.get('cache', {}).update({size_cache_key: 'none'}) %}
            {{ return('') }}
        {% endif %}
    {% else %}
        {{ dbt_macro_polo.logging(message="Batch outside trained range (rows: " ~ row_count ~ ") and live prediction disabled, falling back", model_id=model_id) }}
        {% do macro_polo.get('cache', {}).update({size_cache_key: 'none'}) %}
        {{ return('') }}
    {% endif %}

    {# Validate the candidate against the consumer's configured sizes - never pass an
       unvalidated value to allocate_warehouse (which raises on invalid sizes) #}
    {% set size_order = ['xs', 's', 'm', 'l', 'xl', '2xl', '3xl', '4xl', '5xl', '6xl'] %}
    {% set configured_sizes = macro_polo.get('warehouse_config', {}).get('warehouse_size') or [] %}
    {% set ns.candidate = ns.candidate | trim | lower %}

    {% if ns.candidate not in size_order or ns.candidate not in configured_sizes %}
        {{ dbt_macro_polo.logging(message="Adaptive recommendation '" ~ ns.candidate
            ~ "' is not in configured warehouse sizes " ~ configured_sizes ~ ", falling back", model_id=model_id, level='WARN') }}
        {% do macro_polo.get('cache', {}).update({size_cache_key: 'none'}) %}
        {{ return('') }}
    {% endif %}

    {# Optional per-model hard cap on the ML recommendation #}
    {% set max_size = adaptive_config.max_warehouse_size %}
    {% if max_size and max_size in size_order and max_size in configured_sizes and size_order.index(ns.candidate) > size_order.index(max_size) %}
        {{ dbt_macro_polo.logging(message="Capping adaptive recommendation " ~ ns.candidate | upper
            ~ " at max_warehouse_size", model_id=model_id, status=max_size | upper) }}
        {% set ns.candidate = max_size %}
    {% endif %}

    {% do macro_polo.get('cache', {}).update({size_cache_key: ns.candidate}) %}
    {{ dbt_macro_polo.logging(message="Adaptive warehouse size selected", model_id=model_id, status=ns.candidate | upper) }}
    {{ return(ns.candidate) }}
{% endmacro %}
