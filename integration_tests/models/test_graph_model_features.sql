-- Test polo_get_graph_model_features: the training pipeline's query_history source
-- relies on it to map historical runs (node_id) onto optimiser-enabled models and
-- their compile-time complexity features.
{% set failed_tests = [] %}

{% if execute %}
    {% set entries = dbt_macro_polo.polo_get_graph_model_features() %}

    {% set found = namespace(basic=false) %}
    {% for entry in entries %}
        {% if entry.node_id.endswith('.test_warehouse_optimiser_basic') %}
            {% set found.basic = true %}
            {% if not entry.model_id or '.' not in entry.model_id %}
                {% do failed_tests.append('model_id: Expected schema.name format, got "' ~ entry.model_id ~ '"') %}
            {% endif %}
            {% for key in ['join_count', 'cte_count', 'union_count', 'window_fn_count', 'query_length', 'upstream_count'] %}
                {% if entry.features.get(key) is none %}
                    {% do failed_tests.append('features.' ~ key ~ ': Expected a value, got none') %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}

    {% if not found.basic %}
        {% do failed_tests.append('graph_scan: Expected test_warehouse_optimiser_basic (optimiser-enabled) in entries, got '
            ~ entries | length ~ ' entr(y/ies)') %}
    {% endif %}
{% endif %}

{# Report results #}
{% if failed_tests | length > 0 %}
    {{ dbt_macro_polo.logging(message="Failed tests:\n" ~ failed_tests | join('\n'), level='ERROR') }}
{% endif %}

select {{ 'true' if failed_tests | length == 0 else 'false' }} as test_passed
