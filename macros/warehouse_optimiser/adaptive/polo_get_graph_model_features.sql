{# Collects compile-time features for every optimiser-enabled model in the current
   graph. Used by the training pipeline to attach complexity features (and the
   node_id -> model_id mapping) to historical runs found in
   snowflake.account_usage.query_history, which predate the telemetry audit table. #}

{% macro polo_get_graph_model_features() %}
    {{ return(adapter.dispatch('polo_get_graph_model_features', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default__polo_get_graph_model_features() %}
    {% set entries = [] %}
    {% if execute %}
        {% for node in graph.nodes.values() %}
            {% if node.resource_type == 'model' and node.config.get('meta', {}).get('warehouse_optimiser', {}).get('enabled', false) %}
                {% set features = dbt_macro_polo.extract_compile_features(node.raw_code, node.depends_on.nodes | length) %}
                {% do entries.append({
                    'node_id': node.unique_id,
                    'model_id': node.schema | lower ~ '.' ~ node.name | lower,
                    'database': node.database,
                    'schema': node.schema,
                    'alias': node.alias or node.name,
                    'features': features
                }) %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(entries) }}
{% endmacro %}
