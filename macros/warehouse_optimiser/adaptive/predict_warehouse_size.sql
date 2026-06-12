{% macro predict_warehouse_size(adaptive_config, features) %}
    {{ return(adapter.dispatch('predict_warehouse_size', 'dbt_macro_polo')(adaptive_config, features)) }}
{% endmacro %}

{% macro snowflake__predict_warehouse_size(adaptive_config, features) %}

    {% if not execute %}
        {{ return(none) }}
    {% endif %}

    {% set model_fqn = target.database ~ '.' ~ adaptive_config.audit_schema ~ '.' ~ adaptive_config.model_name %}

    {# Feature keys must match the training view columns (uppercase, unquoted).
       The whole call is wrapped in a Snowflake Scripting block so a missing model,
       missing privileges or a prediction error can never abort the dbt run. #}
    {% set query %}
        execute immediate $$
        declare
            res variant;
        begin
            select {{ model_fqn }}!PREDICT(INPUT_DATA => object_construct(
                'MODEL_ID', '{{ features.model_id | replace("'", "''") }}',
                'UPSTREAM_ROW_COUNT', {{ features.upstream_row_count }},
                'IS_FULL_REFRESH', {{ 'true' if features.is_full_refresh else 'false' }},
                'JOIN_COUNT', {{ features.join_count }},
                'CTE_COUNT', {{ features.cte_count }},
                'UNION_COUNT', {{ features.union_count }},
                'WINDOW_FN_COUNT', {{ features.window_fn_count }},
                'QUERY_LENGTH', {{ features.query_length }},
                'UPSTREAM_COUNT', {{ features.upstream_count }},
                'COLUMN_COUNT', {{ features.column_count }}
            )) into :res;
            return res:class::varchar || '|' || coalesce(get(res:probability, res:class::varchar)::varchar, '0');
        exception
            when other then
                return null;
        end;
        $$
    {% endset %}

    {% set results = run_query(query) %}
    {% set raw_prediction = results.columns[0].values()[0] %}

    {% if raw_prediction is none or '|' not in (raw_prediction | string) %}
        {{ dbt_macro_polo.logging(message="Live prediction unavailable (model missing, privileges or prediction error)", level='WARN') }}
        {{ return(none) }}
    {% endif %}

    {% set parts = (raw_prediction | string).split('|') %}
    {% set prediction = {'size': parts[0] | trim | lower, 'confidence': parts[1] | float} %}
    {{ dbt_macro_polo.logging(message="Live prediction: " ~ prediction.size ~ " (confidence " ~ prediction.confidence ~ ")", level='DEBUG') }}
    {{ return(prediction) }}
{% endmacro %}
