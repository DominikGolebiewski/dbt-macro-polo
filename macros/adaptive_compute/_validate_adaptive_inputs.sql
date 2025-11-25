{% macro _validate_adaptive_inputs(model_id, operation, macro_name) %}
  {{ return(adapter.dispatch('_validate_adaptive_inputs', 'dbt_macro_polo')(model_id, operation, macro_name)) }}
{% endmacro %}

{% macro default___validate_adaptive_inputs(model_id, operation, macro_name) %}
    {% if operation not in ['build', 'append', 'prune'] %}
        {{ dbt_macro_polo.log_event(
            message="Invalid operation: " ~ operation,
            level='ERROR',
            model_id=model_id,
            macro_name=macro_name
        ) }}
        {{ return(none) }}
    {% endif %}

    {{ return(true) }}
{% endmacro %}
