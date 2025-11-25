{% macro _validate_hwm_inputs(column_name, model_id, macro_name) %}
  {{ return(adapter.dispatch('_validate_hwm_inputs', 'dbt_macro_polo')(column_name, model_id, macro_name)) }}
{% endmacro %}

{% macro default___validate_hwm_inputs(column_name, model_id, macro_name) %}
    {% if not column_name %}
        {% set msg = "Configuration Error: column_name is required." %}
        {{ dbt_macro_polo.log_event(
            message=msg,
            level='ERROR',
            model_id=model_id,
            macro_name=macro_name
            ) }}
        {{ return(false) }}
    {% endif %}
    {{ return(true) }}
{% endmacro %}
