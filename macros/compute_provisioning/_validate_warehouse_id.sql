{% macro _validate_warehouse_id(warehouse_id, model_id, macro_name) %}
  {{ return(adapter.dispatch('_validate_warehouse_id', 'dbt_macro_polo')(warehouse_id, model_id, macro_name)) }}
{% endmacro %}

{% macro default___validate_warehouse_id(warehouse_id, model_id, macro_name) %}
    {% if warehouse_id | length > 255 %}
        {% set msg = "Configuration Error: Generated warehouse_id exceeds 255 characters: " ~ warehouse_id %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
        {{ return(false) }}
    {% endif %}
    {{ return(true) }}
{% endmacro %}
