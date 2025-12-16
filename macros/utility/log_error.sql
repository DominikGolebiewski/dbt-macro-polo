{% macro log_error(message, macro_name) %}
    {{ return(adapter.dispatch('log_error', 'dbt_macro_polo')(message, macro_name)) }}
{% endmacro %}

{% macro default__log_error(message, macro_name) %}
    {% set message = "Macro Polo Error: " ~ message %}
    {{ dbt_macro_polo.log_event(message=message, level='ERROR', model_id=this, macro_name=macro_name) }}
    {{ return({}) }}
{% endmacro %}