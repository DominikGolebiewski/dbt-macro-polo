{% macro require(value, message, macro_name) %}
    {{ return(adapter.dispatch('require', 'dbt_macro_polo')(value, message, macro_name)) }}
{% endmacro %}

{% macro default__require(value, message, macro_name) %}
    {% if value %}
        {{ return(value) }}
    {% else %}
        {{ dbt_macro_polo.log_event(message=message, level='WARN', macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}
{% endmacro %}