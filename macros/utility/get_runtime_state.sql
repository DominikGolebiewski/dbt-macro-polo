{% macro get_runtime_state(key) %}
    {{ return(adapter.dispatch('get_runtime_state', 'dbt_macro_polo')(key)) }}
{% endmacro %}

{% macro default__get_runtime_state(key) %}

    {% set macro_polo = var('macro_polo', {}) %}
    {% set macro_name = 'get_runtime_state' %}
    {% set state = macro_polo.get('runtime_state', {}) %}
    {% set value = state.get(key) %}

    {% if value %}
        {% set msg = "Runtime state hit for key: " ~ key %}
        {{ dbt_macro_polo.log_event(message=msg, status=value | upper,level='DEBUG',macro_name=macro_name) }}
    {% else %}
        {% set msg = "Runtime state miss for key: " ~ key %}
        {{ dbt_macro_polo.log_event(message=msg, level='DEBUG', macro_name=macro_name) }}
    {% endif %}

    {{ return(value) }}

{% endmacro %}
