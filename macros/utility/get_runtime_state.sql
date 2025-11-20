{% macro get_runtime_state(key) %}
    {{ return(adapter.dispatch('get_runtime_state', 'dbt_macro_polo')(key)) }}
{% endmacro %}

{% macro default__get_runtime_state(key) %}
    
    {% set macro_polo = var('macro_polo', {}) %}
    {% set state = macro_polo.get('runtime_state', {}) %}
    {% set value = state.get(key) %}
    
    {% if value %}
        {{ dbt_macro_polo.log_event(
            message="Runtime state hit for key: " ~ key, 
            status=value | upper, 
            level='DEBUG', 
            macro_name='get_runtime_state'
        ) }}
    {% else %}
        {{ dbt_macro_polo.log_event(
            message="Runtime state miss for key: " ~ key, 
            level='DEBUG', 
            macro_name='get_runtime_state'
        ) }}
    {% endif %}

    {{ return(value) }}
{% endmacro %}
