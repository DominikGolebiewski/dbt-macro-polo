{% macro set_runtime_state(key, value, model_id=none, macro_name=none) %}
  {{ return(adapter.dispatch('set_runtime_state', 'dbt_macro_polo')(key, value, model_id, macro_name)) }}
{% endmacro %}

{% macro default__set_runtime_state(key, value, model_id=none) %}

    {% set macro_polo = var('macro_polo', {}) %}
    {% set macro_name = 'set_runtime_state' %}
    {% do macro_polo.get('runtime_state', {}).update({key: value}) %}

    {% set msg = "Saving value '" ~ value ~ "' to runtime state with key '" ~ key ~ "'" %}
    {{ dbt_macro_polo.log_event(message=msg, level='DEBUG', model_id=this, macro_name=macro_name) }}

{% endmacro %}
