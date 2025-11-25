{% macro set_runtime_state(key, value, model_id=none, macro_name=none) %}
  {{ return(adapter.dispatch('set_runtime_state', 'dbt_macro_polo')(key, value, model_id, macro_name)) }}
{% endmacro %}

{% macro default__set_runtime_state(key, value, model_id=none, macro_name=none) %}
    {#
    Sets a value in the runtime state (cache).

    Args:
        key (str): The key to set.
        value (Any): The value to cache.
        model_id (str, optional): Context model ID for logging.
        macro_name (str, optional): Calling macro name for logging.
    #}
    {% set macro_polo = var('macro_polo', {}) %}
    {% do macro_polo.get('runtime_state', {}).update({key: value}) %}

    {{ dbt_macro_polo.log_event(
        message="Saving value '" ~ value ~ "' to runtime state with key '" ~ key ~ "'",
        level='DEBUG',
        model_id=model_id,
        macro_name=macro_name
    ) }}
{% endmacro %}
