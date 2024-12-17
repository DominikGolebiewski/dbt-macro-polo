{% macro get_cache_value(cache_key) %}
    {{ return(adapter.dispatch('get_cache_value', 'dbt_macro_polo')(cache_key)) }}
{% endmacro %}

{% macro default__get_cache_value(cache_key) %}
    {% set macro_ctx = dbt_macro_polo.create_macro_context('get_cache_value') %}
    {% set macro_name = macro_ctx.macro_name %}
    {% set model_id = macro_ctx.model_id %}
    
    {% set macro_polo = var('macro_polo', {}) %}
    {% set cache = macro_polo.get('cache', {}) %}
    {% set cache_value = cache.get(cache_key, {}) %}
    {{ dbt_macro_polo.logging(macro_name, message="Cache handling: " ~ {'cache_key': cache_key, 'cache_value': cache_value}, level='DEBUG', model_id=model_id) }}
    {{ return(cache_value) }}
{% endmacro %}