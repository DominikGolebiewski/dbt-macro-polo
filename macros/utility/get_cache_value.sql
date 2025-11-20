{% macro get_cache_value(cache_key) %}
    {{ return(adapter.dispatch('get_cache_value', 'dbt_macro_polo')(cache_key)) }}
{% endmacro %}

{% macro default__get_cache_value(cache_key) %}
    
    {% set macro_polo = var('macro_polo', {}) %}
    {% set cache = macro_polo.get('cache', {}) %}
    {% set cache_value = cache.get(cache_key) %}
    
    {% if cache_value %}
        {{ dbt_macro_polo.logging(
            message="Cache hit for key: " ~ cache_key, 
            status="HIT", 
            level='DEBUG', 
            macro_name='get_cache_value'
        ) }}
    {% else %}
        {{ dbt_macro_polo.logging(
            message="Cache miss for key: " ~ cache_key, 
            level='DEBUG', 
            macro_name='get_cache_value'
        ) }}
    {% endif %}

    {{ return(cache_value) }}
{% endmacro %}
