{% macro allocate_warehouse(incremental_size, fullrefresh_size=none) %}
    {{ return(adapter.dispatch('allocate_warehouse', 'dbt_macro_polo')(incremental_size, fullrefresh_size)) }}
{% endmacro %}

{% macro snowflake__allocate_warehouse(incremental_size, fullrefresh_size=none) %}

    {% set macro_polo = var('macro_polo', {}) %}
    {% set ctx = dbt_macro_polo.create_macro_context('allocate_warehouse') %}
    {% set model_id = ctx.model_id %}

    {% set inc = (incremental_size or '') | trim | lower %}
    {% if not inc %}
        {{ dbt_macro_polo.logging(message='Configuration Error: incremental_size is required', level='ERROR', model_id=model_id) }}
        {{ return(false) }}
    {% endif %}

    {% set fr = (fullrefresh_size or inc) | trim | lower %}
    {% set cfg = macro_polo.get('warehouse_config', {}) %}
    {% set sizes = cfg.get('warehouse_size') or [] %}
    {% set env_cfg = cfg.get('environment', {}).get(target.name, {}) %}
    {% set prefix = env_cfg.get('warehouse_name_prefix') %}

    {% if not sizes or not prefix %}
        {{ dbt_macro_polo.logging(message='Configuration Error: warehouse_config is incomplete', level='ERROR', model_id=model_id) }}
        {{ return(false) }}
    {% endif %}

    {% set size = fr if dbt_macro_polo.should_full_refresh() else inc %}
    {% if size not in sizes %}
        {{ dbt_macro_polo.logging(message='Invalid warehouse size: ' ~ size ~ '. Valid sizes: ' ~ sizes, level='ERROR', model_id=model_id) }}
        {{ return(false) }}
    {% endif %}

    {% set cache_key = '_macro_polo_allocate_warehouse_' ~ size %}
    {% set wh = dbt_macro_polo.get_cache_value(cache_key) %}
    {% if wh %}
        {{ dbt_macro_polo.logging(message='Allocated warehouse from cache', model_id=model_id, status=wh | upper) }}
        {{ return(wh) }}
    {% endif %}

    {% set wh = prefix ~ '_' ~ size %}
    {% if wh | length > 255 %}
        {{ dbt_macro_polo.logging(message="Generated warehouse_id exceeds maximum length: '" ~ wh ~ "'", level='ERROR') }}
        {{ return(false) }}
    {% endif %}

    {{ dbt_macro_polo.logging(ctx.macro_name, message="Caching warehouse '" ~ wh ~ "' with cache key '" ~ cache_key ~ "'", level='DEBUG') }}
    {% do macro_polo.cache.update({cache_key: wh}) %}
    {{ dbt_macro_polo.logging(message='Allocated warehouse', model_id=model_id, status=wh | upper) }}
    {{ return(wh) }}

{% endmacro %}

