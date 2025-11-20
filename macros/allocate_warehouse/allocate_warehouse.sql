{% macro allocate_warehouse(incremental_size, fullrefresh_size=none) %}
    {{ return(adapter.dispatch('allocate_warehouse', 'dbt_macro_polo')(incremental_size, fullrefresh_size)) }}
{% endmacro %}

{% macro snowflake__allocate_warehouse(incremental_size, fullrefresh_size=none) %}

    {% set macro_polo = var('macro_polo', {}) %}

    {# Validate input parameters #}
    {% if not incremental_size %}
        {% set msg = "Configuration Error: incremental_size parameter is required" %}
        {{ dbt_macro_polo.logging(message=msg, level='ERROR', model_id=model_id) }}
        {{ exceptions.raise_compiler_error(msg) }}
    {% endif %}

    {% set incremental = incremental_size | trim | lower %}
    {% set fullrefresh = (fullrefresh_size or incremental_size) | trim | lower %}

    {# Validate relation and materialisation #}
    {% set is_relation_exist = load_relation(this) is not none %}
    {% set materialisation = config.get('materialized', 'undefined') | lower %}
    {% set is_full_refresh = flags.FULL_REFRESH or not is_relation_exist or materialisation == 'table' %}
    {% set size_suffix = fullrefresh if is_full_refresh else incremental %}

    {# Get and validate configuration #}
    {% set warehouse_config = macro_polo.get('warehouse_config', {}) %}
    {% if not warehouse_config %}
        {% set msg = "Configuration Error (dbt_project.yml): warehouse_config project variable must be defined." %}
        {{ dbt_macro_polo.logging(message=msg, level='ERROR', model_id=model_id) }}
    {% endif %}

    {# Get and validate environment configuration #}
    {% set environments = warehouse_config.get('environment', {}) %}
    {% set env_config = environments.get(target.name) %}

    {% set warehouse_prefix = env_config.get('warehouse_name_prefix') %}
    {% if not warehouse_prefix %}
        {% set msg = "Configuration Error (dbt_project.yml): warehouse_name_prefix missing for environment: " ~ target.name %}
        {{ dbt_macro_polo.logging(message=msg, level='ERROR', model_id=model_id) }}
    {% endif %}

    {# Cache handling #}
    {% set cache_key = '_macro_polo_allocate_warehouse_' ~  warehouse_prefix ~ '_' ~ size_suffix %}
    {% set cache_value = dbt_macro_polo.get_cache_value(cache_key) %}
    
    {% if cache_value %}
        {{ dbt_macro_polo.logging(message="Allocated warehouse from cache", model_id=model_id, status=cache_value | upper) }}
        {{ return(cache_value) }}
    {% endif %}

    {# Get and validate configured sizes #}
    {% set available_sizes = ['xs', 's', 'm', 'l', 'xl', '2xl', '3xl', '4xl', '5xl', '6xl'] %}
    {% set configured_sizes = warehouse_config.get('warehouse_size') %}

    {% if not configured_sizes or not (configured_sizes is sequence and configured_sizes is not string and configured_sizes is not mapping) %}
        {% set msg = "Configuration Error (dbt_project.yml): warehouse_size parameter must be a list of strings. Current value: " ~ configured_sizes %}
        {{ dbt_macro_polo.logging(message=msg, level='ERROR', model_id=model_id) }}
        {{ exceptions.raise_compiler_error(msg) }}
    {% endif %}

    {% set invalid_configured_sizes = [] %}
    {% for size in configured_sizes %}
        {% if size not in available_sizes %}
            {% do invalid_configured_sizes.append(size) %}
        {% endif %}
    {% endfor %}

    {% if invalid_configured_sizes %}
        {% set msg = "Configuration Error (dbt_project.yml): Invalid warehouse size(s) configured: " ~ invalid_configured_sizes ~ ". Valid sizes are: " ~ available_sizes %}
        {{ dbt_macro_polo.logging(message=msg, level='ERROR', model_id=model_id) }}
        {{ exceptions.raise_compiler_error(msg) }}
    {% endif %}

    {# Validate requested sizes against configured sizes #}
    {% set invalid_requested_sizes = [] %}
    {% for size, label in [(incremental, 'incremental'), (fullrefresh, 'fullrefresh')] %}
        {% if size not in configured_sizes %}
            {% do invalid_requested_sizes.append(label ~ ': ' ~ size) %}
        {% endif %}
    {% endfor %}

    {% if invalid_requested_sizes %}
        {% set msg = "Configuration Error: Requested size(s) not in configured warehouse_size list: " ~ invalid_requested_sizes ~ ". Configured sizes: " ~ configured_sizes %}
        {{ dbt_macro_polo.logging(message=msg, level='ERROR', model_id=model_id) }}
        {{ exceptions.raise_compiler_error(msg) }}
    {% endif %}

    {% if not env_config %}
        {% set msg = "Configuration Error (dbt_project.yml): No configuration found for target environment: " ~ target.name %}
        {{ dbt_macro_polo.logging(message=msg, level='ERROR', model_id=model_id) }}
        {{ exceptions.raise_compiler_error(msg) }}
    {% endif %}

    {# Generate and validate warehouse identifier #}
    {% set warehouse_id = warehouse_prefix ~ "_" ~ size_suffix %}
    {% if warehouse_id | length > 255 %}
        {% set msg = "Configuration Error: Generated warehouse_id exceeds 255 characters: " ~ warehouse_id %}
        {{ dbt_macro_polo.logging(message=msg, level='ERROR', model_id=model_id) }}
        {{ exceptions.raise_compiler_error(msg) }}
    {% endif %}

    {# Cache and return result #}
    {{ dbt_macro_polo.logging(macro_name, message="Caching warehouse '" ~ warehouse_id ~ "' with cache key '" ~ cache_key ~ "'", level='DEBUG') }}
    {% do macro_polo.cache.update({cache_key: warehouse_id}) %}
    {{ dbt_macro_polo.logging(message="Allocated warehouse", model_id=model_id, status=warehouse_id | upper) }}
    {{ return(warehouse_id) }}
    
{% endmacro %}
