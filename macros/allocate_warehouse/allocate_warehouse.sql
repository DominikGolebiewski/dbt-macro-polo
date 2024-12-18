{% macro allocate_warehouse(incremental_size, fullrefresh_size=none) %}
    {{ return(adapter.dispatch('allocate_warehouse', 'dbt_macro_polo')(incremental_size, fullrefresh_size)) }}
{% endmacro %}  

{% macro snowflake__allocate_warehouse(incremental_size, fullrefresh_size=none) %}

    {% set macro_polo = var('macro_polo', {}) %}

    {# Initialise macro context #}
    {% set macro_ctx = dbt_macro_polo.create_macro_context('allocate_warehouse') %}
    {% set macro_name = macro_ctx.macro_name %}
    {% set model_id = macro_ctx.model_id %}

    {# Validate input parameters #}
    {% set incremental = incremental_size | trim | lower %}
    {% set fullrefresh = (fullrefresh_size or incremental_size) | trim | lower %}

    {# Validate relation and materialisation #}
    {% set is_relation_exist = load_relation(this) is not none %}
    {% set materialisation = config.get('materialized', 'undefined') | lower %}
    {% set is_full_refresh = flags.FULL_REFRESH or not is_relation_exist or materialisation == 'table' %}
    {% set size_suffix = fullrefresh if is_full_refresh else incremental %}

    {% if not incremental %}
        {{ dbt_macro_polo.logging(message="Configuration Error: Incremental size parameter is required", level='ERROR', model_id=model_id) }}
        {{ return(false) }}
    {% endif %}

    {# Cache handling #}
    {% set cache_key = '_macro_polo_allocate_warehouse_' ~ size_suffix %}
    {% set cache_value = dbt_macro_polo.get_cache_value(cache_key) %}
    
    {% if cache_value %}
        {{ dbt_macro_polo.logging(message="Allocated warehouse from cache", model_id=model_id, status=cache_value | upper) }}
        {{ return(cache_value) }}
    {% endif %}

    {# Get and validate configuration #}
    {% set warehouse_config = macro_polo.get('warehouse_config', {}) %}

    {% if not warehouse_config %}
        {{ dbt_macro_polo.logging(message="Configuration Error (dbt_project.yml): warehouse_config project variable must be defined."
        ~ "\nRequired structure:"
        ~ "\nvars:"
        ~ "\n  warehouse_config:",
        level='ERROR'
        ) }}
        {{ return(false) }}
    {% endif %}

    {# Get and validate configured sizes #}
    {% set available_sizes = ['xs', 's', 'm', 'l', 'xl', '2xl', '3xl', '4xl', '5xl', '6xl'] %}
    {% set configured_sizes = warehouse_config.get('warehouse_size') or [] %}

    {% if 'warehouse_size' not in warehouse_config %}
        {{ dbt_macro_polo.logging(message="Configuration Error (dbt_project.yml): Required parameter warehouse_size is not specified"
        ~ " in project variable warehouse_config."
        ~ "\nRequired structure:"
        ~ "\n  warehouse_config:"
        ~ "\n    warehouse_size: [ 'xs', 's', 'm' ]",
        level='ERROR'
        ) }}
        {{ return(false) }}
    {% endif %}

    {% if configured_sizes | length == 0 or configured_sizes is string or not configured_sizes or configured_sizes is mapping %}
        {{ dbt_macro_polo.logging(message="Configuration Error (dbt_project.yml): warehouse_size parameter must be provided as a list."
        ~ "\n Current value: " ~ configured_sizes 
        ~ "\n Expected format: List of strings."
        ~ "\n Example: warehouse_size: [ 'xs', 's', 'm', 'l', 'xl' ]", 
        level='ERROR'
        ) }}
        {{ return(false) }}
    {% endif %}

    {% set invalid_sizes = [] %}
    {% for size in configured_sizes %}
        {% if size not in available_sizes %}
            {% do invalid_sizes.append(size) %}
        {% endif %}
    {% endfor %}
    {% if invalid_sizes | length > 0 %}
        {{ dbt_macro_polo.logging(message="Configuration Error (dbt_project.yml): Warehouse size(s) configured: " ~ invalid_sizes 
        ~ "\n Valid sizes are: " ~ available_sizes, 
        level='ERROR'
        ) }}
        {{ return(false) }}
    {% endif %}

    {# Strict incremental and fullrefresh size validation #}
    {% set invalid_sizes = [] %}
    {% for size, label in [(incremental, 'incremental'), (fullrefresh, 'fullrefresh')] %}
        {% if size not in configured_sizes %}
            {% do invalid_sizes.append(label ~ ': ' ~ size) %}
        {% endif %}
    {% endfor %}

    {% if invalid_sizes %}
        {{ dbt_macro_polo.logging(message="Configuration Error: Invalid warehouse size(s): " ~ invalid_sizes
        ~ "\n Valid sizes are: " ~ configured_sizes, 
        level='ERROR', 
        model_id=model_id
        ) }}
        {{ return(false) }}
    {% endif %}

    {# Get and validate environment configuration #}
    {% set environments = warehouse_config.get('environment', {}) %}
    {% if not environments %}
        {{ dbt_macro_polo.logging(message="Configuration Error: Environment configuration is missing in warehouse_config.environment."  
        ~ "\nRequired structure:"
        ~ "\n  warehouse_config:"
        ~ "\n    warehouse_size: [ 'xs', 's', 'm' ]"
        ~ "\n    environment:"
        ~ "\n      prod:"
        ~ "\n        warehouse_name_prefix: 'PROD'"
        ~ "\n      dev:"
        ~ "\n        warehouse_name_prefix: 'DEV'",
        level='ERROR'
        ) }}
        {{ return(false) }}
    {% endif %}

    {% set env_config = environments.get(target.name, {}) %}
    {% set warehouse_prefix = env_config.get('warehouse_name_prefix') %}
    
    {% if not env_config %}
        {{ dbt_macro_polo.logging(message="Configuration Error (dbt_project.yml): Invalid environment configuration for target environment: " ~ target.name 
        ~ "\n Got environments: " ~ environments.keys() | list, 
        level='ERROR'
        ) }}
        {{ return(false) }}
    {% endif %}

    {% if not warehouse_prefix %}
        {{ dbt_macro_polo.logging(message="Configuration Error (dbt_project.yml): warehouse_name_prefix is not specified"
        ~ " in environment configuration for target environment: " ~ target.name, 
        level='ERROR'
        ) }}
        {{ return(false) }}
    {% endif %}

    {# Generate and validate warehouse identifier #}
    {% set warehouse_id = warehouse_prefix ~ "_" ~ size_suffix %}
    {% if warehouse_id | length > 255 %}
        {{ dbt_macro_polo.logging(message="Configuration Error (dbt_project.yml): Generated warehouse_id exceeds maximum length: '" 
        ~ warehouse_id ~ "'", 
        level='ERROR'
        ) }}
        {{ return(false) }}
    {% endif %}

    {# Cache and return result #}
    {{ dbt_macro_polo.logging(macro_name, message="Caching warehouse '" ~ warehouse_id ~ "' with cache key '" ~ cache_key ~ "'", level='DEBUG') }}
    {% do macro_polo.cache.update({cache_key: warehouse_id}) %}
    {{ dbt_macro_polo.logging(message="Allocated warehouse", model_id=model_id, status=warehouse_id | upper) }}
    {{ return(warehouse_id) }}
    
{% endmacro %}

