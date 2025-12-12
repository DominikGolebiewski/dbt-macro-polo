{#-
    provision_compute.sql
    
    Dynamically provisions a Snowflake warehouse based on run mode and configuration.
    
    Design Principles:
    - Minimal API: Two parameters, one required
    - Smart Defaults: fullrefresh defaults to incremental size
    - Memoised: Caches results for performance
    - Observable: Structured logging for debugging
    
    Usage:
        {{ dbt_macro_polo.provision_compute('xs', 'l') }}
        {{ dbt_macro_polo.provision_compute('m') }}
-#}

{% macro provision_compute(incremental_size, fullrefresh_size=none) %}
    {{ return(adapter.dispatch('provision_compute', 'dbt_macro_polo')(incremental_size, fullrefresh_size)) }}
{% endmacro %}


{% macro snowflake__provision_compute(incremental_size, fullrefresh_size=none) %}

    {#-- Validate required parameter --#}
    {% if not incremental_size %}
        {{ dbt_macro_polo.log_event(
            message="incremental_size parameter is required",
            level='ERROR',
            macro_name='provision_compute'
        ) }}
    {% endif %}

    {#-- Normalise sizes --#}
    {% set inc_size = incremental_size | trim | lower %}
    {% set fr_size = (fullrefresh_size | trim | lower) if fullrefresh_size is not none else inc_size %}

    {#-- Determine effective size based on run mode --#}
    {#-- Only check full refresh if sizes differ (optimisation) --#}
    {% set effective_size = inc_size %}
    {% if fr_size != inc_size and dbt_macro_polo.should_full_refresh() %}
        {% set effective_size = fr_size %}
    {% endif %}

    {#-- Check cache --#}
    {% set cache_key = '_provision_compute_' ~ effective_size %}
    {% set cached = dbt_macro_polo.get_runtime_state(cache_key) %}
    
    {% if cached %}
        {{ return(cached) }}
    {% endif %}

    {#-- Resolve warehouse (validates config + constructs ID) --#}
    {% set warehouse_id = dbt_macro_polo._resolve_warehouse(effective_size) %}

    {#-- Cache result --#}
    {% do dbt_macro_polo.set_runtime_state(cache_key, warehouse_id) %}

    {#-- Log and return --#}
    {{ dbt_macro_polo.log_event(
        message="Provisioned warehouse",
        model_id=this,
        status=warehouse_id | upper,
        macro_name='provision_compute'
    ) }}

    {{ return(warehouse_id) }}

{% endmacro %}
