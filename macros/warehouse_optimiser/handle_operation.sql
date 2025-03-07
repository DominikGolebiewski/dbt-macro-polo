{% macro handle_operation(query_operation, active_config, has_on_dry_run_config, row_count) %}
    {{ return(adapter.dispatch('handle_operation', 'dbt_macro_polo')(query_operation, active_config, has_on_dry_run_config, row_count)) }}
{% endmacro %}

{% macro default__handle_operation(query_operation, active_config, has_on_dry_run_config, row_count) %}
    {# Get default warehouse size from variables #}
    {% set default_warehouse_size = var('macro_polo', {}).get('warehouse_optimiser', {}).get('default_warehouse_size', 'xs') %}
    {% set is_full_refresh = dbt_macro_polo.should_full_refresh() %}

    {# Get operation configuration #}
    {% set operation_config = active_config.get(query_operation, {}) %}
    
    {# For full refresh, use the configured warehouse size directly #}
    {% if is_full_refresh %}
        {% set warehouse_size = active_config.get('warehouse_size', default_warehouse_size) %}
        {{ log("Full refresh detected, using warehouse size: " ~ warehouse_size, info=true) }}
        {{ return(warehouse_size) }}
    {% endif %}

    {# Handle zero row count case when on_dry_run is configured #}
    {% if has_on_dry_run_config and row_count == 0 %}
        {{ log("Total row count is 0, using XS warehouse size", info=true) }}
        {{ return('xs') }}
    {% endif %}

    {# Handle scheduling if configured #}
    {% if operation_config and operation_config is mapping %}
        {% set current_time = modules.datetime.datetime.now() %}
        {{ return(dbt_macro_polo.handle_scheduling(operation_config, row_count, has_on_dry_run_config, current_time, default_warehouse_size)) }}
    {% endif %}

    {# Return default size if no special handling needed #}
    {{ log("Using default warehouse size: " ~ default_warehouse_size, info=true) }}
    {{ return(default_warehouse_size) }}
{% endmacro %}