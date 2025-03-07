{% macro warehouse_optimiser(query_operation='ctas') %}
    {{ return(adapter.dispatch('warehouse_optimiser', 'dbt_macro_polo')(query_operation)) }}
{% endmacro %}

{% macro default__warehouse_optimiser(query_operation='ctas') %}
    {# Validate query operation #}
    {% if query_operation not in ['ctas', 'insert', 'delete'] %}
        {{ log("Invalid query operation. Expected: 'ctas', 'insert', or 'delete'. Received: " ~ query_operation, info=true) }}
        {{ return('') }}
    {% endif %}

    {# Get model configuration #}
    {% set model_config = model.config.get('meta', {}).get('warehouse_optimiser', {}) %}
    {% set project_config = var('macro_polo', {}).get('warehouse_optimiser', {}) %}
    {% set is_incremental = model.config.get('materialized', '') == 'incremental' %}
    {% set is_delete_insert = model.config.get('incremental_strategy', '') == 'delete+insert' %}
    
    {# Check if model is compatible #}
    {% if not (is_incremental and is_delete_insert) %}
        {{ log("Warehouse Optimiser requires incremental models with delete+insert strategy.", info=true) }}
        {{ return('') }}
    {% endif %}

    {# Check if optimiser is enabled #}
    {% if not (project_config.get('enabled', false) and model_config.get('enabled', false)) %}
        {% if query_operation == 'ctas' %}
            {{ log("Warehouse Optimiser is " ~ 
                ("globally disabled" if not project_config.get('enabled', false) else "disabled for this model"), 
                info=true) }}
        {% endif %}
        {{ return('') }}
    {% endif %}

    {# Get operation configurations #}
    {% set is_full_refresh = dbt_macro_polo.should_full_refresh() %}
    {% set operation_type = model_config.get('operation_type', {}) %}
    {% set active_config = operation_type.get('full_refresh' if is_full_refresh else 'incremental', {}) %}
    {% set on_dry_run_config = operation_type.get('dry_run', {}) %}
    {% set has_on_dry_run_config = on_dry_run_config is mapping and on_dry_run_config | length > 0 %}
    
    {{ log("Starting Warehouse Optimiser for " ~ query_operation, info=true) }}
 
    {% if execute %}
        {# Get row count for CTAS operations #}
        {% set timestamp_column = model.config.get('timestamp_column', 'loaded_timestamp') %}
        {% set row_count = var('macro_polo', {}).get('cache', {}).get('_upstream_row_count_' ~ this.identifier, 0) %}
        
        {% if query_operation == 'ctas' and not is_full_refresh and has_on_dry_run_config %}
            {% set row_count = dbt_macro_polo.get_upstream_row_count(on_dry_run_config.get('upstream_dependency', []), timestamp_column) %}
        {% endif %}
    
        {# Determine and allocate warehouse #}
        {% set warehouse_size = dbt_macro_polo.handle_operation(query_operation, active_config, has_on_dry_run_config, row_count) %}
        {% set warehouse = dbt_macro_polo.allocate_warehouse(warehouse_size) %}

        {{ log("Final warehouse selection for " ~ query_operation ~ ": " ~ warehouse, info=true) }}
        {{ return('use warehouse ' ~ warehouse) }}
    {% endif %}
{% endmacro %}

{% macro handle_warehouse_switch(operation_type) %}
    {{ return(adapter.dispatch('handle_warehouse_switch', 'dbt_macro_polo')(operation_type)) }}
{% endmacro %}

{% macro default__handle_warehouse_switch(operation_type) %}
    {% set warehouse_stmt = dbt_macro_polo.warehouse_optimiser(operation_type) %}
    {% if warehouse_stmt %}
        {{ warehouse_stmt }};
    {% endif %}
{% endmacro %}