{% macro warehouse_optimiser(query_operation='ctas') %}
    {{ return(adapter.dispatch('warehouse_optimiser', 'dbt_macro_polo')(query_operation)) }}
{% endmacro %}

{% macro default__warehouse_optimiser(query_operation='ctas') %}

    {% set model_id = this.schema | lower ~ '.' ~ this.name | lower %}
    {% set timestamp_column = model.config.get('timestamp_column', 'loaded_timestamp') %}
    {% set macro_polo = var('macro_polo', {}) %}
    {% set is_incremental = model.config.get('materialized', 'undefined') == 'incremental' %}
    {% set is_delete_insert = model.config.get('incremental_strategy', 'undefinded') == 'delete+insert' %}
    
    {# Validate query operation #}
    {% if query_operation not in ['ctas', 'insert', 'delete'] %}
        {{ dbt_macro_polo.logging(message="Invalid query operation. Expected: 'ctas', 'insert', or 'delete'. Received: " ~ query_operation, 
            level='ERROR', model_id=model_id) }}
        {{ return('') }}
    {% endif %}

    {% if not (is_incremental and is_delete_insert) %}
        {{ dbt_macro_polo.logging(message="Warehouse Optimiser requires incremental models with delete+insert strategy.", 
            model_id=model_id, level='ERROR') }}
        {{ return(false) }}
    {% endif %}

    {# Get and validate configurations #}
    {% set project_config = macro_polo.get('warehouse_optimiser', {}) %}
    {% set model_config = model.config.get('meta', {}).get('warehouse_optimiser', {}) %}
    
    {# Check if optimiser is enabled #}
    {% if not (project_config.get('enabled', false) and model_config.get('enabled', false)) %}
        {% if query_operation == 'ctas' %}
            {{ dbt_macro_polo.logging(
                message="Warehouse Optimiser is " ~ 
                ("globally disabled" if not project_config.get('enabled', false) else "disabled for this model"), 
                model_id=model_id, 
                level=('WARN' if not project_config.get('enabled', false) else 'DEBUG')
            ) }}
        {% endif %}
        {{ return('') }}
    {% endif %}

    {# Get operation configurations #}
    {% set is_full_refresh = dbt_macro_polo.should_full_refresh() %}
    {% set operation_type = model_config.get('operation_type', {}) %}
    {% set active_config = operation_type.get('on_full_refresh' if is_full_refresh else 'on_run', {}) %}

    {% set on_dry_run_config = operation_type.get('on_dry_run', {}) %}
    {% set has_on_dry_run_config = on_dry_run_config is mapping and on_dry_run_config | length > 0 %}
    

    {{ dbt_macro_polo.logging(message="Is full refresh: " ~ is_full_refresh ~ ", Active config: " ~ active_config, 
        model_id=model_id, level='DEBUG') }}

    {# Check for missing configurations #}
    {% if query_operation == 'ctas' %}
        {% if not operation_type.get('on_run', {}) %}
            {{ dbt_macro_polo.logging(message="No on_run_config found. Target warehouse size will be used for incremental runs.", 
                level='WARN', model_id=model_id) }}
        {% endif %}
        
        {% if not operation_type.get('on_full_refresh', {}) %}
            {{ dbt_macro_polo.logging(message="No full refresh config found. Using default warehouse size.", 
                level='WARN', model_id=model_id) }}
        {% endif %}
        
        {% if not has_on_dry_run_config %}
            {{ dbt_macro_polo.logging(message="No on_dry_run_config found. No warehouse switch will be performed.", 
                model_id=model_id, level='WARN') }}
        {% endif %}
    {% endif %}

    {{ dbt_macro_polo.logging(message="Starting Warehouse Optimiser", model_id=model_id, status=query_operation | upper) }}
 
    {% if execute %}
        {# Get row count for CTAS operations #}
        {% set row_count = macro_polo.get('cache', {}).get('_upstream_row_count_' ~ model_id | replace('.', '_'), 0) %}
        {% if query_operation == 'ctas' and not is_full_refresh and has_on_dry_run_config %}
            {% set row_count = dbt_macro_polo.get_upstream_row_count(model_id, upstream_dependency, timestamp_column) %}
        {% endif %}
    
        {# Determine and allocate warehouse #}
        {% set warehouse_size = dbt_macro_polo.handle_operation(model_id, query_operation, active_config, has_on_dry_run_config, row_count) %}
        {% set warehouse = dbt_macro_polo.allocate_warehouse(warehouse_size) %}

        {{ dbt_macro_polo.logging(message="Final warehouse selection for " ~ query_operation | upper, model_id=model_id, status=warehouse | upper) }}
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