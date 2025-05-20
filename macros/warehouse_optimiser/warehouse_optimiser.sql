{% macro warehouse_optimiser(query_operation='ctas') %}
    {{ return(adapter.dispatch('warehouse_optimiser', 'dbt_macro_polo')(query_operation)) }}
{% endmacro %}

{% macro default__warehouse_optimiser(query_operation='ctas') %}

    {# Initialise macro context #}
    {% set macro_ctx = dbt_macro_polo.create_macro_context('warehouse_optimiser') %}
    {% set model_id = macro_ctx.model_id %}
    {% set timestamp_column = model.config.get('timestamp_column', 'loaded_timestamp') %}
    {% set macro_polo = var('macro_polo', {}) %}
    {% set is_incremental = model.config.get('materialized', 'undefined') == 'incremental' %}
    {% set is_delete_insert = model.config.get('incremental_strategy', 'undefined') == 'delete+insert' %}
    
    {# Validate query operation #}
    {% if query_operation not in ['ctas', 'insert', 'delete'] %}
        {{ dbt_macro_polo.logging(message="Invalid query operation. Expected: 'ctas', 'insert', or 'delete'. Received: " ~ query_operation, 
            level='ERROR', model_id=model_id) }}
        {{ return('') }}
    {% endif %}

    {% if not (is_incremental and is_delete_insert) %}
        {{ dbt_macro_polo.logging(message="Warehouse Optimiser is only supported for incremental models with delete+insert strategy."
            ~ "\n\n Expected: \n   materialized: incremental \n   incremental_strategy: delete+insert"
            ~ "\n\n Received: \n   materialized: " ~ model.config.get('materialized', 'undefined') 
            ~ "\n   incremental_strategy: " ~ model.config.get('incremental_strategy', 'undefined'),
            model_id="\n\n In model: " ~ model_id, 
            level='ERROR'
        ) }}
        {{ return(false) }}
    {% endif %}

    {# Get and validate configurations #}
    {% set project_config = macro_polo.get('warehouse_optimiser', {}) %}
    {% set model_config = model.config.get('meta', {}).get('warehouse_optimiser', {}) %}
    
    {# Early returns if optimiser is disabled #}
    {% if not (project_config and project_config.get('enabled', false)) %}
        {% if query_operation == 'ctas' %}
            {{ dbt_macro_polo.logging(message="Warehouse Optimiser is globally disabled or not configured", level='WARN') }}
        {% endif %}
        {{ return('') }}
    {% endif %}

    {% if not (model_config and model_config.get('enabled', false)) %}
        {% if query_operation == 'ctas' %}
            {{ dbt_macro_polo.logging(message="Warehouse Optimiser is disabled for this model", model_id=model_id, level='DEBUG') }}
        {% endif %}
        {{ return('') }}
    {% endif %}

    {# Get operation configurations #}
    {% set operation_type = model_config.get('operation_type', {}) %}
    {% set on_full_refresh_config = operation_type.get('on_full_refresh', {}) %}
    {% set on_run_config = operation_type.get('on_run', {}) %}
    {% set on_dry_run_config = operation_type.get('on_dry_run', {}) %}
    {% set has_on_dry_run_config = on_dry_run_config is mapping and on_dry_run_config | length > 0 %}
    {% set is_full_refresh = dbt_macro_polo.should_full_refresh() %}

    {{ dbt_macro_polo.logging(message="Is full refresh: " ~ is_full_refresh, model_id=model_id, level='DEBUG') }}

    {% set active_config = on_run_config if not is_full_refresh else on_full_refresh_config %}

    {{ dbt_macro_polo.logging(message="Active config: " ~ active_config, model_id=model_id, level='DEBUG') }}

    {% if not on_run_config %}
        {% if query_operation == 'ctas' %}
            {{ dbt_macro_polo.logging(message="No on_run_config found. Target warehouse size will be used for incremental runs.", level='WARN', model_id=model_id) }}
        {% endif %}
        {{ return('') }}
    {% endif %}

    {% if not on_full_refresh_config %}
        {% if query_operation == 'ctas' %}
            {{ dbt_macro_polo.logging(message="Macro Polo: No full refresh config found. Using default warehouse size.", level='WARN', model_id=model_id) }}
        {% endif %}
    {% endif %}

    {% if not has_on_dry_run_config %}
        {% if query_operation == 'ctas' %}
            {{ dbt_macro_polo.logging(message="Macro Polo: No on_dry_run_config found. No warehouse switch will be performed.", model_id=model_id, level='WARN') }}
        {% endif %}
    {% endif %}

    {# Get upstream dependency config - handle v1 and v2 compatibility #}
    {% set upstream_dependency = on_dry_run_config.get('upstream_dependency', []) %}

    {{ dbt_macro_polo.logging(message="Macro Polo: Starting Warehouse Optimiser", model_id=model_id, status=query_operation | upper) }}
 
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