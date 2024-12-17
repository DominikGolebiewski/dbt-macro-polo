{% macro handle_operation(model_id, query_operation, active_config, has_on_dry_run_config, row_count) %}
    {{ return(adapter.dispatch('handle_operation', 'dbt_macro_polo')(model_id, query_operation, active_config, has_on_dry_run_config, row_count)) }}
{% endmacro %}

{% macro default__handle_operation(model_id, query_operation, active_config, has_on_dry_run_config, row_count) %}

    {# Initialise macro context #}
    {% set macro_ctx = dbt_macro_polo.create_macro_context('handle_operation') %}
    {% set macro_name = macro_ctx.macro_name %}
    {% set model_id = macro_ctx.model_id %}
    {% set macro_polo = var('macro_polo', {}) %}
    {% set is_full_refresh = dbt_macro_polo.should_full_refresh() %}

    {# Get default warehouse size from variables #}
    {% set default_warehouse_size = macro_polo.get('warehouse_optimiser', {}).get('default_warehouse_size', 'xs') %}

    {# Get operation configuration #}
    {% set operation_config = active_config.get(query_operation, {}) %}
    {% set warehouse_size = active_config.get('warehouse_size', default_warehouse_size) %}

    {{ dbt_macro_polo.logging(macro_name, "Processing operation: " ~ query_operation ~ " with config: " ~ active_config, level='DEBUG') }}

    {% if is_full_refresh %}
        {{ dbt_macro_polo.logging(message="Full refresh detected, using warehouse size", model_id=model_id, status=warehouse_size | upper) }}
        {{ return(warehouse_size) }}
    {% endif %}

    {# Handle zero row count case when on_dry_run is configured #}
    {% if has_on_dry_run_config and row_count == 0 %}
        {{ dbt_macro_polo.logging(message="Total row count is 0, using XS warehouse size", model_id=model_id, status='XS') }}
        {{ return('xs') }}
    {%- endif -%}

    {# Handle scheduling if configured #}
    {%- if operation_config and operation_config is mapping -%}
        {{ return(dbt_macro_polo.handle_scheduling(operation_config, row_count, has_on_dry_run_config, default_warehouse_size)) }}
    {%- endif -%}

    {# Return default size if no special handling needed #}
    {{ dbt_macro_polo.logging(message="Using default warehouse size", model_id=model_id, status=default_warehouse_size | upper) }}
    {{ return(default_warehouse_size) }}

{%- endmacro -%}