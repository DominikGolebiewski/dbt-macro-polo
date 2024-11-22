--------------------------------------------------------------------------------

{% macro get_warehouse(incremental_size, fullrefresh_size=none) -%}
    {% if not execute %}
        {# During parsing/compilation, just return the warehouse name #}
        {% set config = var('warehouse_config', {}) %}
        {% set validated_config = dbt_macro_polo.validate_warehouse_config(config) %}
        {% set available_sizes = config.get('warehouse_size', dbt_macro_polo.get_default_warehouse_sizes()) %}
        {% set sizes = dbt_macro_polo.validate_input_parameters(incremental_size, fullrefresh_size) %}
        {% set size = sizes.fullrefresh if flags.FULL_REFRESH else sizes.incremental %}
        {% set validated_size = dbt_macro_polo.validate_warehouse_size(size, available_sizes) %}
        {% set warehouse_prefix = dbt_macro_polo.resolve_environment(target.name, validated_config) %}
        {{ return(warehouse_prefix ~ "_" ~ validated_size) }}
    {% else %}
        {# During execution, use the adapter dispatch #}
        {{ return(adapter.dispatch('get_warehouse', 'dbt_macro_polo')(incremental_size, fullrefresh_size)) }}
    {% endif %}
{%- endmacro %}

--------------------------------------------------------------------------------

{# Main Warehouse Selection Macro #}
{% macro snowflake__get_warehouse(incremental_size, fullrefresh_size=none) %}
    {# Macro Polo coordinates the warehouse selection journey #}
    {% set macro_name = 'POLO_GETS_WAREHOUSE' %}
    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo begins his warehouse exploration", {
        'incremental_size': incremental_size,
        'fullrefresh_size': fullrefresh_size,
        'is_full_refresh': flags.FULL_REFRESH
    }) }}
    
    {# Get and validate configuration #}
    {% set config = var('warehouse_config', {}) %}
    {% set validated_config = dbt_macro_polo.validate_warehouse_config(config) %}
    {% set available_sizes = config.get('warehouse_size', dbt_macro_polo.get_default_warehouse_sizes()) %}
    
    {# Validate input parameters #}
    {% set sizes = dbt_macro_polo.validate_input_parameters(incremental_size, fullrefresh_size) %}
    
    {# Select appropriate size based on run type #}
    {% set size = sizes.fullrefresh if flags.FULL_REFRESH else sizes.incremental %}
    {% set validated_size = dbt_macro_polo.validate_warehouse_size(size, available_sizes) %}
    
    {# Get warehouse prefix for environment #}
    {% set warehouse_prefix = dbt_macro_polo.resolve_environment(target.name, validated_config) %}
    {% set warehouse_name = warehouse_prefix ~ "_" ~ validated_size %}
    
    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo has discovered the perfect warehouse", warehouse_name) }}
    {{ return(warehouse_name) }}
{% endmacro %}