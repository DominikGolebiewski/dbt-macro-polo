--------------------------------------------------------------------------------

{% macro get_warehouse(incremental_size, fullrefresh_size=none) -%}
  {{ return(adapter.dispatch('get_warehouse', 'dbt_macro_polo')(incremental_size, fullrefresh_size)) }}
{%- endmacro %}

--------------------------------------------------------------------------------

{# Main Warehouse Selection Macro #}
{% macro snowflake__get_warehouse(incremental_size, fullrefresh_size=none) %}
    {# Macro Polo coordinates the warehouse selection journey #}
    {% set macro_name = 'MACRO_POLO_GET_WAREHOUSE' %}
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

    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo has validated the input parameters", sizes) }}
    
    {# Get materialization type #}
    {% set materialization = model.config.get('materialized') %}

    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo has retrieved the materialization type", materialization) }}

    {# For views and ephemeral, use incremental size as they dont store data #}
    {% if materialization in ['view', 'ephemeral'] %}
        {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo has determined to use the incremental size for views and ephemeral", sizes.incremental) }}
        {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo has determined to check if full refresh or first run", {
            'flags.FULL_REFRESH': flags.FULL_REFRESH,
            'adapter.get_relation(this.database, this.schema, this.table)': adapter.get_relation(this.database, this.schema, this.table)
        }) }}
        {% set size = sizes.incremental %}
    {# For tables and incremental models, check if full refresh or first run #}
    {% elif (flags.FULL_REFRESH or not adapter.get_relation(this.database, this.schema, this.table)) %}

        {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo has determined to use the full refresh size for tables and incremental models", sizes.fullrefresh) }}
        {% set size = sizes.fullrefresh %}
    {% elif materialization in ['table', 'incremental'] %}
        {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo has determined to use the incremental size for tables and incremental models", sizes.incremental) }}
        {% set size = sizes.incremental %}
    {% else %}
        {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo has determined to use the target warehouse size", target.warehouse) }}
        {% set size = target.warehouse %}
    {% endif %}
    
    {% set validated_size = dbt_macro_polo.validate_warehouse_size(size, available_sizes) %}
    
    {# Get warehouse prefix for environment #}
    {% set warehouse_prefix = dbt_macro_polo.resolve_environment(target.name, validated_config) %}
    {% set warehouse_name = warehouse_prefix ~ "_" ~ validated_size %}
    
    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo has discovered the perfect warehouse", warehouse_name) }}
    {{ return(warehouse_name) }}
{% endmacro %}