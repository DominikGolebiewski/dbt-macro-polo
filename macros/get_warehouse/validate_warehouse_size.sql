--------------------------------------------------------------------------------

{% macro validate_warehouse_size(size, available_sizes) -%}
  {{ return(adapter.dispatch('validate_warehouse_size', 'dbt_macro_polo')(size, available_sizes)) }}
{%- endmacro %}

--------------------------------------------------------------------------------

{# Size Validation - Macro Polo validates the warehouse size #}
{% macro snowflake__validate_warehouse_size(size, available_sizes) %}
    {# Macro Polo ensures the warehouse size is valid #}
    {% set macro_name = 'MACRO_POLO_VALIDATES_WAREHOUSE_SIZE' %}
    {% set normalized_size = size | trim | lower %}
    
    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo checks the warehouse size", {
        'requested_size': size,
        'normalized_size': normalized_size,
        'available_sizes': available_sizes
    }) }}
    
    {% if normalized_size not in available_sizes %}
        {% set error_msg = "Macro Polo found invalid size: '" ~ size ~ "'. Available: " ~ available_sizes | join(", ") %}
        {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo found an invalid size", error_msg) }}
        {{ exceptions.raise_compiler_error(error_msg) }}
    {% endif %}
    
    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo confirms the size is valid", normalized_size) }}
    {{ return(normalized_size) }}
{% endmacro %}