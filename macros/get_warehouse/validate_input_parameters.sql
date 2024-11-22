--------------------------------------------------------------------------------

{% macro validate_input_parameters(target_name, config) -%}
  {{ return(adapter.dispatch('validate_input_parameters', 'dbt_macro_polo')(target_name, config)) }}
{%- endmacro %}

--------------------------------------------------------------------------------

{# Input Validation - Macro Polo validates the input parameters #}
{% macro snowflake__validate_input_parameters(incremental_size, fullrefresh_size) %}
    {# Macro Polo checks if the warehouse sizes are provided correctly #}
    {% set macro_name = 'POLO_VALIDATES_INPUT_PARAMETERS' %}
    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo examines the input parameters", {
        'incremental_size': incremental_size,
        'fullrefresh_size': fullrefresh_size
    }) }}

    {% if not incremental_size %}
        {% set error_msg = "Macro Polo needs an incremental_size to proceed!" %}
        {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo found an error", error_msg) }}
        {{ exceptions.raise_compiler_error(error_msg) }}
    {% endif %}
    
    {% set result = {
        "incremental": incremental_size | trim | lower,
        "fullrefresh": fullrefresh_size | trim | lower if fullrefresh_size is not none else none
    } %}
    
    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo confirms the parameters are valid", result) }}
    {{ return(result) }}
{% endmacro %}