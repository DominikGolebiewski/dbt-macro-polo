--------------------------------------------------------------------------------

{% macro validate_warehouse_config(config) -%}
  {{ return(adapter.dispatch('validate_warehouse_config', 'dbt_macro_polo')(config)) }}
{%- endmacro %}

--------------------------------------------------------------------------------

{# Config Validation - Macro Polo validates the warehouse configuration #}
{% macro snowflake__validate_warehouse_config(config) %}
    {# Macro Polo ensures the warehouse configuration is complete #}
    {% set macro_name = 'MACRO_POLO_VALIDATES_WAREHOUSE_CONFIG' %}
    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo reviews the warehouse configuration", config) }}

    {% if not config %}
        {% set error_msg = "Macro Polo cannot find the warehouse_config!" %}
        {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo found an error", error_msg) }}
        {{ exceptions.raise_compiler_error(error_msg) }}
    {% endif %}

    {# Check required top-level keys #}
    {% set required_keys = ['environments', 'warehouse_size'] %}
    {% for key in required_keys %}
        {% if not config.get(key) %}
            {% set error_msg = "Macro Polo needs the '" ~ key ~ "' in warehouse_config!" %}
            {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo found an error", error_msg) }}
            {{ exceptions.raise_compiler_error(error_msg) }}
        {% endif %}
    {% endfor %}

    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo confirms the configuration is valid", config) }}
    {{ return(config) }}
{% endmacro %}