--------------------------------------------------------------------------------

{% macro get_default_warehouse_sizes() -%}
  {{ return(adapter.dispatch('get_default_warehouse_sizes', 'dbt_macro_polo')()) }}
{%- endmacro %}

--------------------------------------------------------------------------------

{# Constants - Macro Polo knows the standard warehouse sizes #}
{% macro snowflake__get_default_warehouse_sizes() %}
    {# Macro Polo suggests warehouse sizes from his travels #}
    {% set macro_name = 'MACRO_POLO_GET_DEFAULT_WAREHOUSE_SIZES' %}
    {% set sizes = ['xs', 's', 'm', 'l', 'xl', '2xl'] %}
    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo suggests these warehouse sizes", sizes) }}
    {{ return(sizes) }}
{% endmacro %}