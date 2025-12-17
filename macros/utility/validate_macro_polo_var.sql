{% macro validate_macro_polo_var() %}
  {{ return(adapter.dispatch('validate_macro_polo_var', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default__validate_macro_polo_var() %}

    {% set macro_name = 'validate_macro_polo_var' %}
    {% set macro_polo = dbt_macro_polo.require(var('macro_polo', none), "macro_polo must be defined in project variable. Macro Polo is disabled.", macro_name) %}
    {{ return(macro_polo or {}) }}

{% endmacro %}