{% macro validate_macro_polo_var() %}
  {{ return(adapter.dispatch('validate_macro_polo_var', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default__validate_macro_polo_var() %}

    {% set macro_name = 'validate_macro_polo_var' %}
    {% set macro_polo = var('macro_polo', {}) %}

    {% if macro_polo == {} %}
        {% set msg = "Configuration Warning (dbt_project.yml): macro_polo must be defined in project variable. Macro Polo is disabled." %}
        {{ dbt_macro_polo.log_event(message=msg, level='WARN', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}

    {{ return(macro_polo) }}

{% endmacro %}