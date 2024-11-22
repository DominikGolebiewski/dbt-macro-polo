--------------------------------------------------------------------------------

{% macro log_debug(macro_name, message, context_info=none) -%}
  {{ return(adapter.dispatch('log_debug', 'dbt_macro_polo')(macro_name, message, context_info)) }}
{%- endmacro %}

--------------------------------------------------------------------------------

{# General debug logging macro #}
{% macro default__log_debug(macro_name, message, context_info=none) %}
    {% if execute and (var('global_debug_mode', false)) %}
        {% set log_message = macro_name ~ ": " ~ message %}
        {% if context_info is not none %}
            {% set log_message = log_message ~ " | Context: " ~ context_info %}
        {% endif %}
        {{ log(log_message, info=true) }}
    {% endif %}
{% endmacro %}