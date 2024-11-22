--------------------------------------------------------------------------------

{% macro resolve_environment(target_name, config) -%}
  {{ return(adapter.dispatch('resolve_environment', 'dbt_macro_polo')(target_name, config)) }}
{%- endmacro %}

--------------------------------------------------------------------------------

{# Environment Resolution - Macro Polo finds the right environment #}
{% macro snowflake__resolve_environment(target_name, config) %}
    {# Macro Polo maps the target name to the correct environment #}
    {% set macro_name = 'MACRO_POLO_RESOLVE_ENVIRONMENT' %}
    {% set env_mapping = config.get('environments', {}) %}
    
    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo is exploring environments", {
        'target_name': target_name | string,
        'available_environments': env_mapping
    }) }}
    
    {% for env_key, env_config in env_mapping.items() %}
        {% if env_config.get('target_name') == target_name %}
            {% set prefix = env_config.get('warehouse_name_prefix') %}
            {% if not prefix %}
                {% set error_msg = "Macro Polo cannot find warehouse_name_prefix for environment: " ~ env_key %}
                {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo found an error", error_msg) }}
                {{ exceptions.raise_compiler_error(error_msg) }}
            {% endif %}
            
            {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo discovered the environment", {
                'environment': env_key,
                'warehouse_prefix': prefix
            }) }}
            {{ return(prefix) }}
        {% endif %}
    {% endfor %}
    
    {# Create helpful error message #}
    {% set available = {} %}
    {% for env_key, env_config in env_mapping.items() %}
        {% do available.update({env_key: env_config.get('target_name', '')}) %}
    {% endfor %}
    
    {% set error_msg = "Macro Polo cannot find environment for target: '" ~ target_name | string ~ 
        "'. Available targets: " ~ available | tojson %}
    {{ dbt_macro_polo.log_debug(macro_name, "Macro Polo is lost in his travels", error_msg) }}
    {{ exceptions.raise_compiler_error(error_msg) }}
{% endmacro %}