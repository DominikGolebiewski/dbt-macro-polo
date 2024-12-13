{# General debug logging macro with log levels #}
{% macro logging(macro_name=none, message=none, level='INFO', model_id=none, status=none) %}
    {% if execute and (var('global_debug_mode', false)) %}

        {% set git_link = "Visit https://github.com/DominikGolebiewski/dbt-macro-polo?tab=readme-ov-file for more information" %}

        {% set level = level | upper %}
        {% set global_level = var('logging_level', 'INFO') | upper %}
        
        {% set level_hierarchy = {
            'DEBUG': 0,
            'INFO': 1,
            'WARN': 2,
            'ERROR': 3
        } %}
        
        {% set message_level_value = level_hierarchy.get(level, 1) %}
        {% set global_level_value = level_hierarchy.get(global_level, 1) %}
        
        {% if message_level_value >= global_level_value %}
            {# Status indicators and colors #}
            {% set status_by_level = {
                'DEBUG': 'DEBUG',
                'INFO': 'SUCCESS',
                'WARN': 'WARNING',
                'ERROR': 'ERROR'
            } %}

            {# ANSI colour codes matching dbt #}
            {% set color_codes = {
                'DEBUG': '\033[90m',       
                'INFO': '\033[96m',     
                'WARN': '\033[33m',    
                'ERROR': '\033[31m'        
            } %}
            
            {% set light_cyan = '\033[96m' %}
            {% set reset_code = '\033[0m' %}
            
            {# Get status and color #}
            {% set status = status if status is not none else status_by_level.get(level, 'INFO') %}
            {% set color_code = color_codes.get(status, color_codes['INFO']) %}

            {# Format sequence and message #}
            {% set sequence_display = "      " %}
            {% set macro_name = macro_name ~ " • " if macro_name is not none else "" %}
            {% set model_id = " • " ~ model_id if model_id is not none else "" %}
            
            {# Modify message formatting based on level #}
            {% if level == 'WARN' %}
                {% set base_message = color_code ~ macro_name ~ message ~ model_id %}
            {% elif level in ['ERROR', 'DEBUG'] %}
                {% set base_message = color_code ~ macro_name ~ message ~ model_id ~ reset_code %}
            {% else %}
                {% set base_message = color_code ~ macro_name ~ reset_code ~ message ~ model_id %}
            {% endif %}
            
            {# Calculate dots for alignment #}
            {% set total_width = 108 %}
            {% set dots_count = total_width - (base_message | length) - 20 %}
            {% set dots = "." * dots_count if dots_count > 0 else "" %}
            
            {# Construct final message #}
            {% if level in ['ERROR', 'DEBUG'] %}
                {% set log_message = base_message %}
            {% elif level == 'WARN' %}
                {% set log_message = base_message ~ " " ~ dots ~ " " ~ "[" ~ status ~ "]" ~ reset_code %}
            {% else %}
                {% set log_message = base_message ~ " " ~ dots ~ " " ~ "[" ~ color_code ~ status ~ reset_code ~ "]" %}
            {% endif %}
            
            {% if level == 'DEBUG' %}
                {{ log(log_message, info=true) }}
            {% elif level == 'INFO' %}
                {{ log(log_message, info=true) }}
            {% elif level == 'WARN' %}
                {{ exceptions.warn(log_message) }}
            {% elif level == 'ERROR' %}
                {{ exceptions.raise_compiler_error(log_message) }}
            {% else %}
                {{ log(log_message, info=true) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
