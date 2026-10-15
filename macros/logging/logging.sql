{% macro logging(macro_name=none, message=none, level='INFO', model_id=none, status=none) %}
    {{ return(adapter.dispatch('logging', 'dbt_macro_polo')(macro_name, message, level, model_id, status)) }}
{% endmacro %}

{% macro default__logging(macro_name=none, message=none, level='INFO', model_id=none, status=none) %}
    {% if execute %}
        {% set macro_polo = var('macro_polo', {}) %}
        {% set level = level | upper %}
        {% set global_level = macro_polo.get('logging_level', 'INFO') | upper %}
        
        {# Simplified level hierarchy using direct comparison #}
        {% set level_order = ['DEBUG', 'INFO', 'WARN', 'ERROR'] %}
        {% if level_order.index(level) >= level_order.index(global_level) %}
            
            {# Simplified colour and status mapping #}
            {% set config = {
                'DEBUG': {'color': '\u001b[90m', 'status': 'DEBUG', 'log_fn': 'log'},
                'INFO': {'color': '\u001b[34m', 'status': 'SUCCESS', 'log_fn': 'log'},
                'WARN': {'color': '\u001b[38;5;214m', 'status': 'WARNING', 'log_fn': 'warn'},
                'ERROR': {'color': '\u001b[31m', 'status': 'ERROR', 'log_fn': 'error'}
            } %}
            
            {% set reset = '\u001b[0m' %}
            {% set level_config = config[level] %}
            {% set status = status if status is not none else level_config['status'] %}
            
            {# Simplified message construction #}
            {% set prefix = "Macro Polo: " %}
            {% set parts = [
                level_config['color'] ~ prefix ~ reset if level != 'DEBUG' else '\u001b[90m' ~ prefix,
                macro_name ~ " • " if macro_name is not none else "",
                message,
                " • " ~ model_id if model_id is not none else "",
                reset if level != 'DEBUG' else ''
            ] %}
            {% set base_message = parts | join('') %}
            
            {# Add status indicator for INFO and WARN levels #}
            {% if level in ['INFO', 'WARN'] %}
                {% set message_length = (base_message | replace('\u001b[90m', '') | replace('\u001b[34m', '') | replace('\u001b[38;5;214m', '') | replace('\u001b[31m', '') | replace('\u001b[0m', '') | length) %}
                {% set dots = "." * ([80 - message_length, 0] | max) %}
                {% set base_message = base_message ~ dots ~ " [" ~ level_config['color'] ~ status ~ reset ~ "]" %}
            {% endif %}
            
            {# Simplified logging dispatch #}
            {% if level_config['log_fn'] == 'log' %}
                {{ log(base_message, info=true) }}
            {% elif level_config['log_fn'] == 'warn' %}
                {{ exceptions.warn(base_message) }}
            {% elif level_config['log_fn'] == 'error' %}
                {{ exceptions.raise_compiler_error(base_message) }}
            {% endif %}
            
        {% endif %}
    {% endif %}
{% endmacro %}