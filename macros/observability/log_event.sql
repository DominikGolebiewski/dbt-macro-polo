{% macro log_event(message, level='INFO', model_id=none, status=none, macro_name=none) %}
    {{ return(adapter.dispatch('log_event', 'dbt_macro_polo')(message, level, model_id, status, macro_name)) }}
{% endmacro %}

{% macro default__log_event(message, level, model_id, status, macro_name) %}
    {%- if execute -%}
        {%- set level = level | default('INFO') | upper -%}

        {# Get global log level #}
        {%- set global_level = var('macro_polo', {}).get('observability', {}).get('log_level', 'INFO') | upper -%}

        {%- set levels = {'DEBUG': 0, 'INFO': 1, 'WARN': 2, 'ERROR': 3} -%}

        {# Default to INFO(1) if level is unknown #}
        {%- set msg_level_val = levels.get(level, 1) -%}
        {%- set global_level_val = levels.get(global_level, 1) -%}

        {%- if msg_level_val >= global_level_val -%}

            {# --- Styling Configuration --- #}
            {%- set colors = {
                'DEBUG': '\033[90m',
                'INFO':  '\033[36m',
                'WARN':  '\033[33m',
                'ERROR': '\033[31m',
                'RESET': '\033[0m',
                'BOLD':  '\033[1m'
            } -%}

            {%- set c = colors.get(level, colors['INFO']) -%}
            {%- set r = colors['RESET'] -%}
            {%- set b = colors['BOLD'] -%}

            {# --- Message Construction --- #}
            {# Format: [Level] [Macro-Polo] [Model] Message ... [Status] #}

            {%- set header = "[" ~ c ~ b ~ level.ljust(5) ~ r ~ "] " -%}
            {%- set package_label = colors['DEBUG'] ~ "Macro-Polo" ~ r -%}

            {%- set context_info = [] -%}

            {#/* -- Breaks the log output when it model_id = this is used as a dictionary in below context
            {%- if model_id -%}
                {%- do context_info.append(colors['DEBUG'] ~ model_id ~ r) -%}
            {%- endif -%}
            */#}
            
            {%- if macro_name -%}
                {%- do context_info.append(colors['DEBUG'] ~ "(" ~ macro_name ~ ")" ~ r) -%}
            {%- endif -%}
            {%- set context_str = " " ~ context_info | join(' ') if context_info else "" -%}

            {%- set final_msg = header ~ package_label ~ context_str ~ " " ~ c ~ message ~ r -%}

            {# --- Right Aligned Status (Optional) --- #}
            {%- if status is not none -%}
                {# Simple padding approximation #}
                {%- set padding = 80 - (message | length) - (model_id | length if model_id else 0) - 25 -%}
                {%- set padding = padding if padding > 0 else 5 -%}
                {%- set dots = colors['DEBUG'] ~ ('.' * padding) ~ r -%}
                {%- set final_msg = final_msg ~ " " ~ dots ~ " [" ~ c ~ b ~ status ~ r ~ "]" -%}
            {%- endif -%}

            {# --- Output Dispatch --- #}
            {%- if level == 'ERROR' -%}
                {{ exceptions.raise_compiler_error(final_msg) }}
            {%- elif level == 'WARN' -%}
                {{ exceptions.warn(final_msg) }}
            {%- else -%}
                {{ log(final_msg, info=true) }}
            {%- endif -%}

        {%- endif -%}
    {%- endif -%}
{% endmacro %}
