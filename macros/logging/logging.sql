{% macro logging(message=none, level='INFO', model_id=none, status=none, macro_name=none) %}
    {{ return(adapter.dispatch('logging', 'dbt_macro_polo')(message, level, model_id, status, macro_name)) }}
{% endmacro %}

{% macro default__logging(message, level, model_id, status, macro_name) %}
    {%- if execute -%}
        {%- set level = level | upper -%}
        {%- set global_level = var('macro_polo', {}).get('logging_level', 'INFO') | upper -%}
        
        {%- set levels = {'DEBUG': 0, 'INFO': 1, 'WARN': 2, 'ERROR': 3} -%}
        {%- if levels[level] >= levels.get(global_level, 1) -%}
            
            {# --- Styling Configuration --- #}
            {%- set colors = {
                'DEBUG': '\033[90m',
                'INFO':  '\033[36m',
                'WARN':  '\033[33m',
                'ERROR': '\033[31m',
                'RESET': '\033[0m',
                'BOLD':  '\033[1m'
            } -%}
            
            {%- set c = colors[level] -%}
            {%- set r = colors['RESET'] -%}
            {%- set b = colors['BOLD'] -%}
            
            {# --- Message Construction --- #}
            {# Format: [Level] [Macro-Polo] [Model] Message ... [Status] #}
            
            {%- set header = "[" ~ c ~ b ~ level.ljust(5) ~ r ~ "] " -%}
            {%- set package_label = colors['DEBUG'] ~ "Macro-Polo" ~ r -%}
            
            {%- set context_info = [] -%}
            {%- if model_id -%}
                {%- do context_info.append(colors['DEBUG'] ~ model_id ~ r) -%}
            {%- endif -%}
            {%- if macro_name -%}
                {%- do context_info.append(colors['DEBUG'] ~ "(" ~ macro_name ~ ")" ~ r) -%}
            {%- endif -%}
            {%- set context_str = " " ~ context_info | join(' ') if context_info else "" -%}

            {%- set final_msg = header ~ package_label ~ context_str ~ " " ~ c ~ message ~ r -%}

            {# --- Right Aligned Status (Optional) --- #}
            {%- if status -%}
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
