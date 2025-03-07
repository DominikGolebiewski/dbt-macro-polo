{% macro parse_cron_expression(cron_expression) %}
    {{ return(adapter.dispatch('parse_cron_expression', 'dbt_macro_polo')(cron_expression)) }}
{% endmacro %}

{% macro default__parse_cron_expression(cron_expression) %}
    {# Initialise macro context #}
    {% set macro_context = dbt_macro_polo.create_macro_context("parse_cron_expression") %}
    {% set macro_name = macro_context.macro_name %}
    {% set model_id = macro_context.model_id %}

    {{ dbt_macro_polo.logging(macro_name, "Parsing cron expression: " ~ cron_expression, level='DEBUG') }}

    {# Parse cron expression #}
    {% set parts = cron_expression.split() %}
    {% if parts | length != 5 %}
        {{ dbt_macro_polo.logging(message="Invalid cron expression format. Expected 5 fields, got: " ~ parts | length, 
            model_id=model_id, level='ERROR') }}
        {{ return(none) }}
    {% endif %}

    {# Extract fields #}
    {% set minute = parts[0] %}
    {% set hour = parts[1] %}
    {% set day_of_month = parts[2] %}
    {% set month = parts[3] %}
    {% set day_of_week = parts[4] %}

    {# Return structured cron parts for further processing #}
    {% set cron_parts = {
        'minute': minute,
        'hour': hour,
        'day_of_month': day_of_month,
        'month': month,
        'day_of_week': day_of_week
    } %}

    {{ return(cron_parts) }}
{% endmacro %}

{% macro expand_cron_field(field_value, min_val, max_val) %}
    {{ return(adapter.dispatch('expand_cron_field', 'dbt_macro_polo')(field_value, min_val, max_val)) }}
{% endmacro %}

{% macro default__expand_cron_field(field_value, min_val, max_val) %}
    {% set result = [] %}
    
    {# Handle asterisk/wildcard #}
    {% if field_value == '*' %}
        {% for i in range(min_val, max_val + 1) %}
            {% do result.append(i) %}
        {% endfor %}
        {{ return(result) }}
    {% endif %}
    
    {# Handle list of values, e.g., "1,3,5" #}
    {% if field_value.find(',') >= 0 %}
        {% for val in field_value.split(',') %}
            {% if val.find('-') >= 0 %}
                {# Handle range within list #}
                {% set range_parts = val.split('-') %}
                {% set range_start = range_parts[0] | int %}
                {% set range_end = range_parts[1] | int %}
                {% for i in range(range_start, range_end + 1) %}
                    {% do result.append(i) %}
                {% endfor %}
            {% elif val.find('/') >= 0 %}
                {# Handle step within list #}
                {% set step_parts = val.split('/') %}
                {% set step_range = step_parts[0] %}
                {% set step_value = step_parts[1] | int %}
                {% set range_min = min_val %}
                {% set range_max = max_val %}
                
                {# Handle range in step range #}
                {% if step_range.find('-') >= 0 %}
                    {% set range_parts = step_range.split('-') %}
                    {% set range_min = range_parts[0] | int %}
                    {% set range_max = range_parts[1] | int %}
                {% elif step_range != '*' %}
                    {# If not asterisk, it's a specific starting point #}
                    {% set range_min = step_range | int %}
                {% endif %}
                
                {% for i in range(range_min, range_max + 1) %}
                    {% if (i - range_min) % step_value == 0 %}
                        {% do result.append(i) %}
                    {% endif %}
                {% endfor %}
            {% else %}
                {# Handle simple value in list #}
                {% do result.append(val | int) %}
            {% endif %}
        {% endfor %}
        {{ return(result) }}
    {% endif %}
    
    {# Handle range of values, e.g., "1-5" #}
    {% if field_value.find('-') >= 0 %}
        {% set range_parts = field_value.split('-') %}
        {% set range_start = range_parts[0] | int %}
        {% set range_end = range_parts[1] | int %}
        {% for i in range(range_start, range_end + 1) %}
            {% do result.append(i) %}
        {% endfor %}
        {{ return(result) }}
    {% endif %}
    
    {# Handle step values, e.g., "*/15" #}
    {% if field_value.find('/') >= 0 %}
        {% set step_parts = field_value.split('/') %}
        {% set step_range = step_parts[0] %}
        {% set step_value = step_parts[1] | int %}
        {% set range_min = min_val %}
        {% set range_max = max_val %}
        
        {# Handle range in step range #}
        {% if step_range.find('-') >= 0 %}
            {% set range_parts = step_range.split('-') %}
            {% set range_min = range_parts[0] | int %}
            {% set range_max = range_parts[1] | int %}
        {% elif step_range != '*' %}
            {# If not asterisk, it's a specific starting point #}
            {% set range_min = step_range | int %}
        {% endif %}
        
        {% for i in range(range_min, range_max + 1) %}
            {% if (i - range_min) % step_value == 0 %}
                {% do result.append(i) %}
            {% endif %}
        {% endfor %}
        {{ return(result) }}
    {% endif %}
    
    {# Handle simple value #}
    {% do result.append(field_value | int) %}
    {{ return(result) }}
{% endmacro %}

{% macro is_cron_match(cron_expression, current_time=none) %}
    {{ return(adapter.dispatch('is_cron_match', 'dbt_macro_polo')(cron_expression, current_time)) }}
{% endmacro %}

{% macro default__is_cron_match(cron_expression, current_time=none) %}
    {# Initialise macro context #}
    {% set macro_context = dbt_macro_polo.create_macro_context("is_cron_match") %}
    {% set macro_name = macro_context.macro_name %}
    {% set model_id = macro_context.model_id %}

    {# Get current time if not provided #}
    {% set current_time = current_time or modules.datetime.datetime.now() %}
    
    {# Parse cron expression #}
    {% set cron_parts = dbt_macro_polo.parse_cron_expression(cron_expression) %}
    {% if cron_parts is none %}
        {{ return(false) }}
    {% endif %}
    
    {# Map day of week names to numbers (0-6 where 0 is Sunday) #}
    {% set day_of_week_names = {
        'sun': 0, 'sunday': 0,
        'mon': 1, 'monday': 1,
        'tue': 2, 'tuesday': 2, 
        'wed': 3, 'wednesday': 3,
        'thu': 4, 'thursday': 4,
        'fri': 5, 'friday': 5,
        'sat': 6, 'saturday': 6
    } %}
    
    {# Expand each field to get valid values #}
    {% set minutes = dbt_macro_polo.expand_cron_field(cron_parts.minute, 0, 59) %}
    {% set hours = dbt_macro_polo.expand_cron_field(cron_parts.hour, 0, 23) %}
    {% set days_of_month = dbt_macro_polo.expand_cron_field(cron_parts.day_of_month, 1, 31) %}
    {% set months = dbt_macro_polo.expand_cron_field(cron_parts.month, 1, 12) %}
    
    {# Parse day of week, handling named days #}
    {% set days_of_week_field = cron_parts.day_of_week.lower() %}
    {% set days_of_week = [] %}
    
    {# Replace named days with numbers #}
    {% for name, number in day_of_week_names.items() %}
        {% set days_of_week_field = days_of_week_field | replace(name, number | string) %}
    {% endfor %}
    
    {# Expand day of week #}
    {% set days_of_week = dbt_macro_polo.expand_cron_field(days_of_week_field, 0, 6) %}
    
    {# Get current date/time components #}
    {% set current_minute = current_time.minute %}
    {% set current_hour = current_time.hour %}
    {% set current_day_of_month = current_time.day %}
    {% set current_month = current_time.month %}
    {% set current_day_of_week = current_time.weekday() %}
    
    {# Adjust Python's weekday (0-6, Monday is 0) to cron's convention (0-6, Sunday is 0) #}
    {% set current_day_of_week = (current_day_of_week + 1) % 7 %}
    
    {# Check if current time matches the cron expression #}
    {% set minute_match = current_minute in minutes %}
    {% set hour_match = current_hour in hours %}
    {% set month_match = current_month in months %}
    
    {# Day match is true if either day of month or day of week matches (or's relationship) #}
    {% set day_of_month_match = current_day_of_month in days_of_month %}
    {% set day_of_week_match = current_day_of_week in days_of_week %}
    {% set day_match = day_of_month_match or (cron_parts.day_of_month == '*' and day_of_week_match) %}
    
    {# If both day_of_month and day_of_week are specified (not *), only one needs to match #}
    {% if cron_parts.day_of_month != '*' and cron_parts.day_of_week != '*' %}
        {% set day_match = day_of_month_match or day_of_week_match %}
    {% endif %}
    
    {# Log match details #}
    {{ dbt_macro_polo.logging(macro_name, "Cron match details: " ~ {
        "minute": minute_match,
        "hour": hour_match,
        "day": day_match,
        "month": month_match,
        "current_time": current_time,
        "cron": cron_expression
    }, level='DEBUG') }}
    
    {# Return true if all components match #}
    {{ return(minute_match and hour_match and day_match and month_match) }}
{% endmacro %} 