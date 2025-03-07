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

{% macro cron_match(cron_expression, current_time=none) %}
    {{ return(adapter.dispatch('cron_match', 'dbt_macro_polo')(cron_expression, current_time)) }}
{% endmacro %}

{% macro default__cron_match(cron_expression, current_time=none) %}
    {# Get current time if not provided #}
    {% set current_time = current_time or modules.datetime.datetime.now() %}
    
    {# Parse cron expression #}
    {% set parts = cron_expression.split() %}
    {% if parts | length != 5 %}
        {{ log("Invalid cron expression format: " ~ cron_expression, info=true) }}
        {{ return(false) }}
    {% endif %}
    
    {# Extract fields #}
    {% set minute = parts[0] %}
    {% set hour = parts[1] %}
    {% set day_month = parts[2] %}
    {% set month = parts[3] %}
    {% set day_week = parts[4] %}
    
    {# Map day of week names to numbers #}
    {% set day_names = {'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6} %}
    {% for name, num in day_names.items() %}
        {% set day_week = day_week | lower | replace(name, num | string) %}
    {% endfor %}
    
    {# Get current date components #}
    {% set cur = {
        'minute': current_time.minute,
        'hour': current_time.hour,
        'day_month': current_time.day,
        'month': current_time.month,
        'day_week': (current_time.weekday() + 1) % 7  # Adjust to cron's Sunday=0 convention
    } %}
    
    {# Check if this is a time range (e.g., 0 7-19 * * 1-5) #}
    {% set is_time_range = hour.find('-') >= 0 and minute != '*' and minute.find('-') < 0 %}
    
    {# Time range check (e.g., business hours) #}
    {% if is_time_range %}
        {# Parse hour range #}
        {% set hour_range = hour.split('-') %}
        {% set hour_start = hour_range[0] | int %}
        {% set hour_end = hour_range[1] | int %}
        
        {# Check if current hour is in range #}
        {% set hour_match = cur.hour >= hour_start and cur.hour <= hour_end %}
        
        {# Check day and month #}
        {% set day_match = field_matches(day_month, cur.day_month, 1, 31) %}
        {% set day_week_match = field_matches(day_week, cur.day_week, 0, 6) %}
        {% set month_match = field_matches(month, cur.month, 1, 12) %}
        
        {# Day match is special - either day of month or day of week matches #}
        {% if day_month != '*' and day_week != '*' %}
            {% set day_match = day_match or day_week_match %}
        {% elif day_month == '*' %}
            {% set day_match = day_week_match %}
        {% endif %}
        
        {{ return(hour_match and day_match and month_match) }}
    
    {# Standard cron match #}
    {% else %}
        {# Check each field #}
        {% set minute_match = field_matches(minute, cur.minute, 0, 59) %}
        {% set hour_match = field_matches(hour, cur.hour, 0, 23) %}
        {% set day_match = field_matches(day_month, cur.day_month, 1, 31) %}
        {% set day_week_match = field_matches(day_week, cur.day_week, 0, 6) %}
        {% set month_match = field_matches(month, cur.month, 1, 12) %}
        
        {# Day match is special - either day of month or day of week matches #}
        {% if day_month != '*' and day_week != '*' %}
            {% set day_match = day_match or day_week_match %}
        {% elif day_month == '*' %}
            {% set day_match = day_week_match %}
        {% endif %}
        
        {{ return(minute_match and hour_match and day_match and month_match) }}
    {% endif %}
{% endmacro %}

{% macro field_matches(field, value, min_val, max_val) %}
    {{ return(adapter.dispatch('field_matches', 'dbt_macro_polo')(field, value, min_val, max_val)) }}
{% endmacro %}

{% macro default__field_matches(field, value, min_val, max_val) %}
    {# Handle wildcards #}
    {% if field == '*' %}
        {{ return(true) }}
    {% endif %}
    
    {# Handle comma-separated values #}
    {% if field.find(',') >= 0 %}
        {% for val in field.split(',') %}
            {% if field_matches(val, value, min_val, max_val) %}
                {{ return(true) }}
            {% endif %}
        {% endfor %}
        {{ return(false) }}
    {% endif %}
    
    {# Handle ranges #}
    {% if field.find('-') >= 0 %}
        {% set range_parts = field.split('-') %}
        {% set start = range_parts[0] | int %}
        {% set end = range_parts[1] | int %}
        {{ return(value >= start and value <= end) }}
    {% endif %}
    
    {# Handle steps #}
    {% if field.find('/') >= 0 %}
        {% set step_parts = field.split('/') %}
        {% set range_expr = step_parts[0] %}
        {% set step = step_parts[1] | int %}
        
        {# Determine the range #}
        {% if range_expr == '*' %}
            {% set start = min_val %}
            {% set end = max_val %}
        {% elif range_expr.find('-') >= 0 %}
            {% set range_parts = range_expr.split('-') %}
            {% set start = range_parts[0] | int %}
            {% set end = range_parts[1] | int %}
        {% else %}
            {% set start = range_expr | int %}
            {% set end = max_val %}
        {% endif %}
        
        {# Check if value matches step pattern #}
        {{ return(value >= start and value <= end and (value - start) % step == 0) }}
    {% endif %}
    
    {# Handle simple value #}
    {{ return(value == field | int) }}
{% endmacro %} 