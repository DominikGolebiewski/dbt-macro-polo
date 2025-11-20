{% macro is_within_time_range(schedule_name, current_time, start_time, end_time) %}
    {{ return(adapter.dispatch('is_within_time_range', 'dbt_macro_polo')(schedule_name, current_time, start_time, end_time)) }}
{% endmacro %}

{% macro default__is_within_time_range(schedule_name, current_time, start_time, end_time) %}
    {% set start_parts = start_time.split(':') %}
    {% set end_parts = end_time.split(':') %}
    
    {% set start_minutes = start_parts[0]|int * 60 + start_parts[1]|int %}
    {% set end_minutes = end_parts[0]|int * 60 + end_parts[1]|int %}
    {% set current_minutes = current_time.hour * 60 + current_time.minute %}
    
    {# Handle 24:00 or 00:00 as end of day if intended, but usually input is 00:00 #}
    {% if end_minutes == 0 and end_parts[0]|int == 0 %}
        {# Treat 00:00 as next day midnight if strictly greater than start? 
           Or just handle standard comparisons. 
        #}
    {% endif %}

    {% if start_minutes <= end_minutes %}
        {# Standard range e.g. 09:00 to 17:00 #}
        {% set is_within = current_minutes >= start_minutes and current_minutes <= end_minutes %}
    {% else %}
        {# Spans midnight e.g. 22:00 to 02:00 #}
        {% set is_within = current_minutes >= start_minutes or current_minutes <= end_minutes %}
    {% endif %}

    {{ return(is_within) }}
{% endmacro %}
