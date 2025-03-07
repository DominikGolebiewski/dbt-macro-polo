{% macro handle_scheduling(operation_config, row_count, has_on_dry_run_config, current_time=none, default_warehouse_size='xs') %}
    {{ return(adapter.dispatch('handle_scheduling', 'dbt_macro_polo')(operation_config, row_count, has_on_dry_run_config, current_time, default_warehouse_size)) }}
{% endmacro %}

{% macro default__handle_scheduling(operation_config, row_count, has_on_dry_run_config, current_time=none, default_warehouse_size='xs') %}
    {# Get configuration values #}
    {% set default_warehouse = operation_config.get('default', default_warehouse_size) %}
    {% set current_time = current_time or modules.datetime.datetime.now() %}
    {% set final_size = namespace(value=default_warehouse) %}
    
    {# Check for cron-based scheduling #}
    {% set cron_schedules = operation_config.get('schedule', []) %}
    {% if cron_schedules | length > 0 %}
        {{ log("Checking cron schedules", info=true) }}
        
        {# Check each cron schedule #}
        {% for schedule in cron_schedules %}
            {% set cron = schedule.get('cron', '') %}
            {% if cron and dbt_macro_polo.cron_match(cron, current_time) %}
                {% set scale = schedule.get('scale', default_warehouse) %}
                {{ log("Matched cron schedule: " ~ cron ~ ", using scale: " ~ scale, info=true) }}
                
                {# Apply monitoring if configured #}
                {% if has_on_dry_run_config and schedule.get('monitor', []) | length > 0 %}
                    {% set final_size.value = dbt_macro_polo.handle_monitoring(
                        row_count, 
                        schedule.get('monitor', []), 
                        scale
                    ) %}
                {% else %}
                    {% set final_size.value = scale %}
                {% endif %}
                
                {# Break after first match #}
                {% break %}
            {% endif %}
        {% endfor %}
        
        {# If no schedule matched, check for monitoring #}
        {% if final_size.value == default_warehouse %}
            {{ log("No cron schedule matched, using default or monitoring", info=true) }}
            {% if has_on_dry_run_config and operation_config.get('monitor', []) | length > 0 %}
                {% set final_size.value = dbt_macro_polo.handle_monitoring(
                    row_count, 
                    operation_config.get('monitor', []), 
                    default_warehouse
                ) %}
            {% endif %}
        {% endif %}
    {% else %}
        {# Legacy scheduling for backwards compatibility #}
        {% set scheduling_config = operation_config.get('scheduling', {}) %}
        {% if scheduling_config.get('enabled', false) %}
            {{ log("Using legacy scheduling", info=true) }}
            {% set schedules = scheduling_config.get('schedules', {}) %}
            {% set current_day = current_time.strftime('%A').lower() %}
            
            {% for schedule in schedules %}
                {% set times = schedule.get('times', {}) %}
                {% set days = schedule.get('days', []) %}
                
                {% if current_day in days and dbt_macro_polo.is_within_time_range(current_time, times.get('start'), times.get('end')) %}
                    {% set scale = schedule.get('warehouse_size', default_warehouse) %}
                    {{ log("Matched legacy schedule, using scale: " ~ scale, info=true) }}
                    
                    {# Apply monitoring if configured #}
                    {% if has_on_dry_run_config and schedule.get('monitoring', {}).get('enabled', false) %}
                        {% set final_size.value = dbt_macro_polo.handle_monitoring(
                            row_count, 
                            schedule.get('monitoring', {}).get('thresholds', []), 
                            scale
                        ) %}
                    {% else %}
                        {% set final_size.value = scale %}
                    {% endif %}
                    
                    {# Break after first match #}
                    {% break %}
                {% endif %}
            {% endfor %}
            
            {# If no schedule matched, check for monitoring #}
            {% if final_size.value == default_warehouse and has_on_dry_run_config and operation_config.get('monitor', []) | length > 0 %}
                {% set final_size.value = dbt_macro_polo.handle_monitoring(
                    row_count, 
                    operation_config.get('monitor', []), 
                    default_warehouse
                ) %}
            {% endif %}
        {% elif has_on_dry_run_config and operation_config.get('monitor', []) | length > 0 %}
            {# Apply monitoring if no scheduling but monitoring is configured #}
            {% set final_size.value = dbt_macro_polo.handle_monitoring(
                row_count, 
                operation_config.get('monitor', []), 
                default_warehouse
            ) %}
        {% endif %}
    {% endif %}
    
    {{ return(final_size.value) }}
{% endmacro %}

{% macro is_within_time_range(current_time, start_time, end_time) %}
    {{ return(adapter.dispatch('is_within_time_range', 'dbt_macro_polo')(current_time, start_time, end_time)) }}
{% endmacro %}

{% macro default__is_within_time_range(current_time, start_time, end_time) %}
    {# Convert times to minutes for comparison #}
    {% set start_parts = start_time.split(':') %}
    {% set end_parts = end_time.split(':') %}
    {% set start_minutes = start_parts[0]|int * 60 + start_parts[1]|int %}
    {% set end_minutes = end_parts[0]|int * 60 + end_parts[1]|int %}
    {% set current_minutes = current_time.hour * 60 + current_time.minute %}

    {# Handle midnight case #}
    {% if end_minutes == 0 %}
        {% set end_minutes = 24 * 60 %}
    {% endif %}

    {# Check if current time is within range #}
    {{ return(current_minutes >= start_minutes and current_minutes <= end_minutes) }}
{% endmacro %}