{% macro handle_scheduling(operation_config, row_count, has_on_dry_run_config, current_time=none, default_warehouse_size='xs') %}
    {{ return(adapter.dispatch('handle_scheduling', 'dbt_macro_polo')(operation_config, row_count, has_on_dry_run_config, current_time, default_warehouse_size)) }}
{% endmacro %}

{% macro default__handle_scheduling(operation_config, row_count, has_on_dry_run_config, current_time=none, default_warehouse_size='xs') %}

    {# Initialise macro context #}
    {% set macro_context = dbt_macro_polo.create_macro_context("handle_scheduling") %}
    {% set macro_name = macro_context.macro_name %}
    {% set model_id = macro_context.model_id %}

    {# Validate operation_config is a dictionary #}
    {% if operation_config is not mapping %}
        {{ dbt_macro_polo.logging(message="operation_config must be a dictionary, got: " ~ operation_config, model_id=model_id, status='ERROR') }}
    {% endif %}

    {# Get scheduling configuration #}
    {% set default_warehouse = operation_config.get('default', default_warehouse_size) %}
    {% set monitoring_enabled = operation_config.get('monitor', {}) | length > 0 %}
    {% set final_size = namespace(value=default_warehouse) %}
    {% set current_time = current_time or modules.datetime.datetime.now() %}

    {# Check for cron-based scheduling #}
    {% set cron_schedules = operation_config.get('schedule', []) %}
    {% if cron_schedules | length > 0 %}
        {{ dbt_macro_polo.logging(message="Cron scheduling config found", model_id=model_id, status='ENABLED') }}
        
        {# Initialize matched schedule flag #}
        {% set matched_schedule = namespace(found=false, scale=default_warehouse) %}
        
        {# Check each cron schedule #}
        {% for schedule in cron_schedules %}
            {% set cron = schedule.get('cron', '') %}
            {% if cron %}
                {{ dbt_macro_polo.logging(message="Checking cron expression", model_id=model_id, status=cron) }}
                
                {# Check if current time matches the cron expression #}
                {% if dbt_macro_polo.is_cron_match(cron, current_time) %}
                    {% set matched_schedule.found = true %}
                    {% set matched_schedule.scale = schedule.get('scale', default_warehouse) %}
                    {{ dbt_macro_polo.logging(message="Matched cron schedule", model_id=model_id, status=cron) }}
                    
                    {# Handle monitoring if enabled for this schedule #}
                    {% if has_on_dry_run_config and schedule.get('monitor', []) | length > 0 %}
                        {{ dbt_macro_polo.logging(message="Applying monitoring for matched cron schedule", model_id=model_id, status='ENABLED') }}
                        {% set thresholds = schedule.get('monitor', []) %}
                        {% set final_size.value = dbt_macro_polo.handle_monitoring(
                            operation_config,
                            row_count,
                            thresholds,
                            matched_schedule.scale
                        ) %}
                    {% else %}
                        {% set final_size.value = matched_schedule.scale %}
                    {% endif %}
                    
                    {# Break after first match #}
                    {% break %}
                {% endif %}
            {% endif %}
        {% endfor %}
        
        {# If no cron schedule matched, use monitoring or default #}
        {% if not matched_schedule.found %}
            {{ dbt_macro_polo.logging(message="No cron schedule matched", model_id=model_id, status='FALLBACK') }}
            {% if has_on_dry_run_config and monitoring_enabled %}
                {{ dbt_macro_polo.logging(message="Applying default monitoring", model_id=model_id, status='ENABLED') }}
                {% set final_size.value = dbt_macro_polo.handle_monitoring(
                    operation_config,
                    row_count,
                    operation_config.get('monitor', []),
                    default_warehouse
                ) %}
            {% else %}
                {{ dbt_macro_polo.logging(message="Using default warehouse size", model_id=model_id, status=default_warehouse | upper) }}
                {% set final_size.value = default_warehouse %}
            {% endif %}
        {% endif %}
    {% else %}
        {# Legacy scheduling for backwards compatibility #}
        {% set scheduling_config = operation_config.get('scheduling', {}) %}
        {% set scheduling_enabled = scheduling_config.get('enabled', false) %}
        
        {% if scheduling_enabled %}
            {{ dbt_macro_polo.logging(macro_name, "Legacy scheduling config: " ~ scheduling_config, level='DEBUG') }}
            {{ dbt_macro_polo.logging(message="Legacy scheduling enabled", model_id=model_id, status=scheduling_enabled | string | upper) }}
            
            {% set schedules = scheduling_config.get('schedules', {}) %}
            {% set current_day = current_time.strftime('%A').lower() %}
            {% set is_matched = namespace(value=false) %}

            {{ dbt_macro_polo.logging(macro_name, "Set schedules: " ~ schedules ~ " current time: " ~ current_time ~ " current day: " ~ current_day, level='DEBUG') }}

            {% for schedule in schedules %}
                {% set times = schedule.get('times', {}) %}
                {% set days = schedule.get('days', []) %}
                {% set schedule_name = schedule.get('name', 'Unnamed schedule') %}

                {% if current_day in days %}
                    {% if dbt_macro_polo.is_within_time_range(schedule_name, current_time, times.get('start'), times.get('end')) %}
                        {% set is_matched.value = true %}
                        {{ dbt_macro_polo.logging(message="Schedule matched", model_id=model_id, status=schedule_name | upper) }}
                        {% if schedule.get('monitoring', {}).get('enabled', false) and has_on_dry_run_config %}
                            {{ dbt_macro_polo.logging(message=schedule_name ~ " monitoring enabled", model_id=model_id, status=true | string | upper) }}   
                            {% set final_size.value = dbt_macro_polo.handle_monitoring(
                                schedule,
                                row_count,
                                schedule.get('monitoring', {}).get('thresholds', []),
                                schedule.get('warehouse_size', default_warehouse)
                            ) %}
                        {% else %}
                            {% set final_size.value = schedule.get('warehouse_size', default_warehouse) %}
                        {% endif %}
                        {% break %}
                    {% endif %}
                {% endif %}
            {% endfor %}

            {% if not is_matched.value %}
                {% if has_on_dry_run_config and monitoring_enabled %}
                    {{ dbt_macro_polo.logging(message="Monitoring enabled", model_id=model_id, status=true | string | upper) }}
                    {% set final_size.value = dbt_macro_polo.handle_monitoring(
                        operation_config,
                        row_count,
                        operation_config.get('monitoring', {}).get('thresholds', []),
                        default_warehouse
                    ) %}
                {% else %}
                    {{ dbt_macro_polo.logging(message="No schedule matched, using default warehouse size", model_id=model_id, status=default_warehouse | upper) }}
                    {% set final_size.value = default_warehouse %}
                {% endif %}
            {% endif %}
        {% else %}
            {% if has_on_dry_run_config and monitoring_enabled %}
                {{ dbt_macro_polo.logging(message="Monitoring enabled", model_id=model_id, status=true | string | upper) }}
                {% set final_size.value = dbt_macro_polo.handle_monitoring(
                    operation_config,
                    row_count,
                    operation_config.get('monitor', []),
                    default_warehouse
                ) %}
            {% else %}
                {% set final_size.value = default_warehouse %}
            {% endif %}
        {% endif %}
    {% endif %}

    {{ return(final_size.value) }}
{% endmacro %}

{% macro is_within_time_range(schedule_name, current_time, start_time, end_time) %}
    {{ return(adapter.dispatch('is_within_time_range', 'dbt_macro_polo')(schedule_name, current_time, start_time, end_time)) }}
{% endmacro %}

{% macro default__is_within_time_range(schedule_name, current_time, start_time, end_time) %}

    {# Initialise macro context #}
    {% set macro_ctx = dbt_macro_polo.create_macro_context('is_within_time_range') %}
    {% set macro_name = macro_ctx.macro_name %}
    {% set model_id = macro_ctx.model_id %}

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

    {{ dbt_macro_polo.logging(macro_name, "Checking time range: " ~ {
        "schedule": schedule_name,
        "start": start_time,
        "end": end_time,
        "current": current_time
    }, level='DEBUG') }}

    {# Check if current time is within range #}
    {% set is_within_range = current_minutes >= start_minutes and current_minutes <= end_minutes %}
    {{ dbt_macro_polo.logging(message="Schedule time range check", model_id=model_id, status=is_within_range | string | upper) }}

    {{ return(is_within_range) }}
{% endmacro %}