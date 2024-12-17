{% macro handle_scheduling(operation_config, row_count, has_on_dry_run_config, current_time=modules.datetime.datetime.now(), default_warehouse_size='xs') %}
    {{ return(adapter.dispatch('handle_scheduling', 'dbt_macro_polo')(operation_config, row_count, has_on_dry_run_config, current_time, default_warehouse_size)) }}
{% endmacro %}

{% macro default__handle_scheduling(operation_config, row_count, has_on_dry_run_config, current_time=modules.datetime.datetime.now(), default_warehouse_size='xs') %}

    {# Initialise macro context #}
    {% set macro_context = dbt_macro_polo.create_macro_context("handle_scheduling") %}
    {% set macro_name = macro_context.macro_name %}
    {% set model_id = macro_context.model_id %}

    {# Validate operation_config is a dictionary #}
    {% if operation_config is not mapping %}
        {{ dbt_macro_polo.logging(message="operation_config must be a dictionary, got: " ~ operation_config, model_id=model_id, status='ERROR') }}
    {% endif %}

    {# Get scheduling configuration #}
    {% set scheduling_config = operation_config.get('scheduling', {}) %}
    {% set scheduling_enabled = scheduling_config.get('enabled', false) %}
    {% set monitoring_enabled = operation_config.get('monitoring', {}).get('enabled', false) %}
    {% set final_size = namespace(value=default_warehouse_size) %}

    {# Check if scheduling is enabled #}
    {% if scheduling_enabled %}
        {{ dbt_macro_polo.logging(macro_name, "Scheduling config: " ~ scheduling_config, level='DEBUG') }}
        {{ dbt_macro_polo.logging(message="Scheduling enabled", model_id=model_id, status=scheduling_enabled | string | upper) }}
        
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
                            schedule.get('warehouse_size', default_warehouse_size)
                        ) %}
                    {% else %}
                        {% set final_size.value = schedule.get('warehouse_size', default_warehouse_size) %}
                    {% endif %}
                    {% break %}
                {% endif %}
            {% endif %}
            {% if is_matched.value %}
                {% break %}
            {% endif %}
        {% endfor %}

        {% if not is_matched.value %}
            {% if has_on_dry_run_config and monitoring_enabled %}
                {{ dbt_macro_polo.logging(message="Monitoring enabled", model_id=model_id, status=true | string | upper) }}
                {% set final_size.value = dbt_macro_polo.handle_monitoring(
                    operation_config,
                    row_count,
                    operation_config.get('monitoring', {}).get('thresholds', []),
                    operation_config.get('warehouse_size', default_warehouse_size)
                ) %}
            {% else %}
                {% set default_warehouse = operation_config.get('warehouse_size', default_warehouse_size) %}
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
                operation_config.get('monitoring', {}).get('thresholds', []),
                operation_config.get('warehouse_size', default_warehouse_size)
            ) %}
        {% else %}
            {% set final_size.value = operation_config.get('warehouse_size', default_warehouse_size) %}
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