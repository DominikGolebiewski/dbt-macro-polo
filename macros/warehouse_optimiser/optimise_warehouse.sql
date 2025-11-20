{% macro optimise_warehouse(query_operation='ctas') %}
    {{ return(adapter.dispatch('optimise_warehouse', 'dbt_macro_polo')(query_operation)) }}
{% endmacro %}

{% macro default__optimise_warehouse(query_operation='ctas') %}

    {% set model_id = this.schema ~ "." ~ this.name if this else 'unknown_model' %}
    {% set macro_polo = var('macro_polo', {}) %}
    {% set macro_name = 'optimise_warehouse' %}
    
    {# 1. Validation #}
    {% if query_operation not in ['ctas', 'insert', 'delete'] %}
        {{ dbt_macro_polo.logging(message="Invalid query operation: " ~ query_operation, level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {% set optimiser_config = macro_polo.get('warehouse_optimiser', {}) %}
    {% set model_config = model.config.get('meta', {}).get('warehouse_optimiser', {}) %}

    {# Check if optimiser is enabled globally and at model level #}
    {% if not (optimiser_config.get('enabled', false) and model_config.get('enabled', false)) %}
        {# Only log disabled status once during CTAS to reduce noise #}
        {% if query_operation == 'ctas' %}
            {{ dbt_macro_polo.logging(message="Warehouse Optimiser disabled", level='DEBUG', model_id=model_id, macro_name=macro_name) }}
        {% endif %}
        {{ return('') }}
    {% endif %}
    
    {{ dbt_macro_polo.logging(message="Starting optimisation for operation", status=query_operation | upper, level='INFO', model_id=model_id, macro_name=macro_name) }}

    {% set is_incremental = model.config.get('materialized') == 'incremental' %}
    {% set strategy = model.config.get('incremental_strategy') %}
    
    {% if not (is_incremental and strategy == 'delete+insert') %}
         {{ dbt_macro_polo.logging(message="Optimiser requires incremental materialization with delete+insert strategy", level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {# 2. Configuration Resolution #}
    {% set is_full_refresh = dbt_macro_polo.should_full_refresh() %}
    {% set op_type_config = model_config.get('operation_type', {}) %}
    
    {% if is_full_refresh %}
        {% set active_config = op_type_config.get('on_full_refresh', {}) %}
        {% set context_label = 'FULL_REFRESH' %}
    {% else %}
        {% set active_config = op_type_config.get('on_run', {}).get(query_operation, {}) %}
        {% set context_label = query_operation | upper %}
    {% endif %}

    {% if not active_config %}
        {{ dbt_macro_polo.logging(message="No configuration found for " ~ context_label, level='WARN', model_id=model_id, macro_name=macro_name) }}
        {{ return('') }}
    {% endif %}

    {# 3. Volume Determination #}
    {% set upstream_dependency = op_type_config.get('upstream_dependency', []) %}
    {% set volume = 0 %}
    
    {% if execute and not is_full_refresh %}
        {% set timestamp_column = model.config.get('timestamp_column') %}
        {% if not timestamp_column %}
             {{ dbt_macro_polo.logging(message="timestamp_column required for monitoring", level='ERROR', model_id=model_id, macro_name=macro_name) }}
        {% endif %}
        {% set volume = dbt_macro_polo.get_upstream_volume(model_id, upstream_dependency, timestamp_column) %}
    {% endif %}
    
    {# Check if volume is 0 - if so, force XS warehouse and skip other checks #}
    {% if volume == 0 and not is_full_refresh %}
         {{ dbt_macro_polo.logging(message="Zero upstream volume detected", status='XS', level='INFO', model_id=model_id, macro_name=macro_name) }}
         {% set target_size = 'xs' %}
    {% else %}
         {# 4. Size Determination #}
         {% set target_size = dbt_macro_polo.determine_optimal_size(active_config, volume, model_id) %}
    {% endif %}

    {# 5. Allocation #}
    {% set warehouse = dbt_macro_polo.allocate_warehouse(target_size) %}
    
    {{ dbt_macro_polo.logging(message="Optimiser selected warehouse", model_id=model_id, status=warehouse | upper, macro_name=macro_name) }}
    {{ return('use warehouse ' ~ warehouse) }}

{% endmacro %}

{# Snowflake specific implementation #}
{% macro determine_optimal_size(config, volume, model_id) %}
    {{ return(adapter.dispatch('determine_optimal_size', 'dbt_macro_polo')(config, volume, model_id)) }}
{% endmacro %}

{% macro default__determine_optimal_size(config, volume, model_id) %}
    {% set default_size = var('macro_polo', {}).get('warehouse_optimiser', {}).get('default_warehouse_size', 'xs') %}
    {% set base_size = config.get('warehouse_size', default_size) %}
    {% set macro_name = 'optimise_warehouse' %}
    
    {# Check Scheduling #}
    {% if config.get('scheduling', {}).get('enabled') %}
        {% set current_time = modules.datetime.datetime.now() %}
        {% set current_day = current_time.strftime('%A').lower() %}
        
        {% for schedule in config.get('scheduling', {}).get('schedules', []) %}
            {% if current_day in schedule.get('days', []) %}
                {% set times = schedule.get('times', {}) %}
                {% if dbt_macro_polo.is_within_time_range(schedule.get('name'), current_time, times.get('start'), times.get('end')) %}
                    {{ dbt_macro_polo.logging(message="Schedule matched: " ~ schedule.get('name'), model_id=model_id, level='DEBUG', macro_name=macro_name) }}
                    
                    {# Schedule Monitoring Override #}
                    {% if schedule.get('monitoring', {}).get('enabled') %}
                        {{ return(dbt_macro_polo.evaluate_thresholds(schedule.get('monitoring', {}).get('thresholds', []), volume, schedule.get('warehouse_size', base_size))) }}
                    {% endif %}
                    
                    {{ return(schedule.get('warehouse_size', base_size)) }}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {# Check Base Monitoring #}
    {% if config.get('monitoring', {}).get('enabled') %}
        {{ return(dbt_macro_polo.evaluate_thresholds(config.get('monitoring', {}).get('thresholds', []), volume, base_size)) }}
    {% endif %}

    {{ return(base_size) }}
{% endmacro %}

{% macro evaluate_thresholds(thresholds, volume, default_size) %}
    {{ return(adapter.dispatch('evaluate_thresholds', 'dbt_macro_polo')(thresholds, volume, default_size)) }}
{% endmacro %}

{% macro default__evaluate_thresholds(thresholds, volume, default_size) %}
    {# Sort thresholds descending by rows #}
    {% set sorted = thresholds | sort(attribute='rows', reverse=true) %}
    {% for t in sorted %}
        {% if volume >= t.rows %}
            {{ return(t.warehouse_size) }}
        {% endif %}
    {% endfor %}
    {{ return(default_size) }}
{% endmacro %}
