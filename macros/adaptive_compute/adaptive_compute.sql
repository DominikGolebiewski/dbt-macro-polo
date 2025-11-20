{% macro adaptive_compute(query_operation='ctas') %}
    {{ return(adapter.dispatch('adaptive_compute', 'dbt_macro_polo')(query_operation)) }}
{% endmacro %}

{% macro default__adaptive_compute(query_operation='ctas') %}

    {% set model_id = this.schema ~ "." ~ this.name if this else 'unknown_model' %}
    {% set macro_polo = var('macro_polo', {}) %}
    {% set macro_name = 'adaptive_compute' %}
    
    {# 1. Validation #}
    {% if query_operation not in ['ctas', 'insert', 'delete'] %}
        {{ dbt_macro_polo.log_event(message="Invalid query operation: " ~ query_operation, level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {# Update: Use adaptive_compute for project config #}
    {% set adaptive_config = macro_polo.get('adaptive_compute', {}) %}
    {# Update: Use compute_provisioning for model config #}
    {% set model_config = model.config.get('meta', {}).get('adaptive_compute', {}) %}

    {# Check if adaptive compute is enabled globally and at model level #}
    {% if not (adaptive_config.get('enabled', false) and model_config.get('enabled', false)) %}
        {# Only log disabled status once during CTAS to reduce noise #}
        {% if query_operation == 'ctas' %}
            {{ dbt_macro_polo.log_event(message="Adaptive Compute disabled", level='DEBUG', model_id=model_id, macro_name=macro_name) }}
        {% endif %}
        {{ return('') }}
    {% endif %}
    
    {{ dbt_macro_polo.log_event(message="Starting adaptive compute for operation", status=query_operation | upper, level='INFO', model_id=model_id, macro_name=macro_name) }}

    {% set is_incremental = model.config.get('materialized') == 'incremental' %}
    {% set strategy = model.config.get('incremental_strategy') %}
    
    {% if not (is_incremental and strategy == 'delete+insert') %}
         {{ dbt_macro_polo.log_event(message="Adaptive compute requires incremental materialization with delete+insert strategy", level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {# 2. Configuration Resolution #}
    {% set is_full_refresh = dbt_macro_polo.should_full_refresh() %}
    {% set strategies_config = model_config.get('execution_strategies', {}) %}
    
    {# Map legacy operations to new strategy keys #}
    {% set operation_map = {
        'ctas': 'build',
        'insert': 'append',
        'delete': 'prune'
    } %}
    
    {% if is_full_refresh %}
        {% set active_config = strategies_config.get('full_refresh', {}) %}
        {% set context_label = 'FULL_REFRESH' %}
    {% else %}
        {% set strategy_key = operation_map.get(query_operation, query_operation) %}
        {% set active_config = strategies_config.get('incremental', {}).get(strategy_key, {}) %}
        {% set context_label = query_operation | upper %}
    {% endif %}

    {% if not active_config %}
        {{ dbt_macro_polo.log_event(message="No configuration found for " ~ context_label, level='WARN', model_id=model_id, macro_name=macro_name) }}
        {{ return('') }}
    {% endif %}

    {# 3. Volume Determination #}
    {% set volume_monitors = model_config.get('volume_monitors', []) %}
    {% set volume = 0 %}
    
    {% if execute and not is_full_refresh %}
        {% set timestamp_column = model.config.get('timestamp_column') %}
        {% if not timestamp_column %}
             {{ dbt_macro_polo.log_event(message="timestamp_column required for monitoring", level='ERROR', model_id=model_id, macro_name=macro_name) }}
        {% endif %}
        {% set volume = dbt_macro_polo.get_upstream_volume(model_id, volume_monitors, timestamp_column) %}
    {% endif %}
    
    {# Check if volume is 0 - if so, force XS warehouse and skip other checks #}
    {% if volume == 0 and not is_full_refresh %}
         {{ dbt_macro_polo.log_event(message="Zero upstream volume detected", status='XS', level='INFO', model_id=model_id, macro_name=macro_name) }}
         {% set target_size = 'xs' %}
    {% else %}
         {# 4. Size Determination #}
         {% set target_size = dbt_macro_polo.determine_optimal_size(active_config, volume, model_id) %}
    {% endif %}

    {# 5. Allocation #}
    {% set warehouse = dbt_macro_polo.provision_compute(target_size) %}
    
    {{ dbt_macro_polo.log_event(message="Adaptive compute selected warehouse", model_id=model_id, status=warehouse | upper, macro_name=macro_name) }}
    {{ return('use warehouse ' ~ warehouse) }}

{% endmacro %}

{# Snowflake specific implementation #}
{% macro determine_optimal_size(config, volume, model_id) %}
    {{ return(adapter.dispatch('determine_optimal_size', 'dbt_macro_polo')(config, volume, model_id)) }}
{% endmacro %}

{% macro default__determine_optimal_size(config, volume, model_id) %}
    {# Update: Use baseline_size from adaptive_compute #}
    {% set default_size = var('macro_polo', {}).get('adaptive_compute', {}).get('baseline_size', 'xs') %}
    {% set base_size = config.get('warehouse_size', default_size) %}
    {% set macro_name = 'adaptive_compute' %}
    
    {# Check Time Based Overrides #}
    {% set time_overrides = config.get('time_based_overrides', {}) %}
    {% if time_overrides.get('enabled') %}
        {% set current_time = modules.datetime.datetime.now() %}
        {% set current_day = current_time.strftime('%A').lower() %}
        
        {% for window in time_overrides.get('windows', []) %}
            {% if current_day in window.get('days', []) %}
                {% set time_range = window.get('time_range', {}) %}
                {% if dbt_macro_polo.is_within_time_range(window.get('name'), current_time, time_range.get('start'), time_range.get('end')) %}
                    {{ dbt_macro_polo.log_event(message="Schedule matched: " ~ window.get('name'), model_id=model_id, level='DEBUG', macro_name=macro_name) }}
                    
                    {# Window Specific Volume Scaling #}
                    {% set win_scaling = window.get('volume_based_scaling', {}) %}
                    {% if win_scaling.get('enabled') %}
                        {{ return(dbt_macro_polo.evaluate_thresholds(win_scaling.get('thresholds', []), volume, window.get('warehouse_size', base_size))) }}
                    {% endif %}
                    
                    {{ return(window.get('warehouse_size', base_size)) }}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {# Check Base Volume Scaling #}
    {% set vol_scaling = config.get('volume_based_scaling', {}) %}
    {% if vol_scaling.get('enabled') %}
        {{ return(dbt_macro_polo.evaluate_thresholds(vol_scaling.get('thresholds', []), volume, base_size)) }}
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
