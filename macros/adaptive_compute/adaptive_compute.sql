{% macro adaptive_compute(operation='build') %}
    {{ return(adapter.dispatch('adaptive_compute', 'dbt_macro_polo')(operation)) }}
{% endmacro %}

{% macro default__adaptive_compute(operation='build') %}

    {% set macro_name = 'adaptive_compute' %}

    {#/* Validate operation */#}
    {% if operation not in ['build', 'append', 'prune'] %}
        {% set msg = "Invalid operation: " ~ operation %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=this, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {#/* Get adaptive config */#}
    {% set model_config = dbt_macro_polo._get_adaptive_config(model, this, operation, macro_name) %}
    {% if not model_config %}
        {{ return(none) }}
    {% endif %}

    {{ dbt_macro_polo.log_event(
        message="Starting adaptive compute",
        status=operation | upper,
        level='INFO',
        model_id=model_id,
        macro_name=macro_name
    ) }}

    {# 3. Determine Context Configuration #}
    {% set is_full_refresh = dbt_macro_polo.should_full_refresh(this, model_id) %}
    {% set strategies = model_config.get('execution_strategies', {}) %}

    {% if is_full_refresh %}
        {% set active_config = strategies.get('full_refresh', {}) %}
    {% else %}
        {% set active_config = strategies.get('incremental', {}).get(operation, {}) %}
    {% endif %}

    {% if not active_config %}
        {{ dbt_macro_polo.log_event(
            message="No configuration found for context execution_strategies.",
            level='WARN',
            model_id=model_id,
            macro_name=macro_name
        ) }}
    {% endif %}

    {# 4. Measure Volume #}
    {% set volume = dbt_macro_polo._measure_volume_if_needed(model_config, execute, is_full_refresh, model_id, model, macro_name) %}
    {% if volume is none %}
        {{ return(none) }}
    {% endif %}

    {# 5. Sizing & Allocation #}
    {% set volume_monitors = model_config.get('volume_monitors', []) %}
    {% set target_size = dbt_macro_polo._determine_target_size(active_config, volume, model_id, is_full_refresh, volume_monitors, macro_name) %}

    {% set warehouse = dbt_macro_polo.provision_compute(target_size) %}
    {% if not warehouse %}
         {{ dbt_macro_polo.log_event(
            message="Failed to allocate warehouse",
            level='ERROR',
            model_id=model_id,
            macro_name=macro_name
         ) }}
         {{ return(none) }}
    {% endif %}

    {{ dbt_macro_polo.log_event(
        message="Adaptive compute selected warehouse",
        status=warehouse | upper,
        model_id=model_id,
        macro_name=macro_name
    ) }}
    {{ return('use warehouse ' ~ warehouse) }}

{% endmacro %}

{% macro determine_optimal_size(config, volume, model_id) %}
    {{ return(adapter.dispatch('determine_optimal_size', 'dbt_macro_polo')(config, volume, model_id)) }}
{% endmacro %}

{% macro default__determine_optimal_size(config, volume, model_id) %}
    {#
    Determines warehouse size by checking:
    1. Time-based overrides (and their specific volume scaling)
    2. Standard volume-based scaling
    3. Base configuration
    #}
    {% set macro_name = 'adaptive_compute' %}
    {% set default_size = var('macro_polo', {}).get('adaptive_compute', {}).get('baseline_size', 'xs') %}
    {% set base_size = config.get('warehouse_size', default_size) %}

    {# 1. Time-Based Overrides #}
    {% set time_overrides = config.get('time_based_overrides', {}) %}
    {% if time_overrides.get('enabled') %}
        {% set now = modules.datetime.datetime.now() %}
        {% set current_day = now.strftime('%A').lower() %}

        {% for window in time_overrides.get('windows', []) %}
            {% set time_range = window.get('time_range', {}) %}

            {% if current_day in window.get('days', []) and dbt_macro_polo.is_within_time_range(window.get('name'), now, time_range.get('start'), time_range.get('end')) %}

                {{ dbt_macro_polo.log_event(
                    message="Schedule match: " ~ window.get('name'),
                    level='DEBUG',
                    model_id=model_id,
                    macro_name=macro_name
                ) }}

                {% set size = window.get('warehouse_size', base_size) %}

                {# Apply window-specific volume scaling if enabled #}
                {% set win_scaling = window.get('volume_based_scaling', {}) %}
                {% if win_scaling.get('enabled') %}
                     {% set size = dbt_macro_polo.evaluate_thresholds(
                        win_scaling.get('thresholds', []),
                        volume,
                        size,
                        model_id,
                        macro_name
                    ) %}
                {% endif %}

                {{ return(size) }}
            {% endif %}
        {% endfor %}
    {% endif %}

    {# 2. Volume-Based Scaling #}
    {% set vol_scaling = config.get('volume_based_scaling', {}) %}
    {% if vol_scaling.get('enabled') %}
        {{ return(dbt_macro_polo.evaluate_thresholds(
            vol_scaling.get('thresholds', []),
            volume,
            base_size,
            model_id,
            macro_name
        )) }}
    {% endif %}

    {{ return(base_size) }}
{% endmacro %}

{% macro evaluate_thresholds(thresholds, volume, default_size, model_id=none, macro_name=none) %}
    {{ return(adapter.dispatch('evaluate_thresholds', 'dbt_macro_polo')(thresholds, volume, default_size, model_id, macro_name)) }}
{% endmacro %}

{% macro default__evaluate_thresholds(thresholds, volume, default_size, model_id, macro_name) %}
    {#
    Selects warehouse size based on volume thresholds.
    Thresholds are checked in descending order of rows.
    #}
    {% for t in thresholds | sort(attribute='rows', reverse=true) %}
        {% if volume >= t.rows %}
            {{ dbt_macro_polo.log_event(
                message="Volume threshold matched. Using: " ~ t.warehouse_size,
                level='DEBUG',
                status=volume ~ " >= " ~ t.rows,
                model_id=model_id,
                macro_name=macro_name
            ) }}
            {{ return(t.warehouse_size) }}
        {% endif %}
    {% endfor %}

    {{ dbt_macro_polo.log_event(
        message="No threshold match. Using default: " ~ default_size,
        level='DEBUG',
        status=volume ~ " < min",
        model_id=model_id,
        macro_name=macro_name
    ) }}
    {{ return(default_size) }}
{% endmacro %}
