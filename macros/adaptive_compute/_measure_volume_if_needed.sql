{% macro _measure_volume_if_needed(model_config, execute, is_full_refresh, model_id, model, macro_name) %}
  {{ return(adapter.dispatch('_measure_volume_if_needed', 'dbt_macro_polo')(model_config, execute, is_full_refresh, model_id, model, macro_name)) }}
{% endmacro %}

{% macro default___measure_volume_if_needed(model_config, execute, is_full_refresh, model_id, model, macro_name) %}
    {% set volume = 0 %}
    {% set volume_monitors = model_config.get('volume_monitors', []) %}

    {% if execute and not is_full_refresh and volume_monitors %}
        {% if volume_monitors is string or volume_monitors is not iterable %}
             {{ dbt_macro_polo.log_event(
                 message="volume_monitors must be a list",
                 level='ERROR',
                 model_id=model_id,
                 macro_name=macro_name
             ) }}
             {{ return(none) }}
        {% endif %}

        {% set timestamp_col = model.config.get('timestamp_column') %}
        {% if not timestamp_col %}
            {{ dbt_macro_polo.log_event(
                message="timestamp_column required for volume monitoring",
                level='ERROR',
                model_id=model_id,
                macro_name=macro_name
            ) }}
            {{ return(none) }}
        {% endif %}

        {% set volume = dbt_macro_polo.measure_upstream_volume(model_id, volume_monitors, timestamp_col) %}
    {% endif %}

    {{ return(volume) }}
{% endmacro %}
