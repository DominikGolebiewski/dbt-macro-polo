{% macro _measure_volume_if_needed(volume_monitors, is_full_refresh) %}
  {{ return(adapter.dispatch('_measure_volume_if_needed', 'dbt_macro_polo')(volume_monitors, is_full_refresh)) }}
{% endmacro %}

{% macro default___measure_volume_if_needed(volume_monitors, is_full_refresh) %}

    {{ return(none) if not execute }}   
    {% set macro_name = '_measure_volume_if_needed' %}

    {% set volume = 0 %}

    {% if volume_monitors is string or volume_monitors is not iterable %}
        {{ dbt_macro_polo.log_event(message="volume_monitors must be a list", level='ERROR', model_id=this, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {% set timestamp_col = model.config.get('timestamp_column') %}
    {% if not timestamp_col %}
        {{ dbt_macro_polo.log_event(message="timestamp_column required for volume monitoring", level='ERROR', model_id=this, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {% set volume = dbt_macro_polo.measure_upstream_volume(volume_monitors, timestamp_col) %}

    {{ return(volume) }}

{% endmacro %}
