{% macro _determine_target_size(active_config, volume, model_id, is_full_refresh, volume_monitors, macro_name) %}
  {{ return(adapter.dispatch('_determine_target_size', 'dbt_macro_polo')(active_config, volume, model_id, is_full_refresh, volume_monitors, macro_name)) }}
{% endmacro %}

{% macro default___determine_target_size(active_config, volume, model_id, is_full_refresh, volume_monitors, macro_name) %}
    {# Special Case: Force XS for zero volume updates #}
    {% if volume == 0 and not is_full_refresh and volume_monitors %}
        {{ dbt_macro_polo.log_event(
            message="Zero upstream volume detected. Forcing XS.",
            level='DEBUG',
            model_id=model_id,
            macro_name=macro_name
        ) }}
        {{ return('xs') }}
    {% endif %}

    {{ return(dbt_macro_polo.determine_optimal_size(active_config, volume, model_id)) }}
{% endmacro %}
