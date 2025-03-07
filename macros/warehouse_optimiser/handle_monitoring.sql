{% macro handle_monitoring(operation_config, row_count, thresholds, default_warehouse_size) %}
    {{ return(adapter.dispatch('handle_monitoring', 'dbt_macro_polo')(operation_config, row_count, thresholds, default_warehouse_size)) }}
{% endmacro %}

{% macro default__handle_monitoring(operation_config, row_count, thresholds, default_warehouse_size) %}

    {# Initialise macro context #}
    {% set macro_ctx = dbt_macro_polo.create_macro_context('handle_monitoring') %}
    {% set macro_name = macro_ctx.macro_name %}
    {% set model_id = macro_ctx.model_id %}

    {{ dbt_macro_polo.logging(macro_name, "Row count: " ~ row_count ~ ", Thresholds: " ~ thresholds, level='DEBUG', model_id=model_id) }}

    {# Check each threshold in the list #}
    {% if thresholds and thresholds is iterable %}
        {# Sort thresholds by threshold value in descending order #}
        {% set sorted_thresholds = thresholds | sort(attribute='threshold', reverse=true) %}
        
        {% for threshold_config in sorted_thresholds %}
            {% set threshold_value = threshold_config.get('threshold', 0) %}
            {% set scale = threshold_config.get('scale', default_warehouse_size) %}
            
            {% if row_count >= threshold_value %}
                {{ dbt_macro_polo.logging(message="Row threshold matched", model_id=model_id, status=row_count ~ " >= " ~ threshold_value) }}
                {{ dbt_macro_polo.logging(message="Using warehouse size", model_id=model_id, status=scale | upper) }}
                {{ return(scale) }}
            {% endif %}
        {% endfor %}
    {% endif %}

    {# If threshold not matched, return default size #}
    {{ dbt_macro_polo.logging(message="No row threshold matched, using default warehouse size", model_id=model_id, status=default_warehouse_size | upper) }}
    {{ return(default_warehouse_size) }}
{% endmacro %}
