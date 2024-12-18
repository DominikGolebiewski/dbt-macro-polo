{% macro handle_monitoring(schedule, row_count, thresholds, default_warehouse_size) %}
    {{ return(adapter.dispatch('handle_monitoring', 'dbt_macro_polo')(schedule, row_count, thresholds, default_warehouse_size)) }}
{% endmacro %}

{% macro default__handle_monitoring(schedule, row_count, thresholds, default_warehouse_size) %}

    {# Initialise macro context #}
    {% set macro_ctx = dbt_macro_polo.create_macro_context('handle_monitoring') %}
    {% set macro_name = macro_ctx.macro_name %}
    {% set model_id = macro_ctx.model_id %}

    {# Convert list of thresholds into a dictionary #}
    {# Sort thresholds by row count in descending order to check largest thresholds first #}
    {% set sorted_thresholds = thresholds | sort(attribute='rows', reverse=true) %}
    
    {{ dbt_macro_polo.logging(macro_name, "Thresholds: " ~ sorted_thresholds, level='DEBUG', model_id=model_id) }}

    {# Check each threshold in descending order #}
    {% for threshold in sorted_thresholds %}
        {% if threshold.get('rows', none) is not none and threshold.get('warehouse_size', none) is not none %}
            {% if row_count >= threshold.rows %}
                {{ dbt_macro_polo.logging(message="Row threshold matched", model_id=model_id, status=row_count ~ " >= " ~ threshold.rows) }}
                {{ dbt_macro_polo.logging(message="Using warehouse size", model_id=model_id, status=threshold.warehouse_size | upper) }}
                {{ return(threshold.warehouse_size) }}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# If threshold not matched, return default size #}
    {{ dbt_macro_polo.logging(message="No row threshold matched, using default warehouse size", model_id=model_id, status=default_warehouse_size | upper) }}
    {{ return(default_warehouse_size) }}
{% endmacro %}
