{% macro handle_monitoring(row_count, thresholds, default_warehouse_size) %}
    {{ return(adapter.dispatch('handle_monitoring', 'dbt_macro_polo')(row_count, thresholds, default_warehouse_size)) }}
{% endmacro %}

{% macro default__handle_monitoring(row_count, thresholds, default_warehouse_size) %}
    {# Check if thresholds are configured #}
    {% if thresholds and thresholds is iterable %}
        {# Sort thresholds by threshold value in descending order #}
        {% set sorted_thresholds = thresholds | sort(attribute='threshold', reverse=true) %}
        
        {# Check each threshold #}
        {% for threshold in sorted_thresholds %}
            {% set threshold_value = threshold.get('threshold', 0) %}
            {% set scale = threshold.get('scale', default_warehouse_size) %}
            
            {# Return first matching threshold #}
            {% if row_count >= threshold_value %}
                {{ log("Row threshold matched: " ~ row_count ~ " >= " ~ threshold_value ~ ", using scale: " ~ scale, info=true) }}
                {{ return(scale) }}
            {% endif %}
        {% endfor %}
    {% endif %}

    {# Return default if no threshold matched #}
    {{ log("No row threshold matched, using default scale: " ~ default_warehouse_size, info=true) }}
    {{ return(default_warehouse_size) }}
{% endmacro %}
