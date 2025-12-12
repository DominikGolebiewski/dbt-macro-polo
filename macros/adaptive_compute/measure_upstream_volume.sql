{% macro measure_upstream_volume(volume_monitors, timestamp_column) %}
    {{ return(adapter.dispatch('measure_upstream_volume', 'dbt_macro_polo')(volume_monitors, timestamp_column)) }}
{% endmacro %}

{% macro default__measure_upstream_volume(volume_monitors, timestamp_column) %}

    {{ return(none) if not execute }}

    {% set macro_name = 'measure_upstream_volume' %}
    {% set maximum_timestamp = dbt_macro_polo.get_high_water_mark(column_name=timestamp_column) %}

    {% set total_rows = namespace(value=0) %}

    {% for monitor in volume_monitors %}
        {#/* Resolve Relation */#}
        {% set monitor_relation = dbt_macro_polo._resolve_monitor_relation(monitor) %}

        {#/* Build Query */#}
        {% set query = dbt_macro_polo._build_volume_query(monitor_relation, timestamp_column, maximum_timestamp) %}

        {#/* Execute with Safe Warehouse Switch */#}
        {% set res = dbt_macro_polo.execute_query_with_warehouse(query, wh) %}

        {% if res is not none and res.rows is not none %}
            {% set count = res.columns[0].values()[0] %}
            {% set total_rows.value = total_rows.value + (count or 0) %}
        {% endif %}
    {% endfor %}

    {% set final_volume = total_rows.value %}

    {#/* Log and Cache */#}
    {% set msg = "Total upstream volume calculated: " ~ final_volume %}
    {{ dbt_macro_polo.log_event(message=msg, level='DEBUG', model_id=this, status=final_volume | int, macro_name=macro_name) }}
    
    {{ return(final_volume) }}

{% endmacro %}
