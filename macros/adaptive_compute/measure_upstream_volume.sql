{% macro measure_upstream_volume(volume_monitors, timestamp_column) %}
    {{ return(adapter.dispatch('measure_upstream_volume', 'dbt_macro_polo')(volume_monitors, timestamp_column)) }}
{% endmacro %}

{% macro default__measure_upstream_volume(volume_monitors, timestamp_column) %}

    {{ return(none) if not execute }}

    {% set macro_name = 'measure_upstream_volume' %}
    
    {#/* Check Cache */#}
    {% set state_key = '_macro_polo_upstream_volume_' ~ this | replace('.', '_') %}
    {% set state_value = dbt_macro_polo.get_runtime_state(state_key) %}

    #/* Iterate and Sum Volume */#}
    {% set total_rows = namespace(value=0) %}

    {% if state_value is none %}

        {#/* Provision Compute for Calculation */#}
        {#/* Use XS warehouse for metadata-like operations */#}
        {% set wh = dbt_macro_polo.provision_compute('xs') %}

        {#/* Determine High Water Mark */#}
        {#/* If maximum_timestamp is none, we treat it as '0' for string comparison in query builder */#}
        {% set maximum_timestamp = dbt_macro_polo.get_high_water_mark(column_name=timestamp_column) or '0' %}

        {#/* Ensure volume_monitors is a list */#}
        {% if volume_monitors is string or volume_monitors is not iterable %}
            {{ dbt_macro_polo.log_event(message="volume_monitors must be a list", level='ERROR', model_id=this, macro_name=macro_name) }}
            {{ return(none) }}
        {% endif %}

        {% for monitor in volume_monitors %}
            {#/* Resolve Relation */#}
            {% set monitor_relation = dbt_macro_polo._resolve_monitor_relation(monitor) %}

            {# Build Query #}
            {% set query = dbt_macro_polo._build_volume_query(monitor_relation, timestamp_column, maximum_timestamp) %}

            {#/* Execute with Safe Warehouse Switch */#}
            {% set res = dbt_macro_polo.execute_query_with_warehouse(query, wh) %}

            {% if res is not none and res.rows is not none %}
                {% set count = res.columns[0].values()[0] %}
                {% set total_rows.value = total_rows.value + (count or 0) %}
            {% endif %}
        {% endfor %}

        {#/* Update Cache */#}
        {{ dbt_macro_polo.set_runtime_state(state_key, total_rows.value) }}

    {% endif %}

    {% set final_volume = total_rows.value %}

    {#/* Log and Cache */#}
    {% set msg = "Total upstream volume calculated: " ~ final_volume %}
    {{ dbt_macro_polo.log_event(message=msg, level='DEBUG', model_id=this, status=final_volume | int, macro_name=macro_name) }}
    
    {{ return(final_volume) }}

{% endmacro %}
