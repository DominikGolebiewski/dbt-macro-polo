{% macro measure_upstream_volume(model_id, volume_monitors, timestamp_column) %}
    {#
    Calculates the volume (row count) of upstream dependencies.
    Used for adaptive compute to scale resources based on incoming data volume.

    Args:
        model_id (str): The model identifier.
        volume_monitors (str or list): One or more upstream model names or source references.
        timestamp_column (str): The column to filter by (e.g., for incremental loads).

    Returns:
        int: The total number of rows to process.
    #}
    {{ return(adapter.dispatch('measure_upstream_volume', 'dbt_macro_polo')(model_id, volume_monitors, timestamp_column)) }}
{% endmacro %}

{% macro default__measure_upstream_volume(model_id, volume_monitors, timestamp_column) %}

    {% set macro_name = 'measure_upstream_volume' %}
    {% set state_key = '_macro_polo_upstream_volume_' ~ model_id | replace('.', '_') %}

    {# 1. Check Cache #}
    {% set state_value = dbt_macro_polo.get_runtime_state(state_key) %}
    {% if state_value %}
        {{ dbt_macro_polo.log_event(
            message="Resolved volume from runtime state",
            model_id=model_id,
            status=state_value,
            level='DEBUG',
            macro_name=macro_name
        ) }}
        {{ return(state_value) }}
    {% endif %}

    {# 2. Provision Compute for Calculation #}
    {# Use XS warehouse for metadata-like operations #}
    {% set wh = dbt_macro_polo.provision_compute('xs') %}
    {% if not wh %}
         {{ dbt_macro_polo.log_event(message="Failed to allocate warehouse for volume measurement", level='ERROR', model_id=model_id, macro_name=macro_name) }}
         {{ return(0) }}
    {% endif %}

    {# 3. Determine High Water Mark #}
    {# If max_value is 0 or None, we treat it as '0' for string comparison in query builder #}
    {% set max_value = dbt_macro_polo.get_high_water_mark(column_name=timestamp_column) or '0' %}

    {# 4. Iterate and Sum Volume #}
    {% set total_rows = namespace(value=0) %}

    {# Ensure volume_monitors is a list #}
    {% set monitors_list = [volume_monitors] if volume_monitors is string else volume_monitors %}

    {% if execute %}
        {% for monitor in monitors_list %}
            {# Resolve Relation #}
            {% set monitor_relation = dbt_macro_polo._resolve_monitor_relation(monitor) %}

            {# Build Query #}
            {% set query = dbt_macro_polo._build_volume_query(monitor_relation, timestamp_column, max_value) %}

            {# Execute with Safe Warehouse Switch #}
            {% set res = dbt_macro_polo.execute_query_with_warehouse(query, wh) %}

            {% if res and res.rows %}
                {% set count = res.columns[0].values()[0] %}
                {% set total_rows.value = total_rows.value + (count or 0) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% set final_volume = total_rows.value %}

    {# 5. Log and Cache #}
    {{ dbt_macro_polo.log_event(
        message="Total upstream volume calculated",
        status=final_volume | int,
        model_id=model_id,
        level='DEBUG',
        macro_name=macro_name
    ) }}

    {{ dbt_macro_polo.set_runtime_state(state_key, final_volume, model_id, macro_name) }}

    {{ return(final_volume) }}

{% endmacro %}
