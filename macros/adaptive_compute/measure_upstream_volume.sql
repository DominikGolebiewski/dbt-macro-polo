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

    {% set total_rows = namespace(value=0) %}
    {% set max_value = dbt_macro_polo.get_high_water_mark(column_name=timestamp_column) or 0 %}

    {# Use XS warehouse for counting #}
    {% set wh = dbt_macro_polo.provision_compute('xs') %}

    {% for monitor in volume_monitors %}
        {% set monitor_relation =  ref(monitor) if '.' not in monitor else source(monitor.split('.')[0], monitor.split('.')[1]) %}

        {% set query %}
            use warehouse {{ wh }};
            select count(1)
            from {{ monitor_relation }}
            {% if max_value != '0' %}
            where {{ timestamp_column }} > {{ max_value }}
            {% endif %}
        {% endset %}

        {% if execute %}
            {% set res = run_query(query) %}
            {% if res and res.rows %}
                {% set count = res.columns[0].values()[0] %}
                {% set total_rows.value = total_rows.value + count %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# Ensure we log an explicit 0 if value is None or 0 #}
    {% set final_volume = total_rows.value if total_rows.value is not none else 0 %}
    {{ dbt_macro_polo.log_event(
        message="Total upstream volume calculated",
        status=final_volume | int,
        model_id=model_id,
        level='DEBUG',
        macro_name=macro_name
    ) }}

    {# Update: Use runtime_state instead of cache #}
    {% do var('macro_polo', {}).get('runtime_state', {}).update({state_key: final_volume}) %}

    {{ return(final_volume) }}

{% endmacro %}
