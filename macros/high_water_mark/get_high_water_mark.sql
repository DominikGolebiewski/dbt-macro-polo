{% macro get_high_water_mark(column_name, predicate=none, warehouse_size='xs') %}
    {{ return(adapter.dispatch('get_high_water_mark', 'dbt_macro_polo')(column_name, predicate, warehouse_size)) }}
{% endmacro %}

{% macro snowflake__get_high_water_mark(column_name, predicate=none, warehouse_size='xs') %}

    {{ return(none) if not execute }}

    {% set macro_name = 'get_high_water_mark' %}

    {#/* Validation */#}
    {% if not column_name %}
        {% set msg = "Configuration Error: column_name is required." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=this, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {#/* Cache handling */#}
    {#-- Logic for key generation --#}
    {% set clean_predicate = '_' ~ predicate | replace(' ', '_') if predicate is not none else '' %}
    {% set clean_model_id = this | replace('.', '_') %}
    {% set state_key = '_macro_polo_hwm_' ~ clean_model_id ~ '_' ~ column_name ~ clean_predicate %}
    {% set state_value = dbt_macro_polo.get_runtime_state(state_key) %}
    {{ break if state_value }}

    {#/* Warehouse allocation */#}
    {% set warehouse = dbt_macro_polo.provision_compute(warehouse_size) %}

    {#/* Build Query */#}
    {% set query = dbt_macro_polo._build_hwm_query(column_name, predicate) %}

    {#/* Execute Query */#}
    {% set result = dbt_macro_polo.execute_query_with_warehouse(query, warehouse) %}

    {#/* Process Result */#}
    {% set max_value = result.columns[0].values()[0] %}

    {#/* Update Cache */#}
    {{ dbt_macro_polo.set_runtime_state(state_key, max_value) }}

    {#/* Log and return Result */#}
    {{ dbt_macro_polo.log_event(message="Resolved high water mark", level='INFO', model_id=this, status=max_value, macro_name=macro_name) }}
    {{ return("'" ~ max_value ~ "'") }}

{% endmacro %}
