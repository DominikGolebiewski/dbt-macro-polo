{% macro get_high_water_mark(column_name, predicate=none, warehouse_size='xs') %}
    {{ return(adapter.dispatch('get_high_water_mark', 'dbt_macro_polo')(column_name, predicate, warehouse_size)) }}
{% endmacro %}

{% macro snowflake__get_high_water_mark(column_name, predicate=none, warehouse_size='xs') %}

    {{ return(none) if not execute }}

    {% set macro_name = 'get_high_water_mark' %}
    {% set model_id = this.schema ~ "." ~ this.name %}

    {#-- 1. Validation --#}
    {% if not column_name %}
        {% set msg = "Configuration Error: column_name is required." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {#-- 2. Cache handling --#}
    {#-- Logic for key generation --#}
    {% set clean_predicate = '_' ~ predicate | replace(' ', '_') if predicate is not none else '' %}
    {% set clean_model_id = this | replace('.', '_') %}
    {% set state_key = '_macro_polo_hwm_' ~ clean_model_id ~ '_' ~ column_name ~ clean_predicate %}
    {% set state_value = dbt_macro_polo.get_runtime_state(state_key) %}
    {{ break if state_value }}

    {#-- 3. Warehouse allocation --#}
    {% set warehouse = dbt_macro_polo.provision_compute(warehouse_size) %}

    {#-- 4. Build Query --#}
    {% set query = dbt_macro_polo._build_hwm_query(column_name, predicate) %}

    {#-- 5. Execute Query --#}
    {% set result = dbt_macro_polo.execute_query_with_warehouse(query, warehouse) %}

    {#-- 6. Process Result --#}
    {% set max_value = result.columns[0].values()[0] %}

    {#-- 7. Update Cache --#}
    {{ dbt_macro_polo.set_runtime_state(state_key, max_value, model_id, macro_name) }}

    {% set msg = "Resolved high water mark" %}
    {{ dbt_macro_polo.log_event(message=msg, level='DEBUG', model_id=model_id, status=max_value | upper, macro_name=macro_name) }}

    {{ return("'" ~ max_value ~ "'") }}

{% endmacro %}
