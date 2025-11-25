{% macro get_high_water_mark(column_name, predicate=none, warehouse_size='xs', model_name=this.name) %}
    {{ return(adapter.dispatch('get_high_water_mark', 'dbt_macro_polo')(column_name, predicate, warehouse_size, model_name)) }}
{% endmacro %}

{% macro snowflake__get_high_water_mark(column_name, predicate=none, warehouse_size='xs', model_name=this.name) %}

    {% set macro_name = 'get_high_water_mark' %}

    {% if this is not defined or not this %}
        {% set msg = "Configuration Error: provision_compute macro requires a valid model context. The 'this' variable is not defined. This macro is intended for model/materialisation execution only." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id='unknown_model', macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {% set model_id = this.schema ~ "." ~ this.name %}

    {# 1. Validation #}
    {% if not column_name %}
        {% set msg = "Configuration Error: column_name is required." %}
        {{ dbt_macro_polo.log_event(
            message=msg,
            level='ERROR',
            model_id=model_id,
            macro_name=macro_name
            ) }}
        {{ return(none) }}
    {% endif %}

    {% if execute %}
        {# 2. Cache handling #}
        {# Logic for key generation #}
        {% set clean_predicate = '_' ~ predicate | replace(' ', '_') if predicate is not none else '' %}
        {% set clean_model_id = model_id | replace('.', '_') %}
        {% set state_key = '_macro_polo_hwm_' ~ clean_model_id ~ '_' ~ column_name ~ clean_predicate %}

        {% set state_value = dbt_macro_polo.get_runtime_state(state_key) %}

        {% if state_value %}
            {{ dbt_macro_polo.log_event(
                message="Resolved high water mark from runtime state",
                model_id=model_id,
                status=state_value | upper,
                macro_name=macro_name
            ) }}
            {{ return("'" ~ state_value ~ "'") }}
        {% endif %}

        {# 3. Warehouse allocation #}
        {% set warehouse = dbt_macro_polo.provision_compute(warehouse_size) %}
        {% if not warehouse %}
             {{ dbt_macro_polo.log_event(
                message="Failed to allocate warehouse",
                level='ERROR',
                model_id=model_id,
                macro_name=macro_name
             ) }}
             {{ return(none) }}
        {% endif %}

        {# 4. Relation Resolution #}
        {% set relation = dbt_macro_polo.resolve_relation(model_name, this) %}
        {% if not relation %}
            {{ dbt_macro_polo.log_event(
                message="Relation not found: " ~ model_name,
                level='ERROR',
                model_id=model_id,
                macro_name=macro_name
            ) }}
            {{ return(none) }}
        {% endif %}

        {# 5. Build Query #}
        {% set query = dbt_macro_polo._build_hwm_query(column_name, relation, predicate) %}

        {# 6. Execute Query #}
        {% set result = dbt_macro_polo.execute_query_with_warehouse(query, warehouse) %}

        {% if not result %}
            {{ dbt_macro_polo.log_event(message="Query execution failed", level='ERROR', model_id=model_id, macro_name=macro_name) }}
            {{ return(none) }}
        {% endif %}

        {# 7. Process Result #}
        {% set max_value = result.columns[0].values()[0] %}
        {% if max_value is none %}
            {{ dbt_macro_polo.log_event(message="No timestamp value returned", level='ERROR', model_id=model_id, macro_name=macro_name) }}
            {{ return(none) }}
        {% endif %}

        {# 8. Update Cache #}
        {{ dbt_macro_polo.set_runtime_state(state_key, max_value, model_id, macro_name) }}

        {{ dbt_macro_polo.log_event(
            message="Resolved high water mark",
            model_id=model_id,
            status=max_value | upper,
            macro_name=macro_name
        ) }}

        {{ return("'" ~ max_value ~ "'") }}

    {% endif %}

{% endmacro %}
