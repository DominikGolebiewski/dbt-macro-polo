{% macro measure_upstream_volume(model_id, upstream_dependency, timestamp_column) %}
    {{ return(adapter.dispatch('measure_upstream_volume', 'dbt_macro_polo')(model_id, upstream_dependency, timestamp_column)) }}
{% endmacro %}

{% macro default__measure_upstream_volume(model_id, upstream_dependency, timestamp_column) %}
    
    {% set macro_name = 'measure_upstream_volume' %}
    {% set state_key = '_macro_polo_upstream_volume_' ~ model_id | replace('.', '_') %}
    {% set state_value = dbt_macro_polo.get_runtime_state(state_key) %}

    {% if state_value %}
        {{ dbt_macro_polo.log_event(message="Resolved volume from runtime state", model_id=model_id, status=state_value, level='DEBUG', macro_name=macro_name) }}
        {{ return(state_value) }}
    {% endif %}

    {% set dependencies = [upstream_dependency] if upstream_dependency is string else upstream_dependency %}
    {% if not dependencies %}
        {{ dbt_macro_polo.log_event(message="No upstream dependencies provided", level='DEBUG', model_id=model_id, macro_name=macro_name) }}
        {{ return(0) }}
    {% endif %}

    {% set total_rows = namespace(value=0) %}
    
    {# Check if target exists to determine incremental scan #}
    {% set target_exists = load_relation(this) is not none %}
    {% set max_value = '0' %}
    
    {# Determine scan strategy #}
    {% if target_exists and timestamp_column %}
        {# Get high water mark using small warehouse #}
        {% set max_value = dbt_macro_polo.get_high_water_mark(column_name=timestamp_column) %}
        {% if not max_value %}
             {% set max_value = '0' %}
        {% endif %}
    {% endif %}
    
    {# Use XS warehouse for counting #}
    {% set wh = dbt_macro_polo.provision_compute('xs') %}
    
    {% for dep in dependencies %}
        {% set dep_relation = ref(dep) if '.' not in dep else source(dep.split('.')[0], dep.split('.')[1]) %}
        
        {% set query %}
            use warehouse {{ wh }};
            select count(1) from {{ dep_relation }}
            {% if target_exists and timestamp_column %}
                where {{ timestamp_column }} > {{ max_value }}
            {% endif %}
        {% endset %}
        
        {% if execute %}
            {% set res = run_query(query) %}
            {% if res and res.rows %}
                {% set count = default(res.columns[0].values()[0], 0) %}
                {% set total_rows.value = total_rows.value + count %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {{ dbt_macro_polo.log_event(message="Total upstream volume calculated", status=total_rows.value, model_id=model_id, level='DEBUG', macro_name=macro_name) }}
    
    {# Update: Use runtime_state instead of cache #}
    {% do var('macro_polo', {}).get('runtime_state', {}).update({state_key: total_rows.value}) %}
    
    {{ return(total_rows.value) }}

{% endmacro %}
