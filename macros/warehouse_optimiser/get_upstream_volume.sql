{% macro get_upstream_volume(model_id, upstream_dependency, timestamp_column) %}
    {{ return(adapter.dispatch('get_upstream_volume', 'dbt_macro_polo')(model_id, upstream_dependency, timestamp_column)) }}
{% endmacro %}

{% macro default__get_upstream_volume(model_id, upstream_dependency, timestamp_column) %}
    
    {% set state_key = '_upstream_volume_' ~ model_id | replace('.', '_') %}
    {% set state_value = dbt_macro_polo.get_runtime_state(state_key) %}
    {% if state_value %}
        {{ return(state_value) }}
    {% endif %}

    {% set dependencies = [upstream_dependency] if upstream_dependency is string else upstream_dependency %}
    {% if not dependencies %}
        {{ return(0) }}
    {% endif %}

    {% set total_rows = namespace(value=0) %}
    
    {# Check if target exists to determine incremental scan #}
    {% set target_exists = load_relation(this) is not none %}
    {% set max_timestamp = '1900-01-01' %}
    {# Update: Call provision_compute instead of allocate_warehouse #}
    {% set wh = dbt_macro_polo.provision_compute('xs') %}

    {% if target_exists and timestamp_column %}
        {% set max_timestamp = dbt_macro_polo.get_high_water_mark(timestamp_column=timestamp_column) %}
        {% if not max_timestamp %}
             {% set max_timestamp = '1900-01-01' %}
        {% endif %}
    {% endif %}
    
    {% for dep in dependencies %}
        {% set dep_relation = ref(dep) if '.' not in dep else source(dep.split('.')[0], dep.split('.')[1]) %}
        
        {% set query %}
            use warehouse {{ wh }};
            select count(*) from {{ dep_relation }}
            {% if target_exists and timestamp_column %}
                where {{ timestamp_column }} > {{ max_timestamp }}
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

    {{ dbt_macro_polo.log_event(message="Total upstream volume: " ~ total_rows.value, model_id=model_id, level='DEBUG') }}
    {# Update: Use runtime_state instead of cache #}
    {% do var('macro_polo', {}).get('runtime_state', {}).update({state_key: total_rows.value}) %}
    
    {{ return(total_rows.value) }}

{% endmacro %}
