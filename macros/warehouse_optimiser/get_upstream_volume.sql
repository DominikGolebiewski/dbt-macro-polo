{% macro get_upstream_volume(model_id, upstream_dependency, timestamp_column) %}
    {{ return(adapter.dispatch('get_upstream_volume', 'dbt_macro_polo')(model_id, upstream_dependency, timestamp_column)) }}
{% endmacro %}

{% macro default__get_upstream_volume(model_id, upstream_dependency, timestamp_column) %}
    
    {% set cache_key = '_upstream_volume_' ~ model_id | replace('.', '_') %}
    {% set cache_value = dbt_macro_polo.get_cache_value(cache_key) %}
    {% if cache_value %}
        {{ return(cache_value) }}
    {% endif %}

    {% set dependencies = [upstream_dependency] if upstream_dependency is string else upstream_dependency %}
    {% if not dependencies %}
        {{ return(0) }}
    {% endif %}

    {% set total_rows = namespace(value=0) %}
    
    {# Check if target exists to determine incremental scan #}
    {% set target_exists = load_relation(this) is not none %}
    {% set max_timestamp = '1900-01-01' %}
    {% set wh = dbt_macro_polo.allocate_warehouse('xs') %}

    {% if target_exists and timestamp_column %}
        {% set max_timestamp = dbt_macro_polo.get_max_timestamp(timestamp_column=timestamp_column) %}
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
                where {{ timestamp_column }} > '{{ max_timestamp }}'
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

    {{ dbt_macro_polo.logging(message="Total upstream volume: " ~ total_rows.value, model_id=model_id, level='DEBUG') }}
    {% do var('macro_polo', {}).get('cache', {}).update({cache_key: total_rows.value}) %}
    
    {{ return(total_rows.value) }}

{% endmacro %}

