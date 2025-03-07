{% macro get_upstream_row_count(upstream_dependency, timestamp_column) %}
    {{ return(adapter.dispatch('get_upstream_row_count', 'dbt_macro_polo')(upstream_dependency, timestamp_column)) }}
{% endmacro %}

{% macro default__get_upstream_row_count(upstream_dependency, timestamp_column) %}
    {# Cache handling #}
    {% set cache_key = '_upstream_row_count_' ~ this.identifier %}
    {% set cache_value = var('macro_polo', {}).get('cache', {}).get(cache_key) %}

    {% if cache_value %}
        {{ log("Using cached upstream row count: " ~ cache_value, info=true) }}
        {{ return(cache_value) }}
    {% endif %}

    {# Initialize row count #}
    {% set row_count = namespace(value=0) %}

    {% if upstream_dependency %}
        {# Determine if target exists #}
        {% set target_exists = load_relation(this) is not none %}
        
        {# Get XS warehouse for counting #}
        {% set warehouse = dbt_macro_polo.allocate_warehouse('xs') %}

        {# Get maximum timestamp for incremental scan #}
        {% set maximum_timestamp = dbt_macro_polo.get_max_timestamp(timestamp_column=timestamp_column) %}

        {# Convert single string to list #}
        {% if upstream_dependency is string %}
            {% set upstream_dependency = [upstream_dependency] %}
        {% endif %}

        {{ log("Checking " ~ upstream_dependency | length ~ " upstream models", info=true) }}

        {# Get total row count from upstream models #}
        {% for dependency in upstream_dependency %}
            {% set rows = dbt_macro_polo.check_upstream_row_count(target_exists, dependency, timestamp_column, warehouse, maximum_timestamp) %}
            {{ log("Row count for " ~ dependency ~ ": " ~ rows, info=true) }}
            {% set row_count.value = row_count.value + rows %}
        {% endfor %}
        
        {{ log("Total upstream row count: " ~ row_count.value, info=true) }}
    {% else %}
        {{ log("No upstream dependencies specified", info=true) }}
    {% endif %}

    {# Cache and return result #}
    {% do var('macro_polo', {}).get('cache', {}).update({cache_key: row_count.value}) %}
    {{ return(row_count.value) }}
{% endmacro %}

{% macro check_upstream_row_count(target_exists, upstream_relation, timestamp_column, warehouse, maximum_timestamp) %}
    {{ return(adapter.dispatch('check_upstream_row_count', 'dbt_macro_polo')(target_exists, upstream_relation, timestamp_column, warehouse, maximum_timestamp)) }}
{% endmacro %}

{% macro default__check_upstream_row_count(target_exists, upstream_relation, timestamp_column, warehouse, maximum_timestamp) %}
    {# Resolve upstream relation #}
    {% if '.' in upstream_relation %}
        {% set upstream_relation = source(upstream_relation.split('.')[0], upstream_relation.split('.')[1]) %}
    {% else %}
        {% set upstream_relation = ref(upstream_relation) %}
    {% endif %}

    {% set query %}
        use warehouse {{ warehouse }};
        
        with source_data as (
            select count(*) as row_count
            from {{ upstream_relation }}
            {% if target_exists and timestamp_column %}
                where {{ timestamp_column }} > {{ maximum_timestamp }}
            {% endif %}
        )
        select row_count from source_data
    {% endset %}

    {% if execute %}
        {% set results = run_query(query) %}
        {% set row_count = results.columns[0].values()[0] %}
        {{ return(row_count) }}
    {% else %}
        {{ return(0) }}
    {% endif %}
{% endmacro %}
