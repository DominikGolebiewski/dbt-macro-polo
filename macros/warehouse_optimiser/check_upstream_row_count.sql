{% macro get_upstream_row_count(model_id, upstream_dependency, timestamp_column) %}
    {{ return(adapter.dispatch('get_upstream_row_count', 'dbt_macro_polo')(model_id, upstream_dependency, timestamp_column)) }}
{% endmacro %}

{% macro default__get_upstream_row_count(model_id, upstream_dependency, timestamp_column) %}

    {# Initialise macro context #}
    {% set macro_ctx = dbt_macro_polo.create_macro_context('get_upstream_row_count') %}
    {% set macro_name = macro_ctx.macro_name %}
    {% set model_id = macro_ctx.model_id %}
    {% set macro_polo = var('macro_polo', {}) %}
 
    {# Cache handling #}
    {% set cache_key = '_upstream_row_count_' ~ model_id | replace('.', '_') %}
    {% set cache_value = dbt_macro_polo.get_cache_value(cache_key) %}

    {% if cache_value %}
        {{ dbt_macro_polo.logging(macro_name, message="Upstream row count from cache", model_id=model_id, status=cache_value | upper) }}
        {{ return(cache_value) }}
    {% endif %}

    {# Initialise row count #}
    {%- set row_count = namespace(value=0) -%}

    {%- if upstream_dependency is string or (upstream_dependency is sequence and upstream_dependency | length > 0) -%}
        {{ dbt_macro_polo.logging(macro_name, "Initialising row count", model_id=model_id, level='DEBUG') }}
        
        {# Determine if target exists #}
        {{ dbt_macro_polo.logging(macro_name, "Checking if target relation exists", model_id=model_id, level='DEBUG') }}
        {% set target_exists = load_relation(this) is not none %}
        {{ dbt_macro_polo.logging(macro_name, "Target relation exists: " ~ target_exists, model_id=model_id, level='DEBUG') }}

        {# Fetch environment-specific XS warehouse name #}
        {{ dbt_macro_polo.logging(macro_name, "Allocating warehouse", model_id=model_id, level='DEBUG') }}
        {%- set warehouse = dbt_macro_polo.allocate_warehouse('xs') -%}

        {# Get maximum timestamp for incremental scan #}
        {% set maximum_timestamp = dbt_macro_polo.get_max_timestamp(timestamp_column=timestamp_column) %}

        {# Convert single string to list #}
        {%- if upstream_dependency is string -%}
            {%- set upstream_dependency = [upstream_dependency] -%}
        {%- endif -%}

        {{ dbt_macro_polo.logging(message="Upstream models count", model_id=model_id, status=upstream_dependency | length) }}

        {# Get total row count from upstream models #}
        {%- for dependency in upstream_dependency -%}
            {#-- Each entry is either a plain name (uses the model's default keys) or a
                 mapping {name, keys} for per-upstream key resolution — needed when the
                 upstreams mix business-key (e.g. staging) and surrogate-key (e.g.
                 intermediary) tables, so each is counted with the keys it actually holds. --#}
            {%- set dep_name = dependency.get('name') if dependency is mapping else dependency -%}
            {%- set dep_keys = dependency.get('keys') if dependency is mapping else none -%}
            {%- set rows = dbt_macro_polo.check_upstream_row_count(target_exists, dep_name, timestamp_column, warehouse, maximum_timestamp, dep_keys) -%}
            {{ dbt_macro_polo.logging(message="Row count for " ~ dep_name ~ (" [keys=" ~ dep_keys ~ "]" if dep_keys else ""), model_id=model_id, status=rows) }}
            {%- set row_count.value = row_count.value + rows -%}
        {%- endfor -%}
        
        {{ dbt_macro_polo.logging(message="Total upstream row count", model_id=model_id, status=row_count.value) }}

    {% else %}
        {{ dbt_macro_polo.logging(message="No upstream dependencies specified", model_id=model_id) }}
    {%- endif -%}

    {# Cache and return result #}
    {{ dbt_macro_polo.logging(macro_name, "Caching upstream row count for " ~ upstream_dependency ~ ": " ~ row_count.value, model_id=model_id, level='DEBUG') }}
    {% do macro_polo.get('cache', {}).update({cache_key: row_count.value}) %}
    {{ return(row_count.value) }}
{%- endmacro -%}

{% macro check_upstream_row_count(target_exists, upstream_relation, timestamp_column, warehouse, maximum_timestamp, keys=none) %}
    {{ return(adapter.dispatch('check_upstream_row_count', 'dbt_macro_polo')(target_exists, upstream_relation, timestamp_column, warehouse, maximum_timestamp, keys)) }}
{% endmacro %}

{% macro default__check_upstream_row_count(target_exists, upstream_relation, timestamp_column, warehouse, maximum_timestamp, keys=none) %}

    {# Initialise macro context #}
    {% set macro_ctx = dbt_macro_polo.create_macro_context('check_upstream_row_count') %}
    {% set macro_name = macro_ctx.macro_name %}
    {% set model_id = macro_ctx.model_id %}

    {# Get global optimisation f#}
    {%- set optimiser_config = var('warehouse_optimiser', {}) -%}
    {%- set global_optimisation_enabled = optimiser_config.get('enabled', false) -%}

    {# Resolve upstream relation (keep the original name so the consumer can infer
       this upstream's key type from its layer, e.g. stg_/int_/fact_ convention) #}
    {%- set upstream_name = upstream_relation -%}

    {%- if '.' in upstream_relation -%}
        {%- set upstream_relation = source(upstream_relation.split('.')[0], upstream_relation.split('.')[1]) -%}
    {%- else -%}
        {%- set upstream_relation = ref(upstream_relation) -%}
    {%- endif -%}
    {{ dbt_macro_polo.logging(macro_name, "Resolved upstream relation: " ~ upstream_relation, model_id=model_id, level='DEBUG') }}

    {% set query %}
        use warehouse {{ warehouse }};
        
        with source_data as (
            select count(*) as row_count
            from {{ upstream_relation }}
            {% if target_exists and timestamp_column %}
                {% if var('selective_refresh', false) == false %}
                    where {{ timestamp_column }} > {{ maximum_timestamp }}
                {% else %}
                    where {{ dbt_macro_polo.polo_selective_refresh_filter(keys=keys, relation=upstream_name) }}
                {% endif %}
            {% endif %}
        )
        select 
            row_count,
            '{{ upstream_relation }}' as source_table,
            {% if target_exists %}
                'incremental' as scan_type,
                '{{ timestamp_column }}' as filter_column
            {% else %}
                'full' as scan_type,
                null as filter_column
            {% endif %}
        from source_data
    {% endset %}

    {% set formatted_query = query | replace('\n', ' ') | replace('    ', ' ') | trim %}

    {% if execute %}
        {{ dbt_macro_polo.logging(macro_name, "Executing query: " ~ formatted_query, level='DEBUG', model_id=model_id) }}
        {% set results = run_query(formatted_query) %}
        {% set row_count = results.columns[0].values()[0] %}
        {{ dbt_macro_polo.logging(macro_name, "Upstream row count for " ~ upstream_relation ~ ": " ~ row_count, model_id=model_id, level='DEBUG') }}
        {{ return(row_count) }}
    {% else %}
        {{ return(0) }}
    {% endif %}
{% endmacro %}
